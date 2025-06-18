// === FILE: client.cpp ===

#include "client.hpp"
#include <vector>

Client::Client() : rank(CLIENT_RANK), rng(std::random_device{}()), processedRequestCount(0) {
    servers = {LEADER_RANK, FOLLOWER1_RANK, FOLLOWER2_RANK};
    serverDist = std::uniform_int_distribution<int>(0, static_cast<int>(servers.size()) - 1);
}

Client::~Client() {
    for (auto& send : outstandingSends) {
        delete[] send.buffer;
    }
}

void Client::run(int numRequests) {
    log_debug("CLIENT", "Started with rank " + std::to_string(rank));

    const int MAX_PENDING_REQUESTS = 50 * BATCH_SIZE; //20 with 10 default

    for (int i = 1; i <= numRequests; i++) {
        while (pendingRequests.size() >= MAX_PENDING_REQUESTS) {
            checkCompletedSends();

            MPI_Status status;
            int flag = 0;
            MPI_Iprobe(MPI_ANY_SOURCE, CLIENT_RESPONSE, MPI_COMM_WORLD, &flag, &status);
            if (flag) {
                handleResponse(status);
            }
        }
        sendRequest(i);
    }

    log_debug("CLIENT", "All requests initiated. Waiting for " + std::to_string(pendingRequests.size()) + " responses...");
    while (!pendingRequests.empty()) {
        MPI_Status status;
        int flag = 0;

        MPI_Iprobe(MPI_ANY_SOURCE, CLIENT_RESPONSE, MPI_COMM_WORLD, &flag, &status);
        if (flag) {
            handleResponse(status);
        }

        checkCompletedSends();
    }

    while (!outstandingSends.empty()) {
        checkCompletedSends();
    }

    printStatistics();
}

void Client::sendRequest(int value) {
    int responderIndex = 0;
    int respondTo = servers[responderIndex];

    std::string payload = "pv_" + std::to_string(value);
    std::string msg_str = std::to_string(value) + "|" +
                         std::to_string(respondTo) + "|" +
                         payload;

    auto startTime = std::chrono::high_resolution_clock::now();
    pendingRequests[value] = {value, respondTo, startTime};

    int msg_len = msg_str.length() + 1;
    char* send_buffer = new char[msg_len];
    strncpy(send_buffer, msg_str.c_str(), msg_len);

    MPI_Request mpi_req;
    MPI_Isend(send_buffer, msg_len, MPI_CHAR, SWITCH_RANK,
              CLIENT_REQUEST, MPI_COMM_WORLD, &mpi_req);

    outstandingSends.push_back({mpi_req, send_buffer});
}

void Client::checkCompletedSends() {
    if (outstandingSends.empty()) return;

    auto it = outstandingSends.begin();
    while (it != outstandingSends.end()) {
        int flag = 0;
        MPI_Test(&it->request, &flag, MPI_STATUS_IGNORE);
        if (flag) {
            delete[] it->buffer;
            it = outstandingSends.erase(it);
        } else {
            ++it;
        }
    }
}

void Client::handleResponse(MPI_Status& status) {
    int msg_size;
    MPI_Get_count(&status, MPI_CHAR, &msg_size);
    std::vector<char> buffer_vec(msg_size);

    // --- NON-BLOCKING RECEIVE UPDATE ---
    MPI_Request recv_request;
    MPI_Irecv(buffer_vec.data(), msg_size, MPI_CHAR, status.MPI_SOURCE, CLIENT_RESPONSE, MPI_COMM_WORLD, &recv_request);
    MPI_Wait(&recv_request, MPI_STATUS_IGNORE);
    // --- END UPDATE ---

    std::string data(buffer_vec.data(), msg_size - 1);

    std::vector<std::string> parts = split_string(data, '|'); // e.g., "SUCCESS|v1|p1|v2|p2..."
    // A valid batch response has "SUCCESS" and then pairs of (value, payload).
    // So, size must be >= 3 and odd.
    if (parts.empty() || parts[0] != "SUCCESS" || parts.size() < 3 || (parts.size() - 1) % 2 != 0) {
        return; // Malformed or empty batch response
    }

    // Process each response in the batch
    for (size_t i = 1; i < parts.size(); i += 2) {
        int value = std::stoi(parts[i]);

        auto it = pendingRequests.find(value);
        if (it != pendingRequests.end()) {
            auto endTime = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(endTime - it->second.startTime);
            latencies.push_back(static_cast<double>(duration.count()) / 1000.0);
            pendingRequests.erase(it);

            processedRequestCount++;
//            const int STATS_INTERVAL = 100000;
            if ((processedRequestCount % STATS_INTERVAL) == 0) {
                std::cout << "\n--- Statistics for interval ending at " << processedRequestCount << " total requests ---" << std::endl;
                printStatistics();
                latencies.clear();
            }
        }
    }
}

void Client::printStatistics() {
    if (latencies.empty()) {
        std::cout << "\n=== Latency Statistics (ms) ===" << std::endl;
        std::cout << "No responses received." << std::endl;
        std::cout << "==============================\n" << std::endl;
        return;
    }

    double sum = 0.0;
    for (double lat : latencies) sum += lat;
    double avgLat = sum / latencies.size();

    std::sort(latencies.begin(), latencies.end());
    double minLat = latencies[0];
    double maxLat = latencies.back();
    double p50 = latencies[latencies.size() / 2];
    double p90 = latencies.empty() ? 0 : latencies[static_cast<size_t>(latencies.size() * 0.90)];
    double p99 = latencies.empty() ? 0 : latencies[static_cast<size_t>(latencies.size() * 0.99)];

    std::cout << "\n=== Latency Statistics (ms) ===" << std::endl;
    std::cout << std::fixed << std::setprecision(3);
    std::cout << "Requests processed: " << latencies.size() << std::endl;
    std::cout << "Average: " << avgLat << std::endl;
    std::cout << "Min:     " << minLat << std::endl;
    std::cout << "Max:     " << maxLat << std::endl;
    std::cout << "P50 (Median): " << p50 << std::endl;
    std::cout << "P90:     " << p90 << std::endl;
    std::cout << "P99:     " << p99 << std::endl;
    std::cout << "==============================\n" << std::endl;
}
