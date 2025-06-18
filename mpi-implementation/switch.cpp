#include "switch.hpp"
#include <vector> // For safer buffer management on receive

// Add a destructor to clean up any remaining buffers
Switch::~Switch() {
    // In a real scenario, you'd wait for sends to complete or cancel them
    for (auto& send : outstandingSends) {
        // This is a simplified cleanup; we're just freeing memory.
        // A robust implementation would MPI_Cancel and MPI_Wait.
        delete[] send.buffer;
    }
}

Switch::Switch() : rank(SWITCH_RANK), currentTerm(1), nextValue(1) {
    // Same as before
    switchSentRecord[LEADER_RANK] = {};
    switchSentRecord[FOLLOWER1_RANK] = {};
    switchSentRecord[FOLLOWER2_RANK] = {};
}

void Switch::run(bool& shutdown_flag) {
    log_debug("SWITCH", "Started with rank " + std::to_string(rank));
    
    while (!shutdown_flag) {
        MPI_Status status;
        int flag;

        // todo Add a probe for the shutdown signal but only consider it when we processed
        // the total number of requests expected to process
        // MPI_Iprobe(MPI_ANY_SOURCE, SHUTDOWN_SIGNAL, MPI_COMM_WORLD, &flag, &status);
        // if (flag) {
        //     shutdown_flag = true;
        //     // Propagate the signal to other servers before exiting loop
        //     std::vector<int> servers_to_notify = {LEADER_RANK, FOLLOWER1_RANK, FOLLOWER2_RANK, NETAGG_RANK};
        //     for (int dest : servers_to_notify) {
        //         MPI_Send(nullptr, 0, MPI_CHAR, dest, SHUTDOWN_SIGNAL, MPI_COMM_WORLD);
        //     }
        //     // Once signals are sent, break the loop
        //     continue; 
        // }

        MPI_Iprobe(MPI_ANY_SOURCE, CLIENT_REQUEST, MPI_COMM_WORLD, &flag, &status);
        
        if (flag) {
            handleClientRequest(status);
        }
        
        replicateBufferedRequests();

        // New step: check for and clean up completed non-blocking sends
        checkCompletedSends();
    }
}

void Switch::handleClientRequest(MPI_Status& status) {
    int msg_size;
    MPI_Get_count(&status, MPI_CHAR, &msg_size);
    
    // SAFER: Use std::vector for the receive buffer
    std::vector<char> buffer_vec(msg_size);
    
    // --- NON-BLOCKING RECEIVE UPDATE ---
    MPI_Request request;
    // 1. Initiate the non-blocking receive
    MPI_Irecv(buffer_vec.data(), msg_size, MPI_CHAR, status.MPI_SOURCE, CLIENT_REQUEST,
             MPI_COMM_WORLD, &request);
    // 2. Wait for the receive to complete before using the buffer
    MPI_Wait(&request, MPI_STATUS_IGNORE);
    // --- END UPDATE ---

    std::string data(buffer_vec.data(), msg_size -1); // Exclude null terminator
    
    // Use the robust helper function already provided
    std::vector<std::string> parts = split_string(data, '|');
    if (parts.size() < 3) {
        log_debug("SWITCH_ERROR", "Malformed client request: " + data);
        return;
    }
    
    ClientRequestMsg request_msg;
    request_msg.value = std::stoi(parts[0]);
    request_msg.respondTo = std::stoi(parts[1]);
    request_msg.payload = parts[2];
    
    RequestID rid{request_msg.value, currentTerm}; // Use client value for RID to match spec better
    LogEntry entry(currentTerm, request_msg.value, request_msg.payload, request_msg.respondTo);
    
    // To avoid processing duplicates if client retries, check existence
    if (switchBuffer.find(rid) == switchBuffer.end()) {
        switchBuffer[rid] = entry;
        //log_debug("SWITCH", "Buffered client request: value=" + std::to_string(request_msg.value));
    }
}

void Switch::sendReplicateMessage(int dest, const RequestID& rid, const LogEntry& entry) {
    std::string msg_str = std::to_string(rid.value) + "|" + 
                         std::to_string(rid.term) + "|" + 
                         entry.payload + "|" + 
                         std::to_string(entry.clientRank);

    // CRITICAL CHANGE: Use non-blocking send
    int msg_len = msg_str.length() + 1;
    char* send_buffer = new char[msg_len];
    strncpy(send_buffer, msg_str.c_str(), msg_len);

    MPI_Request mpi_req;
    MPI_Isend(send_buffer, msg_len, MPI_CHAR, dest, SWITCH_REPLICATE, MPI_COMM_WORLD, &mpi_req);
    
    // Track the request and its buffer
    outstandingSends.push_back({mpi_req, send_buffer});

    // Don't log here, log when buffering. Replicating is an internal detail.
}

void Switch::checkCompletedSends() {
    if (outstandingSends.empty()) {
        return;
    }

    auto it = outstandingSends.begin();
    while (it != outstandingSends.end()) {
        int flag = 0;
        MPI_Status status;
        MPI_Test(&it->request, &flag, &status);
        if (flag) {
            // Send is complete, free the buffer and remove from list
            delete[] it->buffer;
            it = outstandingSends.erase(it);
        } else {
            ++it;
        }
    }
}

// Keep replicateBufferedRequests the same, it now calls the non-blocking sendReplicateMessage
// void Switch::replicateBufferedRequests() {
//     // This logic is mostly the same, but we will remove the entry from the buffer once replication is initiated
//     auto it = switchBuffer.begin();
//     while(it != switchBuffer.end()) {
//         const auto& [rid, entry] = *it;
//         std::vector<int> targetServers = {LEADER_RANK, FOLLOWER1_RANK, FOLLOWER2_RANK};
        
//         bool all_sent = true;
//         for (int server : targetServers) {
//             if (switchSentRecord[server].find(rid) == switchSentRecord[server].end()) {
//                  // We only initiate the send here. It will complete in the background.
//                 sendReplicateMessage(server, rid, entry);
//                 switchSentRecord[server].insert(rid);
//             }
//         }
//         // Once we have *initiated* the send to all parties, we can remove it from the primary buffer.
//         // The outstandingSends list will manage the memory.
//         it = switchBuffer.erase(it);
//     }
// }

void Switch::replicateBufferedRequests() {
    if (switchBuffer.empty()) return;

    auto it = switchBuffer.begin();
    while(it != switchBuffer.end()) {
        const auto& [rid, entry] = *it;
        std::vector<int> targetServers = {LEADER_RANK, FOLLOWER1_RANK, FOLLOWER2_RANK};
        
        for (int server : targetServers) {
            if (switchSentRecord[server].find(rid) == switchSentRecord[server].end()) {
                sendReplicateMessage(server, rid, entry);
                switchSentRecord[server].insert(rid);
            }
        }

        it = switchBuffer.erase(it);
        replicatedCount++;

        // --- PRUNING LOGIC TO PREVENT UNBOUNDED GROWTH ---
        const int PRUNE_INTERVAL = 5000;
        const int RETAIN_WINDOW = 20000; // Keep history for a reasonable number of recent requests
        if (replicatedCount > 0 && replicatedCount % PRUNE_INTERVAL == 0) {
            int prune_threshold = rid.value - RETAIN_WINDOW;
            if (prune_threshold > 0) {
                log_debug("SWITCH", "Pruning records older than value: " + std::to_string(prune_threshold));
                for (auto& pair : switchSentRecord) {
                    auto& sent_set = pair.second;
                    // C++17 compatible way to erase elements from a set based on a condition
                    for (auto set_it = sent_set.begin(); set_it != sent_set.end(); ) {
                        if (set_it->value < prune_threshold) {
                            set_it = sent_set.erase(set_it);
                        } else {
                            ++set_it;
                        }
                    }
                }
            }
        }
    }
}
