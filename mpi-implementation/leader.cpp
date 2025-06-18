#include "leader.hpp"
#include <vector>

Leader::Leader() : rank(LEADER_RANK), currentTerm(1), logStartIndex_0based(0), commitIndex(-1), lastSentLogIndexToNetAgg(-1),
                   clientResponseBatchTimer() {
    nextIndex[FOLLOWER1_RANK] = 1;
    nextIndex[FOLLOWER2_RANK] = 1;
    matchIndex[FOLLOWER1_RANK] = -1;
    matchIndex[FOLLOWER2_RANK] = -1;
}

Leader::~Leader() {
    // Clean up any remaining allocated buffers for sends that might not have completed.
    for (auto& send : outstandingSends) {
        delete[] send.buffer;
    }
}

void Leader::run(bool& shutdown_flag) {
    log_debug("LEADER", "Started with rank " + std::to_string(rank) +
              ", term " + std::to_string(currentTerm));

    while (!shutdown_flag) {
        MPI_Status status;
        int flag = 0;

        MPI_Iprobe(SWITCH_RANK, SWITCH_REPLICATE, MPI_COMM_WORLD, &flag, &status);
        if (flag) {
            handleSwitchReplicate(status);
        }

        MPI_Iprobe(NETAGG_RANK, AGG_COMMIT, MPI_COMM_WORLD, &flag, &status);
        if (flag) {
            handleAggCommit(status);
        }

        processUnorderedRequests();
        sendToNetAgg();
        sendBatchedClientResponses();
        
        checkCompletedSends();
    }
}

void Leader::sendToNetAgg() {
    int nextLogIndexToSend_0based = lastSentLogIndexToNetAgg + 1;
    int lastLogIndex_0based = logStartIndex_0based + log.size() - 1;

    if (nextLogIndexToSend_0based <= lastLogIndex_0based) {
        std::vector<RequestID> batch_ids;
        for (int i = 0; i < BATCH_SIZE && (nextLogIndexToSend_0based + i) <= lastLogIndex_0based; ++i) {
            int current_absolute_idx = nextLogIndexToSend_0based + i;
            int deque_idx = current_absolute_idx - logStartIndex_0based;
            const LogEntry& entry = log[deque_idx];
            batch_ids.push_back({entry.value, entry.term});
        }

        if (batch_ids.empty()) return;

        AppendEntriesNetAggMsg msg;
        msg.term = currentTerm;
        msg.prevLogIndex = nextLogIndexToSend_0based; // This is 0-based for log, but protocol expects 1-based for index > 0
        msg.prevLogTerm = 0;
        if (msg.prevLogIndex > 0) {
            int prev_deque_idx = (msg.prevLogIndex - 1) - logStartIndex_0based;
            // This should always be a valid index on the leader
            if (prev_deque_idx >= 0 && prev_deque_idx < static_cast<int>(log.size())) {
                msg.prevLogTerm = log[prev_deque_idx].term;
            }
        }

        msg.firstEntryIndex = nextLogIndexToSend_0based + 1;
        msg.batchedEntryIds = serialize_batched_ids(batch_ids);
        msg.commitIndex = commitIndex;
        msg.source = rank;

        std::string data_str = std::to_string(msg.term) + "|" +
                              std::to_string(msg.prevLogIndex) + "|" +
                              std::to_string(msg.prevLogTerm) + "|" +
                              std::to_string(msg.firstEntryIndex) + "|" +
                              msg.batchedEntryIds + "|" +
                              std::to_string(msg.commitIndex) + "|" +
                              std::to_string(msg.source);

        int msg_len = data_str.length() + 1;
        char* send_buffer = new char[msg_len];
        strncpy(send_buffer, data_str.c_str(), msg_len);

        MPI_Request mpi_req;
        MPI_Isend(send_buffer, msg_len, MPI_CHAR, NETAGG_RANK,
                  APPEND_ENTRIES_NETAGG_REQUEST, MPI_COMM_WORLD, &mpi_req);

        outstandingSends.push_back({mpi_req, send_buffer});

        lastSentLogIndexToNetAgg += batch_ids.size();
    }
}

void Leader::sendBatchedClientResponses() {
    // 1. If the buffer is empty, there's nothing to do.
    if (clientResponseBuffer.empty()) {
        return;
    }

    // 2. If the timer is not set, start it for this new batch.
    if (clientResponseBatchTimer == std::chrono::high_resolution_clock::time_point{}) {
        clientResponseBatchTimer = std::chrono::high_resolution_clock::now();
    }

    // 3. Decide whether to send the batch now.
    auto now = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - clientResponseBatchTimer);

    // BATCH_TIMEOUT_MS is defined in follower.cpp
    bool batchIsFull = clientResponseBuffer.size() >= BATCH_SIZE;
    bool timeoutReached = elapsed.count() >= BATCH_TIMEOUT_MS;

    if (!batchIsFull && !timeoutReached) {
        return; // Wait for more items or timeout.
    }

    // 4. Time to send.
    size_t count_to_send = clientResponseBuffer.size();

    std::string response_str = "SUCCESS";
    for (size_t i = 0; i < count_to_send; ++i) {
        const auto& entry = clientResponseBuffer[i];
        response_str += "|" + std::to_string(entry.value) + "|" + entry.payload;
    }

    int msg_len = response_str.length() + 1;
    char* send_buffer = new char[msg_len];
    strncpy(send_buffer, response_str.c_str(), msg_len);

    MPI_Request mpi_req;
    MPI_Isend(send_buffer, msg_len, MPI_CHAR, CLIENT_RANK,
              CLIENT_RESPONSE, MPI_COMM_WORLD, &mpi_req);

    outstandingSends.push_back({mpi_req, send_buffer});

    // 5. Clean up and reset for the next batch.
    clientResponseBuffer.erase(clientResponseBuffer.begin(), clientResponseBuffer.begin() + count_to_send);
    clientResponseBatchTimer = std::chrono::high_resolution_clock::time_point{};
}

void Leader::checkCompletedSends() {
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

void Leader::handleSwitchReplicate(MPI_Status& status) {
    int msg_size;
    MPI_Get_count(&status, MPI_CHAR, &msg_size);
    std::vector<char> buffer(msg_size);

    MPI_Request request;
    MPI_Irecv(buffer.data(), msg_size, MPI_CHAR, SWITCH_RANK, SWITCH_REPLICATE, MPI_COMM_WORLD, &request);
    MPI_Wait(&request, MPI_STATUS_IGNORE);

    std::string data(buffer.data(), msg_size - 1);
    
    std::vector<std::string> parts = split_string(data, '|');
    if (parts.size() < 4) { return; }
    
    RequestID rid;
    rid.value = std::stoi(parts[0]);
    rid.term = std::stoi(parts[1]);
    std::string payload = parts[2];
    int clientRank = std::stoi(parts[3]);

    if (requestBuffer.find(rid) == requestBuffer.end()) {
        requestBuffer[rid] = LogEntry(rid.term, rid.value, payload, clientRank);
        unorderedRequests.insert(rid);
    }
}

void Leader::processUnorderedRequests() {
    auto it = unorderedRequests.begin();
    while (it != unorderedRequests.end()) {
        const RequestID& rid_from_switch = *it;
        auto buffer_it = requestBuffer.find(rid_from_switch);
        if (buffer_it != requestBuffer.end()) {
            LogEntry entry_data = buffer_it->second;
            entry_data.term = currentTerm;

            // Append to log and manage fixed size
            log.push_back(entry_data);
            if (log.size() > LOG_MAX_SIZE) {
                log.pop_front();
                logStartIndex_0based++;
            }

            requestBuffer.erase(buffer_it);
            it = unorderedRequests.erase(it);
        } else {
            it = unorderedRequests.erase(it);
        }
    }
}

void Leader::handleAggCommit(MPI_Status& status) {
    int msg_size;
    MPI_Get_count(&status, MPI_CHAR, &msg_size);
    std::vector<char> buffer(msg_size);

    MPI_Request request;
    MPI_Irecv(buffer.data(), msg_size, MPI_CHAR, NETAGG_RANK, AGG_COMMIT, MPI_COMM_WORLD, &request);
    MPI_Wait(&request, MPI_STATUS_IGNORE);

    std::string data(buffer.data(), msg_size - 1);

    std::vector<std::string> commits = split_string(data, ';');
    if (commits.empty()) return;

    int maxCommitIndexInBatch_0based = -1;
    for (const auto& commit_str : commits) {
        if (commit_str.empty()) continue;
        std::vector<std::string> parts = split_string(commit_str, ',');
        if (parts.size() < 2) continue;

        int newCommitIndex_0based = std::stoi(parts[0]) - 1;
        if (newCommitIndex_0based > maxCommitIndexInBatch_0based) {
            maxCommitIndexInBatch_0based = newCommitIndex_0based;
        }
    }

    if (maxCommitIndexInBatch_0based > commitIndex) {
        int old_commit_idx = commitIndex;
        int last_log_idx_0based = log.empty() ? -1 : logStartIndex_0based + log.size() - 1;
        commitIndex = std::min(maxCommitIndexInBatch_0based, last_log_idx_0based);

        for (int i = old_commit_idx + 1; i <= commitIndex; ++i) {
            // Check if this committed entry is still in our log window
            if (i >= logStartIndex_0based) {
                int deque_idx = i - logStartIndex_0based;
                if (log[deque_idx].clientRank == rank) {
                    clientResponseBuffer.push_back(log[deque_idx]);
                }
            }
        }
    }
}
