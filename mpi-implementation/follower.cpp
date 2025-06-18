// === FILE: follower.cpp ===

#include "follower.hpp"
#include <vector>

Follower::Follower(int r) : rank(r), currentTerm(1), logStartIndex_0based(0), commitIndex(-1),
                            clientResponseBatchTimer(), append_count(0) {}

Follower::~Follower() {
    for (auto& send : outstandingSends) {
        delete[] send.buffer;
    }
}

void Follower::run(bool& shutdown_flag) {
    log_debug("FOLLOWER" + std::to_string(rank), "Started with rank " + std::to_string(rank));

    while (!shutdown_flag) {
        MPI_Status status;
        int flag = 0;

        MPI_Iprobe(SWITCH_RANK, SWITCH_REPLICATE, MPI_COMM_WORLD, &flag, &status);
        if (flag) { handleSwitchReplicate(status); }

        MPI_Iprobe(NETAGG_RANK, APPEND_ENTRIES_REQUEST, MPI_COMM_WORLD, &flag, &status);
        if (flag) { handleAppendEntries(status); }

        MPI_Iprobe(NETAGG_RANK, AGG_COMMIT, MPI_COMM_WORLD, &flag, &status);
        if (flag) { handleAggCommit(status); }
        
        sendBatchedClientResponses();
        checkCompletedSends();
    }
}

// --- FIX: Corrected safety check for pruning ---
void Follower::pruneRequestBuffer() {
	return;
//    const int RETAIN_WINDOW = 1000000;
//
//    // THE FIX IS HERE: This check is now correct.
//    // We only prune if the commit index has advanced far enough to define a meaningful "stale" window.
//    // If commitIndex is less than or equal to the window size, the threshold would be <= 0.
//    if (commitIndex <= RETAIN_WINDOW) {
//        return;
//    }
//
//    int prune_threshold = commitIndex - RETAIN_WINDOW;
//    int pruned_count = 0;
//
//    auto it = requestBuffer.begin();
//    while (it != requestBuffer.end()) {
//        if (it->first < prune_threshold) {
//            unorderedRequestValues.erase(it->first);
//            it = requestBuffer.erase(it);
//            pruned_count++;
//        } else {
//            ++it;
//        }
//    }
//    if (pruned_count > 0) {
//        log_debug("FOLLOWER" + std::to_string(rank), "Pruned " + std::to_string(pruned_count) + " old entries from request buffer.");
//    }
}

void Follower::sendAppendEntriesResponse(bool success, int matchIdx_1based) {
    std::string data_str = std::to_string(currentTerm) + "|" +
                          (success ? "1" : "0") + "|" +
                          std::to_string(matchIdx_1based) + "|" +
                          std::to_string(rank);

    int msg_len = data_str.length() + 1;
    char* send_buffer = new char[msg_len];
    strncpy(send_buffer, data_str.c_str(), msg_len);

    MPI_Request mpi_req;
    MPI_Isend(send_buffer, msg_len, MPI_CHAR, NETAGG_RANK,
              APPEND_ENTRIES_RESPONSE, MPI_COMM_WORLD, &mpi_req);

    outstandingSends.push_back({mpi_req, send_buffer});
}

void Follower::sendBatchedClientResponses() {
    if (clientResponseBuffer.empty()) {
        return;
    }
    if (clientResponseBatchTimer == std::chrono::high_resolution_clock::time_point{}) {
        clientResponseBatchTimer = std::chrono::high_resolution_clock::now();
    }

    auto now = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - clientResponseBatchTimer);

    bool batchIsFull = clientResponseBuffer.size() >= BATCH_SIZE;
    bool timeoutReached = elapsed.count() >= BATCH_TIMEOUT_MS;

    if (!batchIsFull && !timeoutReached) {
        return;
    }

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

    clientResponseBuffer.erase(clientResponseBuffer.begin(), clientResponseBuffer.begin() + count_to_send);
    clientResponseBatchTimer = std::chrono::high_resolution_clock::time_point{};
}

void Follower::checkCompletedSends() {
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

void Follower::handleSwitchReplicate(MPI_Status& status) {
    int msg_size;
    MPI_Get_count(&status, MPI_CHAR, &msg_size);
    std::vector<char> buffer(msg_size);

    MPI_Request request;
    MPI_Irecv(buffer.data(), msg_size, MPI_CHAR, SWITCH_RANK, SWITCH_REPLICATE, MPI_COMM_WORLD, &request);
    MPI_Wait(&request, MPI_STATUS_IGNORE);

    std::string data(buffer.data(), msg_size - 1);

    std::vector<std::string> parts = split_string(data, '|');
    if (parts.size() < 4) return;

    RequestID rid;
    rid.value = std::stoi(parts[0]);
    rid.term = std::stoi(parts[1]);
    std::string payload_str = parts[2];
    int client_rank_val = std::stoi(parts[3]);

    if (requestBuffer.find(rid.value) == requestBuffer.end()) {
        requestBuffer[rid.value] = LogEntry(rid.term, rid.value, payload_str, client_rank_val);
        unorderedRequestValues.insert(rid.value);
    }
}

void Follower::handleAppendEntries(MPI_Status& status) {
    int msg_size;
    MPI_Get_count(&status, MPI_CHAR, &msg_size);
    std::vector<char> buffer_vec(msg_size);

    MPI_Request request;
    MPI_Irecv(buffer_vec.data(), msg_size, MPI_CHAR, NETAGG_RANK, APPEND_ENTRIES_REQUEST, MPI_COMM_WORLD, &request);
    MPI_Wait(&request, MPI_STATUS_IGNORE);

    std::string data(buffer_vec.data(), msg_size - 1);

    std::vector<std::string> parts = split_string(data, '|');
    if (parts.size() < 8) return;
    
    AppendEntriesMsg msg;
    msg.term = std::stoi(parts[0]);
    msg.prevLogIndex = std::stoi(parts[1]);
    msg.prevLogTerm = std::stoi(parts[2]);
    msg.firstEntryIndex = std::stoi(parts[3]);
    std::vector<RequestID> batch_rids = deserialize_batched_ids(parts[4]);
    msg.commitIndex = std::stoi(parts[5]);

    int last_log_idx_0based = log.empty() ? -1 : logStartIndex_0based + log.size() - 1;

    if (msg.term < currentTerm) {
        sendAppendEntriesResponse(false, last_log_idx_0based + 1);
        return;
    }
    currentTerm = msg.term;

    int prevLogIndex_0based = msg.prevLogIndex - 1;
    bool prev_log_ok = (msg.prevLogIndex == 0);
    if (!prev_log_ok && prevLogIndex_0based >= logStartIndex_0based && prevLogIndex_0based <= last_log_idx_0based) {
        int deque_idx = prevLogIndex_0based - logStartIndex_0based;
        if (log[deque_idx].term == msg.prevLogTerm) {
            prev_log_ok = true;
        }
    }

    if (!prev_log_ok) {
        sendAppendEntriesResponse(false, last_log_idx_0based + 1);
        return;
    }

    size_t leader_batch_idx = 0;
    for (; leader_batch_idx < batch_rids.size(); ++leader_batch_idx) {
        int entry_idx_0based = msg.firstEntryIndex - 1 + leader_batch_idx;
        if (entry_idx_0based > last_log_idx_0based || entry_idx_0based < logStartIndex_0based) {
            break;
        }
        int deque_idx = entry_idx_0based - logStartIndex_0based;
        if (log[deque_idx].term != batch_rids[leader_batch_idx].term) {
            log.erase(log.begin() + deque_idx, log.end());
            break;
        }
    }

    int appended_this_call = 0;
    for (size_t i = leader_batch_idx; i < batch_rids.size(); ++i) {
        const auto& rid = batch_rids[i];
        auto it = requestBuffer.find(rid.value);
        if (it == requestBuffer.end()) {
            break;
        }

        LogEntry new_entry = it->second;
        new_entry.term = rid.term;

        log.push_back(new_entry);
        if (log.size() > LOG_MAX_SIZE) {
            log.pop_front();
            logStartIndex_0based++;
        }
        requestBuffer.erase(it);
        unorderedRequestValues.erase(rid.value);
        appended_this_call++;
    }

    if (appended_this_call > 0) {
        long long old_append_count = append_count;
        append_count += appended_this_call;
        const int PRUNE_INTERVAL = 5000;
        if ((append_count / PRUNE_INTERVAL) > (old_append_count / PRUNE_INTERVAL)) {
            pruneRequestBuffer();
        }
    }

    int new_last_log_idx = log.empty() ? -1 : logStartIndex_0based + log.size() - 1;
    sendAppendEntriesResponse(true, new_last_log_idx + 1);

    if (msg.commitIndex > commitIndex) {
        int old_ci = commitIndex;
        commitIndex = std::min(msg.commitIndex, new_last_log_idx);
        for (int i = old_ci + 1; i <= commitIndex; ++i) {
            if (i >= logStartIndex_0based) {
                int deque_idx = i - logStartIndex_0based;
                if (log[deque_idx].clientRank == rank) {
                    clientResponseBuffer.push_back(log[deque_idx]);
                }
            }
        }
    }
}

void Follower::handleAggCommit(MPI_Status& status) {
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
             if (i >= logStartIndex_0based) {
                int deque_idx = i - logStartIndex_0based;
                if (log[deque_idx].clientRank == rank) {
                    clientResponseBuffer.push_back(log[deque_idx]);
                }
            }
        }
    }
}
