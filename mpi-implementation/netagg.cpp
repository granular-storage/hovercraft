// === FILE: netagg.cpp ===

#include "netagg.hpp"
#include <vector>

NetAgg::NetAgg() : rank(NETAGG_RANK), currentLeader(LEADER_RANK), currentLeaderTerm(1), netAggCommitIndex(0),
                   aggCommitBatchTimer() {
    followerMatchIndex[FOLLOWER1_RANK] = 0;
    followerMatchIndex[FOLLOWER2_RANK] = 0;
}

NetAgg::~NetAgg() {
    for (auto& send : outstandingSends) {
        delete[] send.buffer;
    }
}

void NetAgg::run(bool& shutdown_flag) {
    log_debug("NETAGG", "Started with rank " + std::to_string(rank));

    while (!shutdown_flag) {
        MPI_Status status;
        int flag = 0;

        // todo Add a probe for the shutdown signal but only consider it when we processed
        // the total number of requests expected to process
        // MPI_Iprobe(MPI_ANY_SOURCE, SHUTDOWN_SIGNAL, MPI_COMM_WORLD, &flag, &status);
        // if (flag) {
        //     shutdown_flag = true;
        //     continue; 
        // }

        MPI_Iprobe(LEADER_RANK, APPEND_ENTRIES_NETAGG_REQUEST, MPI_COMM_WORLD, &flag, &status);
        if (flag) { handleAppendEntriesFromLeader(status); }

        MPI_Iprobe(MPI_ANY_SOURCE, APPEND_ENTRIES_RESPONSE, MPI_COMM_WORLD, &flag, &status);
        if (flag) {
            if (status.MPI_SOURCE == FOLLOWER1_RANK || status.MPI_SOURCE == FOLLOWER2_RANK) {
                 handleAppendEntriesResponse(status);
            }
        }
        
        sendBatchedAggCommits();
        checkCompletedSends();
    }
}

void NetAgg::forwardToFollowers(const AppendEntriesNetAggMsg& leaderMsg, const std::vector<RequestID>& batch_rids) {
    std::vector<int> followers = {FOLLOWER1_RANK, FOLLOWER2_RANK};
    std::string data_to_follower_str =
        std::to_string(leaderMsg.term) + "|" +
        std::to_string(leaderMsg.prevLogIndex) + "|" +
        std::to_string(leaderMsg.prevLogTerm) + "|" +
        std::to_string(leaderMsg.firstEntryIndex) + "|" +
        leaderMsg.batchedEntryIds + "|" +
        std::to_string(leaderMsg.commitIndex) + "|" +
        std::to_string(leaderMsg.source) + "|" +
        std::to_string(rank);

    for (int follower_rank : followers) {
        // --- NON-BLOCKING SEND ---
        int msg_len = data_to_follower_str.length() + 1;
        char* send_buffer = new char[msg_len];
        strncpy(send_buffer, data_to_follower_str.c_str(), msg_len);

        MPI_Request mpi_req;
        MPI_Isend(send_buffer, msg_len, MPI_CHAR, follower_rank,
                  APPEND_ENTRIES_REQUEST, MPI_COMM_WORLD, &mpi_req);

        outstandingSends.push_back({mpi_req, send_buffer});
        // --- END NON-BLOCKING SEND ---
    }
    //log_debug("NETAGG", "Initiated forward of AppendEntries to " + std::to_string(followers.size()) + " followers.");
}

void NetAgg::sendBatchedAggCommits() {
    // 1. If the buffer is empty, there's nothing to do.
    if (aggCommitBuffer.empty()) {
        return;
    }

    // 2. If the timer is not set, start it for this new batch.
    if (aggCommitBatchTimer == std::chrono::high_resolution_clock::time_point{}) {
        aggCommitBatchTimer = std::chrono::high_resolution_clock::now();
    }

    // 3. Decide whether to send the batch now.
    auto now = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - aggCommitBatchTimer);

    // BATCH_TIMEOUT_MS is defined in follower.cpp
    bool batchIsFull = aggCommitBuffer.size() >= BATCH_SIZE;
    bool timeoutReached = elapsed.count() >= BATCH_TIMEOUT_MS;

    if (!batchIsFull && !timeoutReached) {
        return; // Wait for more items or timeout.
    }

    // 4. Time to send.
    size_t count_to_send = aggCommitBuffer.size();

    std::string payload;
    for (size_t i = 0; i < count_to_send; ++i) {
        payload += std::to_string(aggCommitBuffer[i].commitIndex) + "," + std::to_string(aggCommitBuffer[i].term);
        if (i < count_to_send - 1) {
            payload += ";";
        }
    }

    std::vector<int> all_servers = {LEADER_RANK, FOLLOWER1_RANK, FOLLOWER2_RANK};
    for (int server_rank : all_servers) {
        int msg_len = payload.length() + 1;
        char* send_buffer = new char[msg_len];
        strncpy(send_buffer, payload.c_str(), msg_len);

        MPI_Request mpi_req;
        MPI_Isend(send_buffer, msg_len, MPI_CHAR, server_rank,
                  AGG_COMMIT, MPI_COMM_WORLD, &mpi_req);

        outstandingSends.push_back({mpi_req, send_buffer});
    }

    log_debug("NETAGG", "Initiated batched AggCommit for " + std::to_string(count_to_send) + " indices, up to index " + std::to_string(aggCommitBuffer[count_to_send-1].commitIndex));

    // 5. Clean up and reset for the next batch.
    aggCommitBuffer.erase(aggCommitBuffer.begin(), aggCommitBuffer.begin() + count_to_send);
    aggCommitBatchTimer = std::chrono::high_resolution_clock::time_point{};
}

void NetAgg::checkCompletedSends() {
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

void NetAgg::handleAppendEntriesFromLeader(MPI_Status& status) {
    int msg_size;
    MPI_Get_count(&status, MPI_CHAR, &msg_size);
    std::vector<char> buffer_vec(msg_size);
    // --- NON-BLOCKING RECEIVE UPDATE ---
    MPI_Request request;
    MPI_Irecv(buffer_vec.data(), msg_size, MPI_CHAR, LEADER_RANK, APPEND_ENTRIES_NETAGG_REQUEST, MPI_COMM_WORLD, &request);
    MPI_Wait(&request, MPI_STATUS_IGNORE);
    // --- END UPDATE ---
    std::string data(buffer_vec.data(), msg_size - 1);
    
    std::vector<std::string> parts = split_string(data, '|');
    if (parts.size() < 7) return;

    AppendEntriesNetAggMsg leaderMsg;
    std::vector<RequestID> batch_rids;
    leaderMsg.term = std::stoi(parts[0]);
    leaderMsg.prevLogIndex = std::stoi(parts[1]);
    leaderMsg.prevLogTerm = std::stoi(parts[2]);
    leaderMsg.firstEntryIndex = std::stoi(parts[3]);
    leaderMsg.batchedEntryIds = parts[4];
    batch_rids = deserialize_batched_ids(leaderMsg.batchedEntryIds);
    leaderMsg.commitIndex = std::stoi(parts[5]);
    leaderMsg.source = std::stoi(parts[6]);

    if (leaderMsg.term < currentLeaderTerm) return;
    if (leaderMsg.term > currentLeaderTerm) {
        currentLeaderTerm = leaderMsg.term;
        currentLeader = leaderMsg.source;
        pendingEntriesMap.clear();
    }

    for (size_t i = 0; i < batch_rids.size(); ++i) {
        int idx_1based = leaderMsg.firstEntryIndex + i;
        if (idx_1based <= netAggCommitIndex) continue;
        if (pendingEntriesMap.find(idx_1based) == pendingEntriesMap.end()) {
            pendingEntriesMap.emplace(idx_1based, PendingEntry(idx_1based, batch_rids[i].term, leaderMsg.source));
        }
    }

    if (!batch_rids.empty()) {
        forwardToFollowers(leaderMsg, batch_rids);
    }
}

void NetAgg::handleAppendEntriesResponse(MPI_Status& status) {
    int msg_size;
    MPI_Get_count(&status, MPI_CHAR, &msg_size);
    std::vector<char> buffer_vec(msg_size);
    // --- NON-BLOCKING RECEIVE UPDATE ---
    MPI_Request request;
    MPI_Irecv(buffer_vec.data(), msg_size, MPI_CHAR, status.MPI_SOURCE, APPEND_ENTRIES_RESPONSE, MPI_COMM_WORLD, &request);
    MPI_Wait(&request, MPI_STATUS_IGNORE);
    // --- END UPDATE ---
    std::string data(buffer_vec.data(), msg_size - 1);
    
    std::vector<std::string> parts = split_string(data, '|');
    if (parts.size() < 4) return;

    AppendEntriesResponseMsg resp;
    resp.success = (parts[1] == "1");
    if (!resp.success) return;

    resp.matchIndex = std::stoi(parts[2]);
    resp.source = std::stoi(parts[3]);
    followerMatchIndex[resp.source] = resp.matchIndex;

    int majority_acks_needed = 1;

    for (auto& pair : pendingEntriesMap) {
        if (resp.matchIndex >= pair.second.entryIndex) {
            pair.second.acksFrom.insert(resp.source);
        }
    }

    // Find all newly committable entries and buffer them for batched sending.
    std::vector<AggCommitInfo> newlyCommitted;
    int temp_commit_idx = netAggCommitIndex;
    while(true) {
        int next_idx = temp_commit_idx + 1;
        auto it = pendingEntriesMap.find(next_idx);
        if (it == pendingEntriesMap.end() || static_cast<int>(it->second.acksFrom.size()) < majority_acks_needed) {
            break;
        }
        // This entry is now committable.
        newlyCommitted.push_back({it->second.entryIndex, it->second.entryTerm});
        temp_commit_idx = next_idx;
    }

    if (!newlyCommitted.empty()) {
        // Add all newly found committable entries to the main buffer.
        aggCommitBuffer.insert(aggCommitBuffer.end(), newlyCommitted.begin(), newlyCommitted.end());

        // Update the state to reflect the new high-water mark.
        netAggCommitIndex = temp_commit_idx;

        //log_debug("NETAGG", "Buffered " + std::to_string(newlyCommitted.size()) + " new commits. New commit index: " + std::to_string(netAggCommitIndex));

        // Prune the map of all entries that are now committed.
        auto map_it = pendingEntriesMap.begin();
        while (map_it != pendingEntriesMap.end()) {
            if (map_it->first <= netAggCommitIndex) {
                map_it = pendingEntriesMap.erase(map_it);
            } else {
                ++map_it;
            }
        }
    }
}
