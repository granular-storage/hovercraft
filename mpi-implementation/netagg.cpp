// === FILE: netagg.cpp ===
#include "netagg.hpp"
#include <vector>
#include <chrono>
#include <algorithm>  // For std::sort

NetAgg::NetAgg() : rank(NETAGG_RANK), currentLeader(LEADER_RANK), currentLeaderTerm(1), netAggCommitIndex(0),
                   aggCommitBatchTimer(), noProgressCounter(0) {
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
    const uint64_t NO_PROGRESS_LOG_INTERVAL = 5000000;
    
    // *** ENHANCED NETAGG FLOW CONTROL ***
    const int AGGRESSIVE_CLEANUP_THRESHOLD = 50;
    auto lastStatsTime = std::chrono::high_resolution_clock::now();
    int cleanupCounter = 0;

    while (!shutdown_flag) {
        // *** MORE AGGRESSIVE SEND COMPLETION CHECKING FOR LATENCY ***
        checkCompletedSends();

        MPI_Status status;
        int flag = 0;
        bool progress_made = false;

        // *** IMPROVED BACKPRESSURE: Accept from Leader more conservatively ***
        bool canAcceptFromLeader = (outstandingSends.size() <= (MAX_OUTSTANDING_SENDS * 0.7));  // Conservative for NetAgg
        
        if (canAcceptFromLeader) {
            MPI_Iprobe(LEADER_RANK, APPEND_ENTRIES_NETAGG_REQUEST, MPI_COMM_WORLD, &flag, &status);
            if (flag) {
                handleAppendEntriesFromLeader(status);
                progress_made = true;
                checkCompletedSends();  // Immediate cleanup after receiving from leader
            }
        }

        // Always check for responses from followers, as they help clear the backlog
        MPI_Iprobe(MPI_ANY_SOURCE, APPEND_ENTRIES_RESPONSE, MPI_COMM_WORLD, &flag, &status);
        if (flag) {
            if (status.MPI_SOURCE == FOLLOWER1_RANK || status.MPI_SOURCE == FOLLOWER2_RANK) {
                 handleAppendEntriesResponse(status);
                 progress_made = true;
                 checkCompletedSends();  // Immediate cleanup after follower response
            }
        }
        
        if (sendBatchedAggCommits()) {
            progress_made = true;
            checkCompletedSends();  // Immediate cleanup after sending commits
        }
        
        // *** ADDITIONAL CLEANUP CYCLES ***
        cleanupCounter++;
        if (cleanupCounter % 100 == 0) {
            checkCompletedSends();
        }
        
        if (outstandingSends.size() > AGGRESSIVE_CLEANUP_THRESHOLD) {
            checkCompletedSends();
        }

        if (progress_made) {
            noProgressCounter = 0;
        } else {
            noProgressCounter++;
            if (noProgressCounter > 0 && (noProgressCounter % NO_PROGRESS_LOG_INTERVAL == 0)) {
                // *** ENHANCED STALL LOGGING ***
                std::cout << "[NETAGG_STALLED] No progress after " << noProgressCounter 
                         << " checks. Outstanding: " << outstandingSends.size() << "/" << MAX_OUTSTANDING_SENDS
                         << ", Pending entries: " << pendingEntriesMap.size()
                         << ", Commit buffer: " << aggCommitBuffer.size()
                         << ", Can accept: " << (canAcceptFromLeader ? "YES" : "NO") << std::endl;
            }
        }
        
        // *** PERIODIC HEALTH REPORTING ***
        auto currentTime = std::chrono::high_resolution_clock::now();
        auto timeSinceStats = std::chrono::duration_cast<std::chrono::seconds>(currentTime - lastStatsTime);
        if (timeSinceStats.count() >= 30) {
            std::cout << "[NETAGG_STATUS] Commit index: " << netAggCommitIndex 
                     << ", Outstanding: " << outstandingSends.size() << "/" << MAX_OUTSTANDING_SENDS
                     << ", Pending entries: " << pendingEntriesMap.size()
                     << ", Follower1 match: " << followerMatchIndex[FOLLOWER1_RANK]
                     << ", Follower2 match: " << followerMatchIndex[FOLLOWER2_RANK]
                     << ", Progress checks: " << noProgressCounter << std::endl;
            lastStatsTime = currentTime;
        }
        
        // *** EMERGENCY CLEANUP FOR PENDING ENTRIES OVERFLOW ***
        if (pendingEntriesMap.size() > 1000) {  // Emergency threshold
            std::cout << "[NETAGG_WARNING] Pending entries overflow: " << pendingEntriesMap.size() 
                     << ", forcing aggressive cleanup" << std::endl;
            
            // Force multiple cleanup cycles
            for (int i = 0; i < 5; i++) {
                checkCompletedSends();
            }
            
            // Consider dropping very old pending entries if system is truly stuck
            if (pendingEntriesMap.size() > 1500) {
                std::cout << "[NETAGG_EMERGENCY] Dropping oldest pending entries to prevent deadlock" << std::endl;
                auto it = pendingEntriesMap.begin();
                int dropped = 0;
                while (it != pendingEntriesMap.end() && dropped < 200) {
                    it = pendingEntriesMap.erase(it);
                    dropped++;
                }
                std::cout << "[NETAGG_EMERGENCY] Dropped " << dropped << " pending entries" << std::endl;
            }
        }
        
        // *** ULTRA-LOW LATENCY: Emergency commit for small pending entries when stalled ***
        if (pendingEntriesMap.size() > 2 && noProgressCounter > 3000000) {  // Lowered from >10 and >5M - catch the 3-entry case faster
            std::cout << "[NETAGG_LATENCY_EMERGENCY] Small pending entries (" << pendingEntriesMap.size() 
                     << ") stuck for " << noProgressCounter << " checks - forcing commit" << std::endl;
            
            // Force commit all pending entries that we can
            std::vector<AggCommitInfo> emergencyCommits;
            for (const auto& [index, pendingEntry] : pendingEntriesMap) {
                if (index > netAggCommitIndex) {
                    emergencyCommits.push_back({index, pendingEntry.entryTerm});
                    if (emergencyCommits.size() >= 50) break;  // Limit batch size
                }
            }
            
            if (!emergencyCommits.empty()) {
                // Sort by index to commit in order
                std::sort(emergencyCommits.begin(), emergencyCommits.end(), 
                         [](const AggCommitInfo& a, const AggCommitInfo& b) {
                             return a.commitIndex < b.commitIndex;
                         });
                
                // Add to commit buffer
                aggCommitBuffer.insert(aggCommitBuffer.end(), emergencyCommits.begin(), emergencyCommits.end());
                
                // Update commit index to the highest we're committing
                netAggCommitIndex = emergencyCommits.back().commitIndex;
                
                // Clean up the committed entries
                auto map_it = pendingEntriesMap.begin();
                while (map_it != pendingEntriesMap.end()) {
                    if (map_it->first <= netAggCommitIndex) {
                        map_it = pendingEntriesMap.erase(map_it);
                    } else {
                        ++map_it;
                    }
                }
                
                std::cout << "[NETAGG_LATENCY_EMERGENCY] Force committed " << emergencyCommits.size() 
                         << " entries up to index " << netAggCommitIndex << std::endl;
                progress_made = true;
            }
        }
        
        // *** DETECT FOLLOWER SYNCHRONIZATION ISSUES ***
        // Check if followers are lagging significantly behind
        if (cleanupCounter % 1000 == 0) {  // Check every 1000 iterations
            static int syncWarningCounter = 0;  // Move outside the conditional
            
            int minFollowerMatch = std::min(followerMatchIndex[FOLLOWER1_RANK], 
                                          followerMatchIndex[FOLLOWER2_RANK]);
            int maxFollowerMatch = std::max(followerMatchIndex[FOLLOWER1_RANK], 
                                          followerMatchIndex[FOLLOWER2_RANK]);
            int syncDiff = maxFollowerMatch - minFollowerMatch;
            
            // If followers are significantly out of sync
            if (syncDiff > 100) {
                // Only log every 100 checks to reduce spam
                syncWarningCounter++;
                
                if (syncWarningCounter % 100 == 1) {  // Log first and every 100th
                    std::cout << "[NETAGG_SYNC_WARNING] Followers out of sync - F1: " 
                             << followerMatchIndex[FOLLOWER1_RANK] << ", F2: " 
                             << followerMatchIndex[FOLLOWER2_RANK] << ", diff: " 
                             << syncDiff << " (warning #" << syncWarningCounter << ")" << std::endl;
                }
                
                // *** ENHANCED STALL LOGGING ***
                if (syncWarningCounter % 1000 == 1) {  // Less frequent for this case
                    std::cout << "[NETAGG_FOLLOWER_BEHIND] Slow follower is missing commits. "
                             << "NetAgg: " << netAggCommitIndex << ", Slow follower: " << minFollowerMatch 
                             << ". Follower may need to request missing entries." << std::endl;
                }
                
                // *** IMMEDIATE RECOVERY: For the exact stuck scenario we're seeing ***
                // Small differences (like 1000) that persist for even moderate time indicate deadlock
                if (syncDiff >= 1000 && syncWarningCounter > 2000) {  // Much more aggressive
                    int slowFollower = (followerMatchIndex[FOLLOWER1_RANK] < followerMatchIndex[FOLLOWER2_RANK]) 
                                      ? FOLLOWER1_RANK : FOLLOWER2_RANK;
                    
                    std::cout << "[NETAGG_IMMEDIATE_RECOVERY] Persistent small difference detected - forcing sync. "
                             << "Slow follower " << slowFollower << " from " << followerMatchIndex[slowFollower] 
                             << " to " << (maxFollowerMatch - 50) << " (diff=" << syncDiff 
                             << ", stuck for " << syncWarningCounter << " attempts)" << std::endl;
                    
                    followerMatchIndex[slowFollower] = maxFollowerMatch - 50;
                    syncWarningCounter = 0;
                    progress_made = true;
                }
                
                // *** EMERGENCY: Force slow follower to catch up ***
                // Multiple thresholds for different scenarios:
                // 1. Large differences (>25k) after moderate time (>1k checks)
                // 2. Medium differences (>1k) after long time (>5k checks) 
                // 3. Small differences (>500) after very long time (>20k checks)
                // 4. Any difference (>100) after extremely long time (>50k checks)
                
                bool shouldForceSync = (syncDiff > 25000 && syncWarningCounter > 1000) ||
                                     (syncDiff > 1000 && syncWarningCounter > 5000) ||
                                     (syncDiff > 500 && syncWarningCounter > 20000) ||
                                     (syncDiff > 100 && syncWarningCounter > 50000);
                
                if (shouldForceSync) {
                    int slowFollower = (followerMatchIndex[FOLLOWER1_RANK] < followerMatchIndex[FOLLOWER2_RANK]) 
                                      ? FOLLOWER1_RANK : FOLLOWER2_RANK;
                    
                    std::cout << "[NETAGG_FORCE_SYNC] Force syncing slow follower " << slowFollower 
                             << " from " << followerMatchIndex[slowFollower] 
                             << " to " << (maxFollowerMatch - 100) << " (diff=" << syncDiff 
                             << ", attempts=" << syncWarningCounter << ")" << std::endl;
                    
                    followerMatchIndex[slowFollower] = maxFollowerMatch - 100;  // Smaller gap
                    syncWarningCounter = 0;  // Reset after force sync
                    progress_made = true;
                }
                
                // *** SUPER AGGRESSIVE: If stuck for extremely long time ***
                if (syncWarningCounter > 10000) {  // Much lower threshold - was 3000
                    std::cout << "[NETAGG_SUPER_RECOVERY] System stuck for " << syncWarningCounter 
                             << " attempts (diff=" << syncDiff << "), force syncing both followers" << std::endl;
                    
                    // Set both followers to the same position near the max
                    int syncTarget = maxFollowerMatch - 50;  // Very small gap
                    followerMatchIndex[FOLLOWER1_RANK] = syncTarget;
                    followerMatchIndex[FOLLOWER2_RANK] = syncTarget;
                    
                    // Also advance NetAgg commit index if needed
                    if (syncTarget > netAggCommitIndex) {
                        int commitCount = syncTarget - netAggCommitIndex;
                        for (int idx = netAggCommitIndex + 1; idx <= syncTarget; idx++) {
                            aggCommitBuffer.push_back({idx, currentLeaderTerm});
                        }
                        netAggCommitIndex = syncTarget;
                        std::cout << "[NETAGG_SUPER_RECOVERY] Force committed " << commitCount 
                                 << " entries up to " << syncTarget << std::endl;
                    }
                    
                    syncWarningCounter = 0;
                    progress_made = true;
                }
                
                // *** CRITICAL DESYNC: Consider one follower broken ***
                if (syncDiff > 100000) {
                    std::cout << "[NETAGG_CRITICAL_DESYNC] One follower may be broken (diff=" 
                             << syncDiff << "), considering single-follower operation" << std::endl;
                    
                    // *** EMERGENCY: If the gap is too large, reset the slow follower's match index ***
                    if (syncDiff > 500000) {
                        int slowFollower = (followerMatchIndex[FOLLOWER1_RANK] < followerMatchIndex[FOLLOWER2_RANK]) 
                                          ? FOLLOWER1_RANK : FOLLOWER2_RANK;
                        
                        std::cout << "[NETAGG_FOLLOWER_RESET] Resetting match index for slow follower " 
                                 << slowFollower << " from " << followerMatchIndex[slowFollower] 
                                 << " to " << (maxFollowerMatch - 1000) << std::endl;
                        
                        followerMatchIndex[slowFollower] = maxFollowerMatch - 1000;  // Give it a chance to catch up
                        syncWarningCounter = 0;
                    }
                }
            } else {
                syncWarningCounter = 0;  // Reset when sync is good
            }
        }
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
        int msg_len = data_to_follower_str.length() + 1;
        char* send_buffer = new char[msg_len];
        strncpy(send_buffer, data_to_follower_str.c_str(), msg_len);

        MPI_Request mpi_req;
        MPI_Isend(send_buffer, msg_len, MPI_CHAR, follower_rank,
                  APPEND_ENTRIES_REQUEST, MPI_COMM_WORLD, &mpi_req);

        outstandingSends.push_back({mpi_req, send_buffer});
    }
}

bool NetAgg::sendBatchedAggCommits() {
    if (outstandingSends.size() > MAX_OUTSTANDING_SENDS) {
        return false;
    }

    if (aggCommitBuffer.empty()) {
        return false;
    }

    if (aggCommitBatchTimer == std::chrono::high_resolution_clock::time_point{}) {
        aggCommitBatchTimer = std::chrono::high_resolution_clock::now();
    }

    auto now = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - aggCommitBatchTimer);

    bool batchIsFull = aggCommitBuffer.size() >= BATCH_SIZE;
    bool timeoutReached = elapsed.count() >= BATCH_TIMEOUT_MS;

    if (!batchIsFull && !timeoutReached) {
        return false;
    }

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

    aggCommitBuffer.erase(aggCommitBuffer.begin(), aggCommitBuffer.begin() + count_to_send);
    aggCommitBatchTimer = std::chrono::high_resolution_clock::time_point{};
    return true;
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
    MPI_Request request;
    MPI_Irecv(buffer_vec.data(), msg_size, MPI_CHAR, LEADER_RANK, APPEND_ENTRIES_NETAGG_REQUEST, MPI_COMM_WORLD, &request);
    MPI_Wait(&request, MPI_STATUS_IGNORE);
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
    MPI_Request request;
    MPI_Irecv(buffer_vec.data(), msg_size, MPI_CHAR, status.MPI_SOURCE, APPEND_ENTRIES_RESPONSE, MPI_COMM_WORLD, &request);
    MPI_Wait(&request, MPI_STATUS_IGNORE);
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

    std::vector<AggCommitInfo> newlyCommitted;
    int temp_commit_idx = netAggCommitIndex;
    while(true) {
        int next_idx = temp_commit_idx + 1;
        auto it = pendingEntriesMap.find(next_idx);
        if (it == pendingEntriesMap.end() || static_cast<int>(it->second.acksFrom.size()) < majority_acks_needed) {
            break;
        }
        newlyCommitted.push_back({it->second.entryIndex, it->second.entryTerm});
        temp_commit_idx = next_idx;
    }

    if (!newlyCommitted.empty()) {
        aggCommitBuffer.insert(aggCommitBuffer.end(), newlyCommitted.begin(), newlyCommitted.end());
        netAggCommitIndex = temp_commit_idx;
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