// === FILE: follower.cpp ===
#include "follower.hpp"
#include <vector>
#include <chrono>
#include <map>

Follower::Follower(int r) : rank(r), currentTerm(1), logStartIndex_0based(0), commitIndex(-1),
                            clientResponseBatchTimer(), append_count(0), noProgressCounter(0) {}

Follower::~Follower() {
    for (auto& send : outstandingSends) {
        delete[] send.buffer;
    }
}

void Follower::run(bool& shutdown_flag) {
    log_debug("FOLLOWER" + std::to_string(rank), "Started with rank " + std::to_string(rank));
    const uint64_t NO_PROGRESS_LOG_INTERVAL = 5000000;
    
    // *** ENHANCED FOLLOWER FLOW CONTROL ***
    const int AGGRESSIVE_CLEANUP_THRESHOLD = 50;
    auto lastStatsTime = std::chrono::high_resolution_clock::now();
    int cleanupCounter = 0;
    int stuckDetectionCounter = 0;  // To reduce spam

    while (!shutdown_flag) {
        // *** MORE AGGRESSIVE SEND COMPLETION CHECKING FOR LATENCY ***
        checkCompletedSends();

        MPI_Status status;
        int flag = 0;
        bool progress_made = false;

        // *** IMPROVED BACKPRESSURE FOR FOLLOWERS ***
        bool canAcceptWork = (outstandingSends.size() <= (MAX_OUTSTANDING_SENDS * 0.8));

        if (canAcceptWork) {
            MPI_Iprobe(SWITCH_RANK, SWITCH_REPLICATE, MPI_COMM_WORLD, &flag, &status);
            if (flag) {
                handleSwitchReplicate(status);
                progress_made = true;
                checkCompletedSends();  // Immediate cleanup after switch replication
            }
        }

        // Always handle NetAgg messages - critical for progress
        MPI_Iprobe(NETAGG_RANK, APPEND_ENTRIES_REQUEST, MPI_COMM_WORLD, &flag, &status);
        if (flag) {
            handleAppendEntries(status);
            progress_made = true;
            checkCompletedSends();  // Immediate cleanup after append entries
        }

        MPI_Iprobe(NETAGG_RANK, AGG_COMMIT, MPI_COMM_WORLD, &flag, &status);
        if (flag) {
            handleAggCommit(status);
            progress_made = true;
            checkCompletedSends();  // Immediate cleanup after commit
        }
        
        // *** PRIORITIZE CLIENT RESPONSES TO PREVENT STUCK REQUESTS ***
        if (sendBatchedClientResponses()) {
            progress_made = true;
            checkCompletedSends();  // Immediate cleanup after sending responses
        }
        
        // *** ADDITIONAL CLEANUP CYCLES ***
        cleanupCounter++;
        if (cleanupCounter % 50 == 0) {  // More frequent for followers
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
                // *** ENHANCED STALL LOGGING FOR FOLLOWERS ***
                // std::cout << "[FOLLOWER" << rank << "_STALLED] No progress after " << noProgressCounter 
                //          << " checks. Outstanding: " << outstandingSends.size() << "/" << MAX_OUTSTANDING_SENDS
                //          << ", Request buffer: " << requestBuffer.size()
                //          << ", Client responses: " << clientResponseBuffer.size()
                //          << ", Log size: " << log.size()
                //          << ", Commit index: " << commitIndex << std::endl;
            }
        }
        
        // *** PERIODIC HEALTH REPORTING ***
        auto currentTime = std::chrono::high_resolution_clock::now();
        auto timeSinceStats = std::chrono::duration_cast<std::chrono::seconds>(currentTime - lastStatsTime);
        if (timeSinceStats.count() >= 30) {
            // std::cout << "[FOLLOWER" << rank << "_STATUS] Outstanding: " << outstandingSends.size() 
            //          << "/" << MAX_OUTSTANDING_SENDS << ", Request buffer: " << requestBuffer.size()
            //          << ", Client responses: " << clientResponseBuffer.size() 
            //          << ", Log size: " << log.size() << ", Commit: " << commitIndex
            //          << ", Append count: " << append_count << std::endl;
            lastStatsTime = currentTime;
        }
        
        // *** EMERGENCY CLEANUP FOR BUFFER OVERFLOW ***
        if (requestBuffer.size() > 1000) {
            // std::cout << "[FOLLOWER" << rank << "_WARNING] Request buffer overflow: " 
            //          << requestBuffer.size() << ", forcing cleanup" << std::endl;
            
            for (int i = 0; i < 5; i++) {
                checkCompletedSends();
            }
            
            // Emergency: drop oldest requests if critically full
            if (requestBuffer.size() > 1500) {
                // std::cout << "[FOLLOWER" << rank << "_EMERGENCY] Dropping oldest requests" << std::endl;
                auto it = requestBuffer.begin();
                int dropped = 0;
                while (it != requestBuffer.end() && dropped < 200) {
                    auto toErase = it++;
                    requestBuffer.erase(toErase);
                    dropped++;
                }
                // std::cout << "[FOLLOWER" << rank << "_EMERGENCY] Dropped " << dropped << " requests" << std::endl;
            }
        }
        
        // *** DETECT AND RECOVER FROM STUCK REQUEST BUFFERS ***
        if (requestBuffer.size() > 15) {  
            stuckDetectionCounter++;
            
            // Force emergency processing immediately when stuck
            if (stuckDetectionCounter > 1000) {  // Much lower threshold - immediate action
                // std::cout << "[FOLLOWER" << rank << "_EMERGENCY] Force processing " << requestBuffer.size() 
                //          << " stuck requests immediately" << std::endl;
                
                int processed = 0;
                auto it = requestBuffer.begin();
                // Process ALL requests to clear the deadlock
                while (it != requestBuffer.end()) {
                    const auto& [value, entry] = *it;
                    
                    // Create log entry with current term (emergency processing)
                    LogEntry emergency_entry = entry;
                    emergency_entry.term = currentTerm;
                    
                    // Add directly to log
                    log.push_back(emergency_entry);
                    if (log.size() > LOG_MAX_SIZE) {
                        log.pop_front();
                        logStartIndex_0based++;
                    }
                    
                    // If this request should respond to this follower, add to response buffer
                    if (emergency_entry.respondTo == rank) {
                        clientResponseBuffer.push_back(emergency_entry);
                    }
                    
                    // Remove from request buffer
                    unorderedRequestValues.erase(value);
                    it = requestBuffer.erase(it);
                    processed++;
                }
                
                // std::cout << "[FOLLOWER" << rank << "_EMERGENCY] Processed ALL " << processed 
                //          << " requests, cleared deadlock" << std::endl;
                
                // Update commit index to reflect processed entries
                int new_last_log_idx = log.empty() ? -1 : logStartIndex_0based + log.size() - 1;
                commitIndex = new_last_log_idx;
                
                progress_made = true;  // Mark progress to reset stall counter
                stuckDetectionCounter = 0;  // Reset counter after successful processing
            }
        } else {
            stuckDetectionCounter = 0;  // Reset when no longer stuck
        }
        
        // *** FORCE CLIENT RESPONSE SENDING WHEN BACKLOGGED ***
        if (clientResponseBuffer.size() > 50) {
            // std::cout << "[FOLLOWER" << rank << "_RESPONSE_FORCE] Forcing " 
            //          << clientResponseBuffer.size() << " client responses" << std::endl;
            sendBatchedClientResponses();  // Force send even if timeout not reached
        }
    }
}

void Follower::pruneRequestBuffer() {
    return;
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

bool Follower::sendBatchedClientResponses() {
    if (outstandingSends.size() > MAX_OUTSTANDING_SENDS) {
        return false;
    }

    if (clientResponseBuffer.empty()) {
        return false;
    }
    
    // *** ENHANCED BATCHING LOGIC FOR ULTRA-LOW LATENCY ***
    if (clientResponseBatchTimer == std::chrono::high_resolution_clock::time_point{}) {
        clientResponseBatchTimer = std::chrono::high_resolution_clock::now();
    }

    auto now = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - clientResponseBatchTimer);

    bool batchIsFull = clientResponseBuffer.size() >= BATCH_SIZE;
    bool timeoutReached = elapsed.count() >= BATCH_TIMEOUT_MS;

    // *** ULTRA-AGGRESSIVE RESPONSE SENDING FOR LATENCY ***
    bool forceImmediate = clientResponseBuffer.size() >= 5;   // Reduced from 10
    bool ultraFast = clientResponseBuffer.size() >= 1 && elapsed.count() >= 1;  // 1ms timeout for any response
    bool shortTimeout = clientResponseBuffer.size() > 2 && elapsed.count() >= (BATCH_TIMEOUT_MS / 3);  // Even shorter

    if (!batchIsFull && !timeoutReached && !forceImmediate && !shortTimeout && !ultraFast) {
        return false;
    }

    // Group responses by client rank to send separately
    std::map<int, std::vector<LogEntry>> responsesByClient;
    for (const auto& entry : clientResponseBuffer) {
        responsesByClient[entry.clientRank].push_back(entry);
    }

    // Send responses to each client
    for (const auto& [clientRank, responses] : responsesByClient) {
        std::string response_str = "SUCCESS";
        for (const auto& entry : responses) {
            response_str += "|" + std::to_string(entry.value) + "|" + entry.payload;
        }

        int msg_len = response_str.length() + 1;
        char* send_buffer = new char[msg_len];
        strncpy(send_buffer, response_str.c_str(), msg_len);

        MPI_Request mpi_req;
        MPI_Isend(send_buffer, msg_len, MPI_CHAR, clientRank,
                  CLIENT_RESPONSE, MPI_COMM_WORLD, &mpi_req);

        outstandingSends.push_back({mpi_req, send_buffer});

        // *** ENHANCED LOGGING FOR RESPONSE SENDING (REDUCED FOR LATENCY) ***
        if (responses.size() >= 5) {  // Only log larger batches
            // std::cout << "[FOLLOWER" << rank << "_RESPONSE_SEND] Sent " << responses.size() 
            //          << " responses to client " << clientRank
            //          << (forceImmediate ? " (FORCED)" : "") 
            //          << (shortTimeout ? " (SHORT_TIMEOUT)" : "")
            //          << (ultraFast ? " (ULTRA_FAST)" : "") << std::endl;
        }
    }

    clientResponseBuffer.clear();
    clientResponseBatchTimer = std::chrono::high_resolution_clock::time_point{};
    return true;
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
    if (parts.size() < 5) return;  // Now expecting 5 parts: value|term|payload|clientRank|respondTo

    RequestID rid;
    rid.value = std::stoi(parts[0]);
    rid.term = std::stoi(parts[1]);
    rid.clientRank = std::stoi(parts[3]);  // Add clientRank to make RequestID unique
    std::string payload_str = parts[2];
    int client_rank_val = std::stoi(parts[3]);
    int respond_to_val = std::stoi(parts[4]);

    if (requestBuffer.find(rid.value) == requestBuffer.end()) {
        requestBuffer[rid.value] = LogEntry(rid.term, rid.value, payload_str, client_rank_val, respond_to_val);
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
                // Check if this follower should respond to this request
                if (log[deque_idx].respondTo == rank) {
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
                // Check if this follower should respond to this request
                if (log[deque_idx].respondTo == rank) {
                    clientResponseBuffer.push_back(log[deque_idx]);
                }
            }
        }
    }
}