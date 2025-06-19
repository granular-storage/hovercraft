// === FILE: leader.cpp ===
#include "leader.hpp"
#include <vector>
#include <chrono>
#include <map>

Leader::Leader() : rank(LEADER_RANK), currentTerm(1), logStartIndex_0based(0), commitIndex(-1), lastSentLogIndexToNetAgg(-1),
                   clientResponseBatchTimer(), noProgressCounter(0) {
    nextIndex[FOLLOWER1_RANK] = 1;
    nextIndex[FOLLOWER2_RANK] = 1;
    matchIndex[FOLLOWER1_RANK] = -1;
    matchIndex[FOLLOWER2_RANK] = -1;
}

Leader::~Leader() {
    for (auto& send : outstandingSends) {
        delete[] send.buffer;
    }
}

void Leader::run(bool& shutdown_flag) {
    log_debug("LEADER", "Started with rank " + std::to_string(rank) +
              ", term " + std::to_string(currentTerm));
    
    const uint64_t NO_PROGRESS_LOG_INTERVAL = 5000000;
    
    // *** ENHANCED LEADER FLOW CONTROL ***
    const int AGGRESSIVE_CLEANUP_THRESHOLD = 50;
    auto lastStatsTime = std::chrono::high_resolution_clock::now();
    int cleanupCounter = 0;

    while (!shutdown_flag) {
        // *** MORE AGGRESSIVE SEND COMPLETION CHECKING FOR LATENCY ***
        checkCompletedSends();

        MPI_Status status;
        int flag = 0;
        bool progress_made = false;

        // *** IMPROVED BACKPRESSURE MANAGEMENT ***
        bool canAcceptFromSwitch = (outstandingSends.size() <= (MAX_OUTSTANDING_SENDS * 0.8));  // More conservative
        bool canSendToNetAgg = (outstandingSends.size() <= (MAX_OUTSTANDING_SENDS * 0.9));       // Increased from default - more parallel
        
        if (canAcceptFromSwitch) {
            MPI_Iprobe(SWITCH_RANK, SWITCH_REPLICATE, MPI_COMM_WORLD, &flag, &status);
            if (flag) {
                handleSwitchReplicate(status);
                progress_made = true;
                checkCompletedSends();  // Immediate cleanup after receiving
            }
        }

        // Always probe for commits from NetAgg, as they help clear the pipeline
        MPI_Iprobe(NETAGG_RANK, AGG_COMMIT, MPI_COMM_WORLD, &flag, &status);
        if (flag) {
            handleAggCommit(status);
            progress_made = true;
            checkCompletedSends();  // Immediate cleanup after commit
        }

        if (processUnorderedRequests()) {
            progress_made = true;
            checkCompletedSends();  // Cleanup after processing
        }
        if (sendToNetAgg()) {
            progress_made = true;
            checkCompletedSends();  // Cleanup after sending
        }
        if (sendBatchedClientResponses()) {
            progress_made = true;
            checkCompletedSends();  // Cleanup after responses
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
                // std::cout << "[LEADER_STALLED] No progress after " << noProgressCounter 
                //          << " checks. Outstanding: " << outstandingSends.size() << "/" << MAX_OUTSTANDING_SENDS
                //          << ", Unordered: " << unorderedRequests.size() 
                //          << ", Client responses: " << clientResponseBuffer.size()
                //          << ", Can accept: " << (canAcceptFromSwitch ? "YES" : "NO") << std::endl;
            }
        }
        
        // *** EMERGENCY CLEANUP FOR REQUEST BUFFER OVERFLOW ***
        if (requestBuffer.size() > 10) {  // Lower threshold for faster action
            // std::cout << "[LEADER_EMERGENCY] Processing " << requestBuffer.size() 
            //          << " buffered requests immediately" << std::endl;
            
            // Force process ALL unordered requests
            processUnorderedRequests();
            
            // If still stuck, force process everything in buffer
            if (requestBuffer.size() > 5) {
                int processed = 0;
                auto it = requestBuffer.begin();
                while (it != requestBuffer.end()) {
                    LogEntry entry_data = it->second;
                    entry_data.term = currentTerm;

                    log.push_back(entry_data);
                    if (log.size() > LOG_MAX_SIZE) {
                        log.pop_front();
                        logStartIndex_0based++;
                    }

                    it = requestBuffer.erase(it);
                    processed++;
                }
                
                // std::cout << "[LEADER_EMERGENCY] Force processed " << processed 
                //          << " requests, cleared buffer" << std::endl;
                unorderedRequests.clear();  // Clear since we processed everything
            }
            
            // Force multiple cleanup cycles
            for (int i = 0; i < 5; i++) {
                checkCompletedSends();
            }
        }
    }
}

bool Leader::sendToNetAgg() {
    if (outstandingSends.size() > MAX_OUTSTANDING_SENDS) {
        return false;
    }

    int nextLogIndexToSend_0based = lastSentLogIndexToNetAgg + 1;
    int lastLogIndex_0based = logStartIndex_0based + log.size() - 1;

    if (nextLogIndexToSend_0based <= lastLogIndex_0based) {
        std::vector<RequestID> batch_ids;
        for (int i = 0; i < BATCH_SIZE && (nextLogIndexToSend_0based + i) <= lastLogIndex_0based; ++i) {
            int current_absolute_idx = nextLogIndexToSend_0based + i;
            int deque_idx = current_absolute_idx - logStartIndex_0based;
            const LogEntry& entry = log[deque_idx];
            batch_ids.push_back({entry.value, entry.term, entry.clientRank});  // Include clientRank
            // std::cout << "[LEADER] SEND_TO_NETAGG value=" << entry.value << std::endl;
        }

        if (batch_ids.empty()) return false;

        AppendEntriesNetAggMsg msg;
        msg.term = currentTerm;
        msg.prevLogIndex = nextLogIndexToSend_0based; 
        msg.prevLogTerm = 0;
        if (msg.prevLogIndex > 0) {
            int prev_deque_idx = (msg.prevLogIndex - 1) - logStartIndex_0based;
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
        
        return true;
    }
    return false;
}

bool Leader::sendBatchedClientResponses() {
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
            // std::cout << "[LEADER_RESPONSE_SEND] Sent " << responses.size() 
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
    if (parts.size() < 5) { return; }  // Now expecting 5 parts: value|term|payload|clientRank|respondTo
    
    RequestID rid;
    rid.value = std::stoi(parts[0]);
    rid.term = std::stoi(parts[1]);
    rid.clientRank = std::stoi(parts[3]);  // Add clientRank to make RequestID unique
    std::string payload = parts[2];
    int clientRank = std::stoi(parts[3]);
    int respondTo = std::stoi(parts[4]);

    if (requestBuffer.find(rid) == requestBuffer.end()) {
        requestBuffer[rid] = LogEntry(rid.term, rid.value, payload, clientRank, respondTo);
        unorderedRequests.insert(rid);
    }
}

bool Leader::processUnorderedRequests() {
    if (unorderedRequests.empty()) {
        return false;
    }
    
    int processedCount = 0;
    auto it = unorderedRequests.begin();
    while (it != unorderedRequests.end()) {
        const RequestID& rid_from_switch = *it;
        auto buffer_it = requestBuffer.find(rid_from_switch);
        if (buffer_it != requestBuffer.end()) {
            LogEntry entry_data = buffer_it->second;
            entry_data.term = currentTerm;

            log.push_back(entry_data);
            if (log.size() > LOG_MAX_SIZE) {
                log.pop_front();
                logStartIndex_0based++;
            }

            requestBuffer.erase(buffer_it);
            it = unorderedRequests.erase(it);
            processedCount++;
        } else {
            it = unorderedRequests.erase(it);
        }
    }
    
    return processedCount > 0;
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
            if (i >= logStartIndex_0based) {
                int deque_idx = i - logStartIndex_0based;
                // Check if this leader should respond to this request
                if (log[deque_idx].respondTo == rank) {
                    clientResponseBuffer.push_back(log[deque_idx]);
                }
            }
        }
    }
}