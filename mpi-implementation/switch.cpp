#include "switch.hpp"
#include <vector> // For safer buffer management on receive
#include <chrono>

// Add a destructor to clean up any remaining buffers
Switch::~Switch() {
    for (auto& send : outstandingSends) {
        delete[] send.buffer;
    }
}

Switch::Switch() : rank(SWITCH_RANK), currentTerm(1), nextValue(1), replicatedCount(0), noProgressCounter(0) {
    switchSentRecord[LEADER_RANK] = {};
    switchSentRecord[FOLLOWER1_RANK] = {};
    switchSentRecord[FOLLOWER2_RANK] = {};
}

void Switch::run(bool& shutdown_flag) {
    log_debug("SWITCH", "Started with rank " + std::to_string(rank));
    const uint64_t NO_PROGRESS_LOG_INTERVAL = 5000000;
    
    // *** ENHANCED SWITCH FLOW CONTROL ***
    const int MAX_BUFFER_SIZE = 200;  // Limit switch buffer growth
    const int AGGRESSIVE_CLEANUP_THRESHOLD = 50;  // More aggressive cleanup
    auto lastStatsTime = std::chrono::high_resolution_clock::now();
    int cleanupCounter = 0;
    
    while (!shutdown_flag) {
        auto loopStart = std::chrono::high_resolution_clock::now();
        
        // *** MORE AGGRESSIVE SEND COMPLETION CHECKING FOR LATENCY ***
        checkCompletedSends();
        
        MPI_Status status;
        int flag;
        bool progress_made = false;
        
        // *** IMPROVED BACKPRESSURE WITH BUFFER LIMITS ***
        bool canAcceptRequests = (outstandingSends.size() <= MAX_OUTSTANDING_SENDS) && 
                                (switchBuffer.size() < MAX_BUFFER_SIZE);
        
        if (canAcceptRequests) {
            MPI_Iprobe(MPI_ANY_SOURCE, CLIENT_REQUEST, MPI_COMM_WORLD, &flag, &status);
            if (flag) {
                handleClientRequest(status);
                progress_made = true;
                checkCompletedSends();  // Immediate cleanup after receiving client request
            }
        }
        
        // *** PRIORITIZE REPLICATION TO CLEAR BUFFERS ***
        if (replicateBufferedRequests()) {
            progress_made = true;
            checkCompletedSends();  // Immediate cleanup after replication
        }
        
        // *** ADDITIONAL CLEANUP CYCLES FOR STALL PREVENTION ***
        cleanupCounter++;
        if (cleanupCounter % 100 == 0) {  // Every 100 iterations
            checkCompletedSends();  // Extra cleanup
        }
        
        // *** MORE AGGRESSIVE CLEANUP WHEN BACKLOGGED ***
        if (outstandingSends.size() > AGGRESSIVE_CLEANUP_THRESHOLD) {
            checkCompletedSends();  // Extra aggressive cleanup
        }

        if (progress_made) {
            noProgressCounter = 0;
        } else {
            noProgressCounter++;
            if (noProgressCounter > 0 && (noProgressCounter % NO_PROGRESS_LOG_INTERVAL == 0)) {
                // *** ENHANCED STALL LOGGING ***
                // std::cout << "[SWITCH_STALLED] No progress after " << noProgressCounter 
                //          << " checks. Buffer: " << switchBuffer.size() << "/" << MAX_BUFFER_SIZE
                //          << ", Outstanding: " << outstandingSends.size() << "/" << MAX_OUTSTANDING_SENDS
                //          << ", Can accept: " << (canAcceptRequests ? "YES" : "NO") << std::endl;
            }
        }
        
        // *** PERIODIC HEALTH REPORTING ***
        auto currentTime = std::chrono::high_resolution_clock::now();
        auto timeSinceStats = std::chrono::duration_cast<std::chrono::seconds>(currentTime - lastStatsTime);
        if (timeSinceStats.count() >= 30) {  // Every 30 seconds
            // std::cout << "[SWITCH_STATUS] Processed: " << replicatedCount 
            //          << ", Buffer: " << switchBuffer.size() << "/" << MAX_BUFFER_SIZE
            //          << ", Outstanding: " << outstandingSends.size() << "/" << MAX_OUTSTANDING_SENDS
            //          << ", Progress checks: " << noProgressCounter << std::endl;
            lastStatsTime = currentTime;
        }
        
        // *** EMERGENCY BUFFER OVERFLOW PROTECTION ***
        if (switchBuffer.size() >= MAX_BUFFER_SIZE * 0.9) {  // 90% full
            // std::cout << "[SWITCH_WARNING] Buffer near capacity: " << switchBuffer.size() 
            //          << "/" << MAX_BUFFER_SIZE << ", forcing aggressive cleanup" << std::endl;
            
            // Force multiple cleanup cycles
            for (int i = 0; i < 5; i++) {
                checkCompletedSends();
            }
            
            // If still problematic, drop oldest requests (emergency measure)
            if (switchBuffer.size() >= MAX_BUFFER_SIZE) {
                // std::cout << "[SWITCH_EMERGENCY] Dropping oldest buffered requests to prevent deadlock" << std::endl;
                auto it = switchBuffer.begin();
                int dropped = 0;
                while (it != switchBuffer.end() && dropped < 50) {
                    it = switchBuffer.erase(it);
                    dropped++;
                }
                // std::cout << "[SWITCH_EMERGENCY] Dropped " << dropped << " requests" << std::endl;
            }
        }
    }
}

void Switch::handleClientRequest(MPI_Status& status) {
    int msg_size;
    MPI_Get_count(&status, MPI_CHAR, &msg_size);
    std::vector<char> buffer_vec(msg_size);
    MPI_Request request;
    MPI_Irecv(buffer_vec.data(), msg_size, MPI_CHAR, status.MPI_SOURCE, CLIENT_REQUEST,
             MPI_COMM_WORLD, &request);
    MPI_Wait(&request, MPI_STATUS_IGNORE);

    std::string data(buffer_vec.data(), msg_size -1);
    
    std::vector<std::string> parts = split_string(data, '|');
    if (parts.size() < 3) {
        log_debug("SWITCH_ERROR", "Malformed client request: " + data);
        return;
    }
    
    ClientRequestMsg request_msg;
    request_msg.value = std::stoi(parts[0]);
    request_msg.respondTo = std::stoi(parts[1]);
    request_msg.payload = parts[2];

    RequestID rid{request_msg.value, currentTerm, status.MPI_SOURCE};
    // Store both the actual client rank and which server should respond
    LogEntry entry(currentTerm, request_msg.value, request_msg.payload, status.MPI_SOURCE, request_msg.respondTo);
    
    if (switchBuffer.find(rid) == switchBuffer.end()) {
        switchBuffer[rid] = entry;
    } else {
        // std::cout << "[SWITCH] Duplicate request " << request_msg.value 
        //           << " from client " << status.MPI_SOURCE << " - IGNORED" << std::endl;
    }
}

void Switch::sendReplicateMessage(int dest, const RequestID& rid, const LogEntry& entry) {
    std::string msg_str = std::to_string(rid.value) + "|" + 
                         std::to_string(rid.term) + "|" + 
                         entry.payload + "|" + 
                         std::to_string(entry.clientRank) + "|" +
                         std::to_string(entry.respondTo);

    int msg_len = msg_str.length() + 1;
    char* send_buffer = new char[msg_len];
    strncpy(send_buffer, msg_str.c_str(), msg_len);

    // std::cout << "[SWITCH] REPL_SEND to=" << dest << " value=" << rid.value << std::endl;
    
    MPI_Request mpi_req;
    MPI_Isend(send_buffer, msg_len, MPI_CHAR, dest, SWITCH_REPLICATE, MPI_COMM_WORLD, &mpi_req);
    
    outstandingSends.push_back({mpi_req, send_buffer});
}

void Switch::checkCompletedSends() {
    if (outstandingSends.empty()) {
        return;
    }
    
    int initialCount = outstandingSends.size();
    int completedCount = 0;
    
    auto it = outstandingSends.begin();
    while (it != outstandingSends.end()) {
        int flag = 0;
        MPI_Test(&it->request, &flag, MPI_STATUS_IGNORE);
        if (flag) {
            delete[] it->buffer;
            it = outstandingSends.erase(it);
            completedCount++;
        } else {
            ++it;
        }
    }
    
    // *** ENHANCED MONITORING FOR SEND COMPLETION ***
    if (initialCount > 50 && completedCount > 0) {  // Only log when there's significant activity
        // std::cout << "[SWITCH_SENDS] Completed " << completedCount << "/" << initialCount 
        //          << " sends, remaining: " << outstandingSends.size() << std::endl;
    }
    
    // *** WARNING FOR STUCK SENDS ***
    static int consecutiveNoCompletion = 0;
    if (completedCount == 0 && initialCount > 75) {
        consecutiveNoCompletion++;
        if (consecutiveNoCompletion % 1000 == 0) {  // Every 1000 calls with no completion
            // std::cout << "[SWITCH_SEND_WARNING] " << consecutiveNoCompletion 
            //          << " consecutive cleanup cycles with no completions, outstanding: " 
            //          << outstandingSends.size() << std::endl;
        }
    } else {
        consecutiveNoCompletion = 0;
    }
}

bool Switch::replicateBufferedRequests() {
    if (switchBuffer.empty()) return false;

    bool made_progress = false;
    auto it = switchBuffer.begin();
    int processedInThisCycle = 0;
    const int MAX_PROCESS_PER_CYCLE = 10;  // Limit to prevent send queue overflow
    
    while (it != switchBuffer.end() && processedInThisCycle < MAX_PROCESS_PER_CYCLE) {
        // *** CHECK SEND QUEUE CAPACITY BEFORE EACH REQUEST ***
        // Each request generates 3 sends, so check if we have space for 3
        if (outstandingSends.size() > (MAX_OUTSTANDING_SENDS - 3)) {
            // std::cout << "[SWITCH_BACKPRESSURE] Send queue near capacity: " 
            //          << outstandingSends.size() << "/" << MAX_OUTSTANDING_SENDS 
            //          << ", pausing replication" << std::endl;
            return made_progress;
        }

        const auto& [rid, entry] = *it;
        std::vector<int> targetServers = {LEADER_RANK, FOLLOWER1_RANK, FOLLOWER2_RANK};
        
        bool sentToAnyServer = false;
        for (int server : targetServers) {
            if (switchSentRecord[server].find(rid) == switchSentRecord[server].end()) {
                sendReplicateMessage(server, rid, entry);
                switchSentRecord[server].insert(rid);
                sentToAnyServer = true;
            }
        }

        // Only remove from buffer if we actually sent to at least one server
        if (sentToAnyServer) {
            it = switchBuffer.erase(it);
            replicatedCount++;
            made_progress = true;
            processedInThisCycle++;
            
            // *** PERIODIC CLEANUP DURING REPLICATION ***
            if (processedInThisCycle % 5 == 0) {
                checkCompletedSends();  // Check every 5 requests
            }
        } else {
            ++it;  // Move to next if nothing sent for this request
        }

        // *** EXISTING PRUNING LOGIC (IMPROVED) ***
        const int PRUNE_INTERVAL = 5000;
        const int RETAIN_WINDOW = 20000;
        if (replicatedCount > 0 && replicatedCount % PRUNE_INTERVAL == 0) {
            int prune_threshold = rid.value - RETAIN_WINDOW;
            if (prune_threshold > 0) {
                int totalPruned = 0;
                for (auto& pair : switchSentRecord) {
                    auto& sent_set = pair.second;
                    for (auto set_it = sent_set.begin(); set_it != sent_set.end(); ) {
                        if (set_it->value < prune_threshold) {
                            set_it = sent_set.erase(set_it);
                            totalPruned++;
                        } else {
                            ++set_it;
                        }
                    }
                }
                if (totalPruned > 0) {
                    // std::cout << "[SWITCH_PRUNED] Cleaned " << totalPruned 
                    //          << " old sent records, threshold=" << prune_threshold << std::endl;
                }
            }
        }
    }
    
    // *** FINAL CLEANUP AFTER REPLICATION CYCLE ***
    if (made_progress) {
        checkCompletedSends();
    }
    
    return made_progress;
}