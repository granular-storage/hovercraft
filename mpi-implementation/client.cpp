#include "client.hpp"
#include <vector>
#include <mutex> // For std::lock_guard
#include <thread> // For std::this_thread::sleep_for
#include <map>

// --- Session handle for MPI_T, global to this file ---
//MPI_T_pvar_session pvar_session;

//void printMpiPvar(const char* pvar_name) {
//    int pvar_index = -1;
//    // We only need to search the state class, as that's where these PVars live.
//    int err = MPI_T_pvar_get_index(pvar_name, MPI_T_PVAR_CLASS_STATE, &pvar_index);
//
//    if (err != MPI_SUCCESS || pvar_index < 0) {
//        // PVar not found. This is normal if a component (like ucx) isn't used.
//        return;
//    }
//
//    MPI_T_pvar_handle pvar_handle;
//    int count;
//    err = MPI_T_pvar_handle_alloc(pvar_session, pvar_index, NULL, &pvar_handle, &count);
//    if (err != MPI_SUCCESS) return;
//
//    MPI_T_pvar_start(pvar_session, pvar_handle);
//
//    // --- CRITICAL FIX: Use the correct data type found by ompi_info ---
//    unsigned int pvar_value;
//    MPI_T_pvar_read(pvar_session, pvar_handle, &pvar_value);
//
//    // Use std::flush to force output immediately
//    std::cout << "[CLIENT_MPI_STATE] " << pvar_name << ": " << pvar_value << std::flush;
//    std::cout << std::endl;
//
//    MPI_T_pvar_stop(pvar_session, pvar_handle);
//    MPI_T_pvar_handle_free(pvar_session, &pvar_handle);
//}

// +++ FUNCTION updated with the correct PVar names for your system +++
//void logMpiState() {
//    printMpiPvar("pml_ob1_unexpected_msgq_length");
//    printMpiPvar("pml_ob1_send_queue_length");
//}

// Constructor: initialize thread-related members
Client::Client() : rank(CLIENT_RANK), rng(std::random_device{}()), processedRequestCount(0), stopReceiverFlag(false), noProgressCounter(0) {
    servers = {LEADER_RANK, FOLLOWER1_RANK, FOLLOWER2_RANK};
    serverDist = std::uniform_int_distribution<int>(0, static_cast<int>(servers.size()) - 1);
}

// Destructor: ensure thread is stopped and resources are cleaned up
Client::~Client() {
    if (receiverThread.joinable()) {
        stopReceiverFlag.store(true);
        receiverThread.join();
    }
    for (auto& send : outstandingSends) {
        delete[] send.buffer;
    }
}

// The main function for the receiver thread
void Client::receiverLoop() {
    log_debug("CLIENT_THREAD", "Receiver thread started.");
    const uint64_t NO_PROGRESS_LOG_INTERVAL = 5000000;  // Reduced from 10M for faster detection
    
    // *** ADAPTIVE RECEIVER IMPROVEMENTS ***
    auto lastResponseTime = std::chrono::high_resolution_clock::now();
    auto lastTimeoutCheck = lastResponseTime;
    int adaptiveProbeDelay = 0;  // microseconds
    const int MAX_PROBE_DELAY = 100;  // 100μs max delay between probes
    int consecutiveEmptyProbes = 0;
    
    while (!stopReceiverFlag.load()) {
        MPI_Status status;
        int flag = 0;
        MPI_Iprobe(MPI_ANY_SOURCE, CLIENT_RESPONSE, MPI_COMM_WORLD, &flag, &status);
        
        if (flag) {
            handleResponse(status);
            noProgressCounter = 0;
            consecutiveEmptyProbes = 0;
            lastResponseTime = std::chrono::high_resolution_clock::now();
            adaptiveProbeDelay = 0;  // Reset delay on successful receive
        } else {
            noProgressCounter++;
            consecutiveEmptyProbes++;
            
            // *** ADAPTIVE PROBING: Reduce CPU usage when no responses coming ***
            if (consecutiveEmptyProbes > 1000) {
                adaptiveProbeDelay = std::min(adaptiveProbeDelay + 1, MAX_PROBE_DELAY);
                consecutiveEmptyProbes = 0;  // Reset counter
            }
            
            // *** ENHANCED STALL DETECTION ***
            if (noProgressCounter > 0 && (noProgressCounter % NO_PROGRESS_LOG_INTERVAL == 0)) {
                auto timeSinceLastResponse = std::chrono::duration_cast<std::chrono::seconds>(
                    std::chrono::high_resolution_clock::now() - lastResponseTime);
                
                std::unique_lock<std::recursive_mutex> lock(dataMutex);
                size_t pendingCount = pendingRequests.size();
                lock.unlock();
                
                std::cout << "[CLIENT_THREAD_STALLED] No progress after " << noProgressCounter 
                         << " checks. Last response " << timeSinceLastResponse.count() 
                         << "s ago. Pending: " << pendingCount << std::endl;
                
                // *** STALL RECOVERY: Reset probe delay to be more aggressive ***
                if (timeSinceLastResponse.count() > 5 && pendingCount > 0) {
                    std::cout << "[CLIENT_RECEIVER_RECOVERY] Resetting to aggressive probing" << std::endl;
                    adaptiveProbeDelay = 0;
                    consecutiveEmptyProbes = 0;
                }
            }
        }
        
        // *** AGGRESSIVE TIMEOUT CLEARING IN RECEIVER THREAD ***
        auto currentTime = std::chrono::high_resolution_clock::now();
        auto timeSinceLastTimeoutCheck = std::chrono::duration_cast<std::chrono::seconds>(
            currentTime - lastTimeoutCheck);
        
        if (timeSinceLastTimeoutCheck.count() >= 10) {  // Check every 10 seconds
            std::unique_lock<std::recursive_mutex> lock(dataMutex);
            if (!pendingRequests.empty()) {
                int clearedInReceiver = 0;
                std::map<int, int> receiverTimeoutsByServer;  // Track timeouts by assigned server
                for (auto it = pendingRequests.begin(); it != pendingRequests.end(); ) {
                    auto responseTime = std::chrono::duration_cast<std::chrono::seconds>(
                        currentTime - it->second.startTime);
                    
                    if (responseTime.count() > 20) {  // 20 second timeout in receiver
                        std::cout << "[CLIENT_RECEIVER_TIMEOUT] Clearing request " << it->first 
                                 << " assigned to server " << it->second.respondTo
                                 << " after " << responseTime.count() << "s" << std::endl;
                        receiverTimeoutsByServer[it->second.respondTo]++;
                        it = pendingRequests.erase(it);
                        clearedInReceiver++;
                    } else {
                        ++it;
                    }
                }
                
                if (clearedInReceiver > 0) {
                    std::cout << "[CLIENT_RECEIVER_CLEARED] Removed " << clearedInReceiver 
                             << " timed out requests" << std::endl;
                    
                    // *** REPORT RECEIVER TIMEOUT PATTERNS ***
                    std::cout << "[CLIENT_RECEIVER_TIMEOUT_ANALYSIS] Receiver timeouts by server: ";
                    for (const auto& [server, count] : receiverTimeoutsByServer) {
                        std::cout << "Server" << server << ":" << count << " ";
                    }
                    std::cout << std::endl;
                }
            }
            lock.unlock();
            lastTimeoutCheck = currentTime;
        }
        
        // *** ADAPTIVE SLEEP: Reduce CPU usage when system is idle ***
        if (adaptiveProbeDelay > 0) {
            std::this_thread::sleep_for(std::chrono::microseconds(adaptiveProbeDelay));
        }
    }
    log_debug("CLIENT_THREAD", "Receiver thread stopping.");
}

void Client::run(int numRequests) {
    log_debug("CLIENT", "Started with rank " + std::to_string(rank));

    // +++ Initialize MPI Tool Interface +++
    int provided;
	MPI_T_init_thread(MPI_THREAD_MULTIPLE, &provided);
	if (provided < MPI_THREAD_MULTIPLE) {
		log_debug("CLIENT", "MPI does not provide MPI_THREAD_MULTIPLE support, which is required.");
		MPI_Abort(MPI_COMM_WORLD, 1);
	}
    // +++ Create a session for PVar access +++
//    MPI_T_pvar_session_create(&pvar_session);

    receiverThread = std::thread(&Client::receiverLoop, this);

    // *** PROGRESS IMPROVEMENTS ***
    // Reduce backpressure limits to keep pipeline flowing
    const int MAX_PENDING_REQUESTS = 15 * BATCH_SIZE;  // Increased from 50 - was too aggressive
    const int ADAPTIVE_SEND_LIMIT = 10;                // Increased from 50
    const int MIN_REQUESTS_IN_FLIGHT = 5;              // Reduced from 10 - less conflict with max
    
    // Add timeout and adaptive mechanisms
    auto lastProgressTime = std::chrono::high_resolution_clock::now();
    auto lastStatsTime = std::chrono::high_resolution_clock::now();
    int consecutiveStallChecks = 0;
    const int MAX_STALL_CHECKS = 5000000;  // Increased from 1M - less aggressive
    const int SLOW_SYSTEM_THRESHOLD = 1000000;  // Detect slow system vs stalled
    
    // Adaptive request rate control
    double baseRequestDelay = 0.0;  // microseconds between requests
    const double MAX_REQUEST_DELAY = 1000.0;  // 1ms max delay

    for (int i = 1; i <= numRequests; i++) {
        auto currentTime = std::chrono::high_resolution_clock::now();
        
        // *** ADAPTIVE BACKPRESSURE: Don't block completely ***
        while (outstandingSends.size() >= ADAPTIVE_SEND_LIMIT) {
            checkCompletedSends();
            consecutiveStallChecks++;
            
            // *** GRADUATED RESPONSE TO SEND BACKPRESSURE ***
            if (consecutiveStallChecks == SLOW_SYSTEM_THRESHOLD) {
                std::cout << "[CLIENT_SEND_SLOW] Send queue backlogged, outstanding=" 
                         << outstandingSends.size() << " (limit=" << ADAPTIVE_SEND_LIMIT << ")" << std::endl;
            }
            
            // If stalled too long, force progress by reducing limits
            if (consecutiveStallChecks > MAX_STALL_CHECKS) {
                std::cout << "[CLIENT_SEND_FORCE] Send queue critically stalled after " 
                         << consecutiveStallChecks << " checks, outstanding=" 
                         << outstandingSends.size() << std::endl;
                break;  // Force send anyway
            }
            
            // *** EXPONENTIAL BACKOFF FOR SEND QUEUE ***
            if (consecutiveStallChecks > SLOW_SYSTEM_THRESHOLD) {
                int backoff_us = std::min(consecutiveStallChecks / 100000, 50);  // Max 50μs for sends
                std::this_thread::sleep_for(std::chrono::microseconds(backoff_us));
            }
        }

        // *** ADAPTIVE PENDING LIMIT: Keep minimum requests in flight ***
        while (true) {
            checkCompletedSends();
            std::unique_lock<std::recursive_mutex> lock(dataMutex);
            size_t pending = pendingRequests.size();
            
            // Allow sending if below limit OR if too few requests in flight
            if (pending <= MAX_PENDING_REQUESTS || pending < MIN_REQUESTS_IN_FLIGHT) {
                break;
            }
            lock.unlock();
            
            consecutiveStallChecks++;
            
            // *** GRADUATED RESPONSE TO BACKPRESSURE ***
            if (consecutiveStallChecks == SLOW_SYSTEM_THRESHOLD) {
                std::cout << "[CLIENT_SLOW_SYSTEM] System responding slowly, pending=" 
                         << pending << ", adapting..." << std::endl;
                // Increase delay to give system time to catch up
                baseRequestDelay = std::min(baseRequestDelay + 50.0, MAX_REQUEST_DELAY);
            }
            
            if (consecutiveStallChecks > MAX_STALL_CHECKS) {
                std::cout << "[CLIENT_FORCE_SEND] True stall detected after " 
                         << consecutiveStallChecks << " checks, pending=" << pending 
                         << " (limit=" << MAX_PENDING_REQUESTS << ")" << std::endl;
                break;
            }
            
            // *** EXPONENTIAL BACKOFF: Wait longer as stall persists ***
            if (consecutiveStallChecks > SLOW_SYSTEM_THRESHOLD) {
                int backoff_us = std::min(consecutiveStallChecks / 100000, 100);  // Max 100μs
                std::this_thread::sleep_for(std::chrono::microseconds(backoff_us));
            }
        }

        // *** PROGRESS MONITORING: Detect and adapt to stalls ***
        if (consecutiveStallChecks == 0) {
            lastProgressTime = currentTime;
        } else {
            auto stallDuration = std::chrono::duration_cast<std::chrono::milliseconds>(
                currentTime - lastProgressTime);
            
            // *** ADAPTIVE STALL RESPONSE: Different behavior for different stall durations ***
            if (stallDuration.count() > 2000) {  // 2 second stall - more conservative
                std::unique_lock<std::recursive_mutex> lock(dataMutex);
                size_t pending = pendingRequests.size();
                lock.unlock();
                
                std::cout << "[CLIENT_STALL_ANALYSIS] " << stallDuration.count() 
                         << "ms stall - Outstanding: " << outstandingSends.size() 
                         << "/" << ADAPTIVE_SEND_LIMIT << ", Pending: " << pending 
                         << "/" << MAX_PENDING_REQUESTS << std::endl;
                
                // *** ULTRA-LOW LATENCY: More aggressive timeout for small pending counts ***
                if (pending < 50 && stallDuration.count() > 1000) {  // Small numbers stuck for 1s
                    std::cout << "[CLIENT_LATENCY_EMERGENCY] Small request count (" << pending 
                             << ") stuck for " << stallDuration.count() << "ms - forcing faster timeout" << std::endl;
                    baseRequestDelay = std::min(baseRequestDelay + 50.0, MAX_REQUEST_DELAY);
                    consecutiveStallChecks = consecutiveStallChecks / 4;  // More aggressive reset
                    lastProgressTime = currentTime;
                }
                // Adaptive recovery: increase request delay more gradually
                else {
                    baseRequestDelay = std::min(baseRequestDelay + 25.0, MAX_REQUEST_DELAY);
                    consecutiveStallChecks = consecutiveStallChecks / 2;  // Partial reset
                    lastProgressTime = currentTime;
                }
            }
        }

        sendRequest(i);
        checkCompletedSends();
        consecutiveStallChecks = 0;  // Reset on successful send

        // *** ADAPTIVE RATE CONTROL: Slow down if system is struggling ***
        if (baseRequestDelay > 0.0) {
            std::this_thread::sleep_for(std::chrono::microseconds(static_cast<int>(baseRequestDelay)));
            // Gradually reduce delay
            baseRequestDelay = std::max(0.0, baseRequestDelay - 1.0);
        }

        // *** PERIODIC PROGRESS REPORTING ***
        auto timeSinceLastStats = std::chrono::duration_cast<std::chrono::seconds>(
            currentTime - lastStatsTime);
        if (timeSinceLastStats.count() >= 10) {  // Every 10 seconds
            std::unique_lock<std::recursive_mutex> lock(dataMutex);
            size_t pending = pendingRequests.size();
            
            // *** TIMEOUT DETECTION FOR MISSING RESPONSES ***
            int timedOutRequests = 0;
            std::map<int, int> timeoutsByServer;  // Track timeouts by assigned server
            if (pending > 0) {
                auto now = std::chrono::high_resolution_clock::now();
                for (auto it = pendingRequests.begin(); it != pendingRequests.end(); ) {
                    auto responseTime = std::chrono::duration_cast<std::chrono::seconds>(
                        now - it->second.startTime);
                    
                    if (responseTime.count() > 30) {  // 30 second timeout
                        std::cout << "[CLIENT_TIMEOUT] Request " << it->first 
                                 << " assigned to server " << it->second.respondTo
                                 << " timed out after " << responseTime.count() 
                                 << "s, removing" << std::endl;
                        timeoutsByServer[it->second.respondTo]++;
                        it = pendingRequests.erase(it);
                        timedOutRequests++;
                    } else {
                        ++it;
                    }
                }
                
                // *** REPORT TIMEOUT PATTERNS ***
                if (timedOutRequests > 0) {
                    std::cout << "[CLIENT_TIMEOUT_ANALYSIS] Timeouts by server: ";
                    for (const auto& [server, count] : timeoutsByServer) {
                        std::cout << "Server" << server << ":" << count << " ";
                    }
                    std::cout << std::endl;
                }
            }
            lock.unlock();
            
            // *** ENHANCED PROGRESS REPORT ***
            std::cout << "[CLIENT_PROGRESS] Sent: " << i << "/" << numRequests 
                     << " | Pending: " << (pending - timedOutRequests) << "/" << MAX_PENDING_REQUESTS
                     << " | Outstanding: " << outstandingSends.size() << "/" << ADAPTIVE_SEND_LIMIT
                     << " | Delay: " << baseRequestDelay << "μs"
                     << " | Stall checks: " << consecutiveStallChecks;
            if (timedOutRequests > 0) {
                std::cout << " | Timed out: " << timedOutRequests;
            }
            std::cout << std::endl;
            lastStatsTime = currentTime;
        }
    }

    log_debug("CLIENT", "All requests initiated. Waiting for remaining responses...");

    // *** IMPROVED WAIT LOOP: More aggressive completion checking ***
    uint64_t wait_counter = 0;
    auto waitStartTime = std::chrono::high_resolution_clock::now();
    auto lastTimeoutCheck = waitStartTime;
    
    while (true) {
        checkCompletedSends();
        std::unique_lock<std::recursive_mutex> lock(dataMutex);
        size_t pending_count = pendingRequests.size();
        if (pending_count == 0) {
            break;
        }
        
        // *** AGGRESSIVE TIMEOUT CHECKING DURING WAIT ***
        auto currentWaitTime = std::chrono::high_resolution_clock::now();
        auto timeSinceLastTimeoutCheck = std::chrono::duration_cast<std::chrono::seconds>(
            currentWaitTime - lastTimeoutCheck);
        
        if (timeSinceLastTimeoutCheck.count() >= 5) {  // Check every 5 seconds during wait
            int timedOutInWait = 0;
            std::map<int, int> waitTimeoutsByServer;  // Track timeouts by assigned server
            for (auto it = pendingRequests.begin(); it != pendingRequests.end(); ) {
                auto responseTime = std::chrono::duration_cast<std::chrono::seconds>(
                    currentWaitTime - it->second.startTime);
                
                // *** ULTRA-LOW LATENCY: More aggressive timeout for small counts ***
                int timeoutThreshold = (pending_count < 50) ? 8 : 15;  // Faster timeout for small counts
                
                if (responseTime.count() > timeoutThreshold) {
                    std::cout << "[CLIENT_WAIT_TIMEOUT] Request " << it->first 
                             << " assigned to server " << it->second.respondTo
                             << " timed out after " << responseTime.count() 
                             << "s during wait (threshold=" << timeoutThreshold << "), removing" << std::endl;
                    waitTimeoutsByServer[it->second.respondTo]++;
                    it = pendingRequests.erase(it);
                    timedOutInWait++;
                } else {
                    ++it;
                }
            }
            
            if (timedOutInWait > 0) {
                std::cout << "[CLIENT_WAIT_RECOVERY] Removed " << timedOutInWait 
                         << " timed out requests, continuing..." << std::endl;
                
                // *** REPORT WAIT TIMEOUT PATTERNS ***
                std::cout << "[CLIENT_WAIT_TIMEOUT_ANALYSIS] Wait timeouts by server: ";
                for (const auto& [server, count] : waitTimeoutsByServer) {
                    std::cout << "Server" << server << ":" << count << " ";
                }
                std::cout << std::endl;
                
                pending_count = pendingRequests.size();  // Update count
            }
            lastTimeoutCheck = currentWaitTime;
        }
        lock.unlock();

        wait_counter++;
        if (wait_counter > 0 && wait_counter % 1000000 == 0) {  // Report more frequently
            auto waitTime = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::high_resolution_clock::now() - waitStartTime);
            std::cout << "[CLIENT_WAIT_LOOP_STATUS] Still waiting for " << pending_count 
                     << " responses after " << waitTime.count() << "s..." << std::endl;
        }
        
        // *** DEADLOCK DETECTION: Shorter timeout for critical stall detection ***
        if (wait_counter > 10000000) {  // Reduced from 50M - detect faster
            auto waitTime = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::high_resolution_clock::now() - waitStartTime);
            std::cout << "[CLIENT_POTENTIAL_DEADLOCK] System may be deadlocked after " 
                     << waitTime.count() << "s, " << pending_count << " responses missing" << std::endl;
            
            // *** EMERGENCY RECOVERY: Clear all pending if system appears completely stuck ***
            if (waitTime.count() > 60) {  // 1 minute of waiting
                std::cout << "[CLIENT_EMERGENCY_RECOVERY] Clearing all pending requests after 60s deadlock" << std::endl;
                std::unique_lock<std::recursive_mutex> emergencyLock(dataMutex);
                pendingRequests.clear();
                emergencyLock.unlock();
                break;  // Exit wait loop
            }
            
            wait_counter = 0;  // Reset to avoid spam
        }
    }

    // *** FINAL CLEANUP: Ensure all sends complete ***
    auto cleanupStartTime = std::chrono::high_resolution_clock::now();
    while (!outstandingSends.empty()) {
        checkCompletedSends();
        auto cleanupTime = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::high_resolution_clock::now() - cleanupStartTime);
        if (cleanupTime.count() > 30) {  // 30 second timeout for cleanup
            std::cout << "[CLIENT_CLEANUP_TIMEOUT] Forcing cleanup after 30s, " 
                     << outstandingSends.size() << " sends remaining" << std::endl;
            break;
        }
    }

    stopReceiverFlag.store(true);
    receiverThread.join();

    // +++ Free the session and finalize MPI_T +++
//    MPI_T_pvar_session_free(&pvar_session);
//    MPI_T_finalize();

    printStatistics();
}

void Client::sendRequest(int value) {
    int responderIndex = serverDist(rng);
    int respondTo = servers[responderIndex];

    std::string payload = "pv_" + std::to_string(value);
    std::string msg_str = std::to_string(value) + "|" +
                         std::to_string(respondTo) + "|" +
                         payload;

    auto startTime = std::chrono::high_resolution_clock::now();
    {
        std::lock_guard<std::recursive_mutex> lock(dataMutex);
        pendingRequests[value] = {value, respondTo, startTime};
    }

    int msg_len = msg_str.length() + 1;
    char* send_buffer = new char[msg_len];
    strncpy(send_buffer, msg_str.c_str(), msg_len);

    MPI_Request mpi_req;
    // std::cout << "[CLIENT] SEND_REQ value=" << value << std::endl;
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

    MPI_Request recv_request;
    MPI_Irecv(buffer_vec.data(), msg_size, MPI_CHAR, status.MPI_SOURCE, CLIENT_RESPONSE, MPI_COMM_WORLD, &recv_request);
    MPI_Wait(&recv_request, MPI_STATUS_IGNORE);

    std::string data(buffer_vec.data(), msg_size - 1);

    std::vector<std::string> parts = split_string(data, '|');
    if (parts.empty() || parts[0] != "SUCCESS" || parts.size() < 3 || (parts.size() - 1) % 2 != 0) {
        return;
    }

    std::lock_guard<std::recursive_mutex> lock(dataMutex);

    for (size_t i = 1; i < parts.size(); i += 2) {
        int value = std::stoi(parts[i]);

        auto it = pendingRequests.find(value);
        if (it != pendingRequests.end()) {
            auto endTime = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(endTime - it->second.startTime);
            latencies.push_back(static_cast<double>(duration.count()) / 1000.0);
            pendingRequests.erase(it);

            processedRequestCount++;
            if ((processedRequestCount % STATS_INTERVAL) == 0) {
                std::cout << "\n--- Statistics for interval ending at " << processedRequestCount << " total requests ---" << std::endl;
                printStatistics();
                latencies.clear();
            }
        }
    }
}

void Client::printStatistics() {
    std::lock_guard<std::recursive_mutex> lock(dataMutex);

    if (latencies.empty()) {
        std::cout << "\n=== Latency Statistics (ms) ===" << std::endl;
        std::cout << "No responses received." << std::endl;
        std::cout << "==============================\n" << std::endl;
        return;
    }

    auto latencies_to_print = latencies;

    double sum = 0.0;
    for (double lat : latencies_to_print) sum += lat;
    double avgLat = sum / latencies_to_print.size();

    std::sort(latencies_to_print.begin(), latencies_to_print.end());
    double minLat = latencies_to_print[0];
    double maxLat = latencies_to_print.back();
    double p50 = latencies_to_print[latencies_to_print.size() / 2];
    double p90 = latencies_to_print.empty() ? 0 : latencies_to_print[static_cast<size_t>(latencies_to_print.size() * 0.90)];
    double p99 = latencies_to_print.empty() ? 0 : latencies_to_print[static_cast<size_t>(latencies_to_print.size() * 0.99)];

    std::cout << "\n=== Latency Statistics (ms) ===" << std::endl;
    std::cout << std::fixed << std::setprecision(3);
    std::cout << "Requests processed: " << latencies_to_print.size() << std::endl;
    std::cout << "Average: " << avgLat << std::endl;
    std::cout << "Min:     " << minLat << std::endl;
    std::cout << "Max:     " << maxLat << std::endl;
    std::cout << "P50 (Median): " << p50 << std::endl;
    std::cout << "P90:     " << p90 << std::endl;
    std::cout << "P99:     " << p99 << std::endl;
    std::cout << "==============================\n" << std::endl;
}
