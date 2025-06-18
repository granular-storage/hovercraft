#ifndef FOLLOWER_HPP
#define FOLLOWER_HPP

#include "common.hpp"
#include <thread>
#include <list> // For managing outstanding sends
#include <deque>
#include <chrono> // For batching timer

class Follower {
private:
    int rank;
    int currentTerm;
    std::deque<LogEntry> log;
    int logStartIndex_0based; // Absolute index of log.front()
    std::vector<LogEntry> clientResponseBuffer;

    // --- BATCHING TIMER ---
    std::chrono::high_resolution_clock::time_point clientResponseBatchTimer;

    int commitIndex;

    std::map<int, LogEntry> requestBuffer;
    std::set<int> unorderedRequestValues;
    
    // Structure for non-blocking sends
    struct OutstandingSend {
        MPI_Request request;
        char* buffer;
    };
    std::list<OutstandingSend> outstandingSends;

    // --- FIX: Members for periodic cleanup ---
    long long append_count; // Counter to trigger periodic cleanup

    void handleSwitchReplicate(MPI_Status& status);
    void handleAppendEntries(MPI_Status& status);
    void sendAppendEntriesResponse(bool success, int matchIdx_1based);
    void handleAggCommit(MPI_Status& status);
    void sendBatchedClientResponses();
    void checkCompletedSends();
    void pruneRequestBuffer(); // New cleanup function

public:
    Follower(int r);
    ~Follower(); // Destructor
    void run(bool& shutdown_flag);
};

#endif // FOLLOWER_HPP
