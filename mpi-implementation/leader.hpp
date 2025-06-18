#ifndef LEADER_HPP
#define LEADER_HPP

#include "common.hpp"
#include <thread>
#include <algorithm>
#include <list> // For managing outstanding sends
#include <deque>
#include <chrono> // For batching timer

class Leader {
private:
    int rank;
    int currentTerm;
    std::deque<LogEntry> log;
    int logStartIndex_0based; // Absolute index of log.front()
    std::vector<LogEntry> clientResponseBuffer;

    // --- BATCHING TIMER ---
    std::chrono::high_resolution_clock::time_point clientResponseBatchTimer;

    int commitIndex;
    std::map<int, int> nextIndex;
    std::map<int, int> matchIndex;

    std::set<RequestID> unorderedRequests;
    std::map<RequestID, LogEntry> requestBuffer;

    int lastSentLogIndexToNetAgg;

    // Structure for non-blocking sends
    struct OutstandingSend {
        MPI_Request request;
        char* buffer; // Buffer must remain valid until send completes
    };
    std::list<OutstandingSend> outstandingSends;

    void handleSwitchReplicate(MPI_Status& status);
    void processUnorderedRequests();
    void sendToNetAgg();
    void handleAggCommit(MPI_Status& status);
    void sendBatchedClientResponses();
    void checkCompletedSends();

public:
    Leader();
    ~Leader(); // Destructor to clean up resources
    void run(bool& shutdown_flag);
};

#endif // LEADER_HPP
