#ifndef NETAGG_HPP
#define NETAGG_HPP

#include "common.hpp"
#include <thread>
#include <algorithm>
#include <list> // For managing outstanding sends
#include <chrono> // For batching timer

struct AggCommitInfo {
    int commitIndex; // 1-based index
    int term;
};

struct PendingEntry {
    int entryIndex;
    int entryTerm;
    int leaderId;
    std::set<int> acksFrom;
    PendingEntry(int idx, int term, int leader) : entryIndex(idx), entryTerm(term), leaderId(leader) {}
    bool operator<(const PendingEntry& other) const { return entryIndex < other.entryIndex; }
};

class NetAgg {
private:
    int rank;
    int currentLeader;
    int currentLeaderTerm;
    std::map<int, int> followerMatchIndex;
    std::map<int, PendingEntry> pendingEntriesMap;
    int netAggCommitIndex; // 1-based index of highest known-committable entry

    // Structure for non-blocking sends
    struct OutstandingSend {
        MPI_Request request;
        char* buffer;
    };
    std::list<OutstandingSend> outstandingSends;

    std::vector<AggCommitInfo> aggCommitBuffer;

    // --- BATCHING TIMER ---
    std::chrono::high_resolution_clock::time_point aggCommitBatchTimer;

    void handleAppendEntriesFromLeader(MPI_Status& status);
    void forwardToFollowers(const AppendEntriesNetAggMsg& leaderMsg, const std::vector<RequestID>& batch_rids);
    void handleAppendEntriesResponse(MPI_Status& status);
    void sendBatchedAggCommits();
    void checkCompletedSends();

public:
    NetAgg();
    ~NetAgg(); // Destructor
    void run(bool& shutdown_flag);
};

#endif // NETAGG_HPP
