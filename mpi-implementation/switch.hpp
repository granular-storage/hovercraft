#ifndef SWITCH_HPP
#define SWITCH_HPP

#include "common.hpp"
#include <list> // Use list for efficient removal

class Switch {
private:
    int rank;
    int currentTerm;
    // No change to these
    std::map<RequestID, LogEntry> switchBuffer;
    std::map<int, std::set<RequestID>> switchSentRecord;
    int nextValue;
    long long replicatedCount; // Counter to trigger periodic cleanup

    // A structure to track an ongoing send operation
    struct OutstandingSend {
        MPI_Request request;
        char* buffer; // We need to keep the buffer alive
    };
    std::list<OutstandingSend> outstandingSends;

    void handleClientRequest(MPI_Status& status);
    void replicateBufferedRequests();
    // No change to sendReplicateMessage signature
    void sendReplicateMessage(int dest, const RequestID& rid, const LogEntry& entry);
    
    // New function to clean up completed sends
    void checkCompletedSends();

public:
    Switch();
    ~Switch(); // Add a destructor to clean up
    void run(bool& shutdown_flag);
};

#endif // SWITCH_HPP