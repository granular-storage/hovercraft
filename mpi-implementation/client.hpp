#ifndef CLIENT_HPP
#define CLIENT_HPP

#include "common.hpp"
#include <random>
#include <thread>
#include <algorithm>
#include <iomanip>
#include <list>    // For managing outstanding sends
#include <atomic>  // For thread-safe flag
#include <mutex>   // For protecting shared data

class Client {
private:
    int rank;
    std::vector<int> servers;
    std::mt19937 rng;
    std::uniform_int_distribution<int> serverDist;

    struct RequestTracker {
        int value;
        int respondTo;
        std::chrono::high_resolution_clock::time_point startTime;
    };

    // --- Threading Members ---
    // A recursive mutex allows a thread to re-lock a mutex it already holds.
    // This is needed because handleResponse() (which holds the lock) calls
    // printStatistics() (which also needs the lock).
    std::recursive_mutex dataMutex;
    std::map<int, RequestTracker> pendingRequests;
    std::vector<double> latencies;
    long long processedRequestCount;
    std::thread receiverThread;
    std::atomic<bool> stopReceiverFlag;

    // For logging stalled progress
    uint64_t noProgressCounter;

    // Structure for non-blocking sends
    struct OutstandingSend {
        MPI_Request request;
        char* buffer;
    };
    std::list<OutstandingSend> outstandingSends;

    void sendRequest(int value);
    void handleResponse(MPI_Status& status);
    void printStatistics();
    void checkCompletedSends();

    // The function for the receiver thread
    void receiverLoop();

public:
    Client(int clientRank);
    ~Client(); // Destructor
    void run(int numRequests);
};

#endif // CLIENT_HPP
