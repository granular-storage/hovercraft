#ifndef CLIENT_HPP
#define CLIENT_HPP

#include "common.hpp"
#include <random>
#include <thread>
#include <algorithm>
#include <iomanip>
#include <list> // For managing outstanding sends

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
    std::map<int, RequestTracker> pendingRequests;
    std::vector<double> latencies;
    long long processedRequestCount;

    // Structure for non-blocking sends
    struct OutstandingSend {
        MPI_Request request;
        char* buffer;
    };
    std::list<OutstandingSend> outstandingSends;

    void sendRequest(int value);
    void handleResponse(MPI_Status& status);
    void printStatistics();
    void checkCompletedSends(); // New function

public:
    Client();
    ~Client(); // Destructor
    void run(int numRequests);
};

#endif // CLIENT_HPP