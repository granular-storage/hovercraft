// Main driver for HoverCraft++ consensus protocol implementation

#include "common.hpp"
#include "switch.hpp"
#include "leader.hpp"
#include "follower.hpp"
#include "netagg.hpp"
#include "client.hpp"

// Include implementations
#include "switch.cpp"
#include "leader.cpp"
#include "follower.cpp"
#include "netagg.cpp"
#include "client.cpp"

bool shutdown_flag = false;

int main(int argc, char** argv) {
    // MPI_Init(&argc, &argv);
    
    // int rank, size;
    // MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    // MPI_Comm_size(MPI_COMM_WORLD, &size);

    int provided_thread_level;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided_thread_level);
    
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    // Check if the MPI library provides the required level of thread support
    if (provided_thread_level < MPI_THREAD_MULTIPLE) {
        if (rank == 0) {
            fprintf(stderr, "ERROR: Your MPI implementation does not support MPI_THREAD_MULTIPLE.\n");
        }
        MPI_Finalize();
        return 1;
    }
    
    if (size != 6) {
        if (rank == 0) {
            std::cerr << "Error: This program requires exactly 6 MPI processes:" << std::endl;
            std::cerr << "  Rank 0: Switch" << std::endl;
            std::cerr << "  Rank 1: Leader" << std::endl;
            std::cerr << "  Rank 2: Follower1" << std::endl;
            std::cerr << "  Rank 3: Follower2" << std::endl;
            std::cerr << "  Rank 4: NetAgg" << std::endl;
            std::cerr << "  Rank 5: Client" << std::endl;
            std::cerr << "Run with: mpirun -np 6 ./hovercraft_demo [num_requests]" << std::endl;
        }
        MPI_Finalize();
        return 1;
    }
    
    // Each rank runs its designated component
    switch (rank) {
        case SWITCH_RANK: {
            Switch switchComponent;
            switchComponent.run(shutdown_flag);
            break;
        }
        case LEADER_RANK: {
            Leader leader;
            leader.run(shutdown_flag);
            break;
        }
        case FOLLOWER1_RANK:
        case FOLLOWER2_RANK: {
            Follower follower(rank);
            follower.run(shutdown_flag);
            break;
        }
        case NETAGG_RANK: {
            NetAgg netagg;
            netagg.run(shutdown_flag);
            break;
        }
        case CLIENT_RANK: {
            int numRequests = 10000;  // Default
            if (argc > 1) {
                numRequests = std::atoi(argv[1]);
            }
            
            std::this_thread::sleep_for(std::chrono::seconds(1));

            auto start_time = std::chrono::high_resolution_clock::now();
            
            Client client;
            client.run(numRequests);

            auto end_time = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
            
            std::cout << "\n==============================" << std::endl;
            std::cout << "Client total runtime: " << std::fixed << std::setprecision(3) 
                      << duration.count() / 1000.0 << " seconds." << std::endl;
            std::cout << "==============================\n" << std::endl;
            
            // Give time for final messages to be processed
            std::this_thread::sleep_for(std::chrono::seconds(2));
            
            // --- GRACEFUL SHUTDOWN INITIATION ---
            std::cout << "Client completed. Sending shutdown signal..." << std::endl;
            // Send a shutdown signal to the Switch. It will propagate it.
            // todo send total number of requests
            // The message content doesn't matter, only the tag.
            // MPI_Send(nullptr, 0, MPI_CHAR, SWITCH_RANK, SHUTDOWN_SIGNAL, MPI_COMM_WORLD);

            // Wait for all other processes to finish before client exits
            // MPI_Barrier(MPI_COMM_WORLD);
            
            break;
        }
    }
    
    MPI_Finalize();
    return 0;
}