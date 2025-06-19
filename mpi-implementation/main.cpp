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
    
    if (size < 6) {
        if (rank == 0) {
            std::cerr << "Error: This program requires at least 6 MPI processes:" << std::endl;
            std::cerr << "  Rank 0: Switch" << std::endl;
            std::cerr << "  Rank 1: Leader" << std::endl;
            std::cerr << "  Rank 2: Follower1" << std::endl;
            std::cerr << "  Rank 3: Follower2" << std::endl;
            std::cerr << "  Rank 4: NetAgg" << std::endl;
            std::cerr << "  Ranks 5+: Clients" << std::endl;
            std::cerr << "Run with: mpirun -np <N> ./hovercraft_demo [num_requests] (where N >= 6)" << std::endl;
        }
        MPI_Finalize();
        return 1;
    }
    
    int numClients = size - getNumServerComponents();
    if (rank == 0) {
        std::cout << "Starting HoverCraft++ with " << numClients << " concurrent client(s)" << std::endl;
    }
    
    // Each rank runs its designated component
    if (rank == SWITCH_RANK) {
        Switch switchComponent;
        switchComponent.run(shutdown_flag);
    } else if (rank == LEADER_RANK) {
        Leader leader;
        leader.run(shutdown_flag);
    } else if (rank == FOLLOWER1_RANK || rank == FOLLOWER2_RANK) {
        Follower follower(rank);
        follower.run(shutdown_flag);
    } else if (rank == NETAGG_RANK) {
        NetAgg netagg;
        netagg.run(shutdown_flag);
    } else if (isClientRank(rank)) {
        int numRequests = 10000;  // Default
        if (argc > 1) {
            numRequests = std::atoi(argv[1]);
        }
        
        // std::cout << "[CLIENT" << rank << "] Starting with " << numRequests << " requests" << std::endl;

        auto start_time = std::chrono::high_resolution_clock::now();
        
        Client client(rank);  // Pass the rank to the client
        client.run(numRequests);

        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        
        std::cout << "\n==============================" << std::endl;
        std::cout << "Client " << rank << " total runtime: " << std::fixed << std::setprecision(3) 
                  << duration.count() / 1000.0 << " seconds." << std::endl;
        std::cout << "==============================\n" << std::endl;
        
        // Only the last client initiates shutdown
        if (rank == size - 1) {
            std::cout << "Last client completed. System will shutdown..." << std::endl;
        }
    }
    
    MPI_Finalize();
    return 0;
}