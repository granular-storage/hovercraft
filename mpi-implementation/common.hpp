#ifndef COMMON_HPP
#define COMMON_HPP

#include <mpi.h>
#include <string>
#include <vector>
#include <map>
#include <set>
#include <chrono>
#include <iostream>
#include <sstream> // For string splitting helper
#include <cstring>
#include <deque> // For fixed-size log
#include <cstdint> // For uint64_t

// MPI Tags for different message types
enum MessageType {
    CLIENT_REQUEST = 1,
    SWITCH_REPLICATE = 2,
    APPEND_ENTRIES_NETAGG_REQUEST = 3,
    APPEND_ENTRIES_REQUEST = 4,
    APPEND_ENTRIES_RESPONSE = 5,
    AGG_COMMIT = 6,
    CLIENT_RESPONSE = 7,
    SHUTDOWN_SIGNAL = 99
};

// Component IDs (MPI ranks)
enum ComponentID {
    SWITCH_RANK = 0,
    LEADER_RANK = 1,
    FOLLOWER1_RANK = 2,
    FOLLOWER2_RANK = 3,
    NETAGG_RANK = 4,
    CLIENT_RANK = 5
};

const int BATCH_SIZE = 1; // Max number of entries in a message batch
const int LOG_MAX_SIZE = 1000; // Max number of entries to keep in server logs
const int BATCH_TIMEOUT_MS = 5; // Reduced from 25ms - much more aggressive for low latency

const int STATS_INTERVAL = 1000;

const int MAX_OUTSTANDING_SENDS = 100; // Limit outstanding MPI sends

// Request identifier
struct RequestID {
    int value;
    int term;
    
    bool operator<(const RequestID& other) const {
        if (term != other.term) return term < other.term;
        return value < other.value;
    }
    
    bool operator==(const RequestID& other) const {
        return value == other.value && term == other.term;
    }
};

// Log entry structure
struct LogEntry {
    int term;
    int value;
    std::string payload;
    int clientRank;  // Who should respond to client
    
    LogEntry() : term(0), value(0), payload(""), clientRank(-1) {}
    LogEntry(int t, int v, const std::string& p, int cr) 
        : term(t), value(v), payload(p), clientRank(cr) {}
};

// Client request message
struct ClientRequestMsg {
    int value;
    std::string payload;
    int respondTo;  // Which server should respond
    std::chrono::high_resolution_clock::time_point timestamp;
};

// Switch replicate message
struct SwitchReplicateMsg {
    RequestID id; // Contains Switch's term
    std::string payload;
    int clientRank;
};

// AppendEntries to NetAgg message
struct AppendEntriesNetAggMsg {
    int term;
    int prevLogIndex; // Index of log entry immediately preceding the new ones (1-based)
    int prevLogTerm;
    int firstEntryIndex; // Index of the first entry in the batch (1-based)
    std::string batchedEntryIds; // Serialized: "value1,term1;value2,term2;..." (terms are Leader's term)
    int commitIndex; // Leader's commitIndex (0-based)
    int source;
};

// AppendEntries from NetAgg to Followers
struct AppendEntriesMsg {
    int term;
    int prevLogIndex;
    int prevLogTerm;
    int firstEntryIndex;
    std::string batchedEntryIds; // Serialized: "value1,term1;value2,term2;..."
    int commitIndex; // Leader's commitIndex (0-based)
    int originalLeader;
    int source;  // NetAgg
};

// AppendEntries response
struct AppendEntriesResponseMsg {
    int term;
    bool success;
    int matchIndex; // Highest log index known to be replicated on follower (1-based)
    int source;
    int dest;
};

// AggCommit message
struct AggCommitMsg {
    int commitIndex; // 1-based index
    int term;
    int source;
};

// Utility functions
inline void log_debug(const std::string& component, const std::string& message) {
    //std::cout << "[" << component << "] " << message << std::endl;
}

// New function for progress logging
inline void log_progress_stalled(const std::string& component, uint64_t checks) {
    std::cout << "[" << component << "_STALLED] No progress after "
              << checks << " checks." << std::endl;
}

// Helper to split string by delimiter
inline std::vector<std::string> split_string(const std::string& s, char delimiter) {
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream tokenStream(s);
    while (std::getline(tokenStream, token, delimiter)) {
        tokens.push_back(token);
    }
    return tokens;
}

// Helper to deserialize batchedEntryIds
inline std::vector<RequestID> deserialize_batched_ids(const std::string& batched_str) {
    std::vector<RequestID> ids;
    if (batched_str.empty()) return ids;

    std::vector<std::string> entry_tokens = split_string(batched_str, ';');
    for (const auto& entry_token : entry_tokens) {
        if (entry_token.empty()) continue;
        std::vector<std::string> id_parts = split_string(entry_token, ',');
        if (id_parts.size() == 2) {
            try {
                ids.push_back({std::stoi(id_parts[0]), std::stoi(id_parts[1])});
            } catch (const std::exception& e) {
                // log error or handle malformed string
            }
        }
    }
    return ids;
}

// Helper to serialize batchedEntryIds
inline std::string serialize_batched_ids(const std::vector<RequestID>& ids) {
    std::string s;
    for (size_t i = 0; i < ids.size(); ++i) {
        s += std::to_string(ids[i].value) + "," + std::to_string(ids[i].term);
        if (i < ids.size() - 1) {
            s += ";";
        }
    }
    return s;
}


#endif // COMMON_HPP
