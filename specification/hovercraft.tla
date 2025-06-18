------------------------------ MODULE hovercraft ------------------------------

(* 
 * Formal specification of the HoverCraft++ consensus algorithm.
 * See paper: https://marioskogias.github.io/docs/hovercraft.pdf
 * Based on the Raft consensus algorithm with enhancements for improved throughput.
 * Original Raft TLA+ specification: https://github.com/ongardie/raft.tla
 * See also the Raft paper: https://raft.github.io/raft.pdf
 * 
 * This specification introduces two key components:
 * - Switch: Abstracts multicast/broadcast mechanisms to decouple payload replication
 *   from consensus ordering, reducing leader bandwidth bottlenecks
 * - NetAgg: Network aggregator that collects acknowledgments from followers
 *   and sends commit notifications, further reducing leader coordination overhead
 * 
 * We verify safety properties and track message counts for commit index advancement.
 * The specification supports server crashes and leader election mechanisms.
 * 
 * Modified by Ovidiu Marcu.
 * Original Raft specification Copyright 2014 Diego Ongaro.
 * This work is licensed under the Creative Commons Attribution-4.0
 * International License: https://creativecommons.org/licenses/by/4.0/
 *)

(* In standard Raft the leader is the central hub.
  A client sends a request only to the leader.
  The leader must then replicate the entire request payload to all followers.
  The leader gathers acknowledgments from Followers, commits the entry,
  applies it, and sends a response to the client.
  Bottleneck: The leader's network bandwidth and processing power for sending
  the full payload to every follower becomes a limiting factor as the
  cluster size (N) or client request size increases.
  Throughput is limited by Leader_Bandwidth / ((N-1) * Request_Payload_Size).
 *)

(* We introduce a "Switch" abstraction (representing mechanisms like IP Multicast
  or a dedicated middlebox/programmable switch as used in the HovercRaft paper).
  Clients send requests via Switch. The Switch is non-crashing.
  Client requests timeouts due to Switch failures will force clients choose another Switch.
  The Switch's responsibility is to deliver the request payload to all server
  nodes (Leader and Followers) "simultaneously".
  The 'simultaneous' delivery is a model abstraction for efficient 
  broadcast mechanisms like IP Multicast, 
  where payload dissemination is handled by the network infrastructure.
  We introduce the "NetAgg" network aggregation component of HovercRaft++.
  The NetAgg is non-crashing and stateless. Client requests will timeout and retry.
  The leader sends a single message containing the ordering metadata to NetAgg.
  Leader expects a AggCommit message in return. If one is not received in due time,
  the Leader declares NetAgg failure, another NetAgg will be chosen.
  NetAgg is then responsible for disseminating this metadata to followers 
  and collecting their acknowledgments.
  When a follower receives the ordering metadata message from NetAgg,
  it uses the identifier in the metadata to find the corresponding payload
  in its temporary buffer.
  Once matched, the follower places the request payload into its replicated log
  at the correct index specified by the leader's metadata.
  Once NetAgg receives acknowledgments from a quorum of followers, 
  it sends an AggCommit message to all servers, 
  informing them that they can advance their commit index for that entry.
  AggCommit also ensures NetAgg failure handling by Leader choosing another NetAgg.
  This model assumes that the Switch and NetAgg components will never fail.
  We do not model Switch and NetAgg replicas due to model space constraints.
  Point-to-point recovery mechanisms between Followers and the Leader 
  are partially addressed (assumes no Follower crashes).
 *)

EXTENDS Naturals, FiniteSets, Sequences, TLC

\************************* CONSTANTS *******************************************

\* The set of server IDs including the Switch and NetAgg
CONSTANTS Server

\* The set of client requests that can go into the log
(* Represents the possible values of client requests that are stored in the log.
 A value with its term represent the request metadata. Same value is used as payload.
  A typical flow for a request's data/metadata might be:
 * Client -> Switch (metadata and payload)
 * Switch -> Leader & Followers (metadata and payload)
 * Leader -> NetAgg (metadata for ordering)
 * NetAgg -> Followers -> NetAgg (metadata for ordering and acknowledgements)
 * NetAgg -> All Servers (AggCommit sent when majority quorum of acks received)
 * One server of Servers will reply back to client (not handled by this model).
 *)
CONSTANTS Value

\* Server states for Raft protocol.
CONSTANTS Follower, Candidate, Leader

\*Fixed states for Switch and NetAgg indices in Server
CONSTANTS Switch, NetAgg

\* A reserved value.
CONSTANTS Nil

\* Message types
\* Standard Raft message types for leader election and log replication.
CONSTANTS RequestVoteRequest, RequestVoteResponse,
          AppendEntriesRequest, AppendEntriesResponse

\* Hovercraft++ specific message types for interactions involving the NetAgg component.
CONSTANTS AppendEntriesNetAggRequest, AggCommit

\* Limits the total number of client requests processed in the model, 
\* used for bounding the state space during model checking.
CONSTANTS MaxClientRequests

\* Limits the number of times any given server can transition to 
\* the Leader state, for state space bounding.
CONSTANTS MaxBecomeLeader

\* Defines the maximum value a term number can reach, used for state space bounding.
CONSTANTS MaxTerm

\************************* VARIABLES *******************************************

\* Global variables

\* A bag of records representing requests and responses sent from one server
\* to another. This is a function mapping Message to Nat.
VARIABLE messages

\* An instrumentation variable mapping each server ID (from Server) 
\* to a natural number, counting how many times it has become a leader. 
\* Used with MaxBecomeLeader for state space bounding.
VARIABLE leaderCount

\* An instrumentation variable; a natural number tracking the count of 
\* client requests processed so far. Used with MaxClientRequests for 
\* state space bounding.
VARIABLE maxc

\* variable for tracking entry commit message counts
\* Maps <<logIndex, logTerm>> to a record tracking message counts.
\* [ sentCount |-> Nat,   \* AppendEntriesRequests sent for the entry
\*   ackCount  |-> Nat,   \* AppendEntriesResponses received for this entry
\*   committed |-> Bool ] \* Flag indicating if the entry is committed
VARIABLE entryCommitStats

\* A tuple grouping all instrumentation-specific variables. Useful for specifying 
\* UNCHANGED instrumentationVars in actions that do not modify them.
instrumentationVars == <<leaderCount, maxc, entryCommitStats>>

\* The unique identifier (ID from Server set) of the server designated 
\* to act as the Switch component.
VARIABLE switchIndex

\* The Switch's internal buffer. Maps a request identifier (<<value, term>>) 
\* to the complete request data and payload
VARIABLE switchBuffer

\* A per-server variable (maps Server ID to a set of request identifiers). 
\* For each server (Leader and Followers), it stores the set of request 
\* identifiers (<<value, term>> tuples) for payloads received from the Switch 
\* that are awaiting ordering metadata.
VARIABLE unorderedRequests

\* Records which <<value, term>> pairs the Switch has sent to each server.
\* Maps Server ID -> Set of <<Value, Term>> pairs.
VARIABLE switchSentRecord

\* A tuple grouping variables specific to the Hovercraft Switch functionality.
hovercraftVars == <<switchBuffer, unorderedRequests, 
                    switchIndex, switchSentRecord>>

\* NetAgg variables

\* Stores the leader (leader field, type Server) and its term (term field, type Nat) 
\* that NetAgg currently recognizes as active.
VARIABLE netAggCurrentLeaderTerm 

VARIABLE netAggIndex          \* Index of the NetAgg server
VARIABLE netAggMatchIndex     \* NetAgg's view of follower match indices
VARIABLE netAggPendingEntries \* Entries pending aggregation at NetAgg
VARIABLE netAggCommitIndex    \* NetAgg's view of commit index

\* A tuple grouping all variables specific to the NetAgg component.
netAggVars == <<netAggIndex, netAggMatchIndex, netAggPendingEntries, 
                netAggCommitIndex, netAggCurrentLeaderTerm>>

\* The following variables are all per server (functions with domain Server).

\* Each server's current known term number. (Maps Server ID to Nat).
VARIABLE currentTerm

\* The server's state (Follower, Candidate, Leader, Switch, or NetAgg).
\* For Switch and NetAgg entities, this state is fixed.
VARIABLE state

\* The candidate the server voted for in its current term, or
\* Nil if it hasn't voted for any.
VARIABLE votedFor

serverVars == <<currentTerm, state, votedFor>>

\* A Sequence of log entries. The index into this sequence is the index of the
\* log entry.
VARIABLE log

\* The index of the latest entry in the log the state machine may apply.
VARIABLE commitIndex

logVars == <<log, commitIndex>>

\* The following variables are used only on candidates:
\* The set of servers from which the candidate has received a RequestVote
\* response in its currentTerm.
VARIABLE votesResponded

\* The set of servers from which the candidate has received a vote in its
\* currentTerm.
VARIABLE votesGranted

\* A history variable used in the proof. This would not be present in an
\* implementation.
\* Function from each server that voted for this candidate in its currentTerm
\* to that voter's log.
VARIABLE voterLog

candidateVars == <<votesResponded, votesGranted, voterLog>>

\* The following variables are used only on leaders:
\* The next entry to send to each follower.
VARIABLE nextIndex

\* The latest entry that each follower has acknowledged is the same as the
\* leader's. This is used to calculate commitIndex on the leader.
VARIABLE matchIndex

leaderVars == <<nextIndex, matchIndex>>

\* The set of server IDs participating in the Raft consensus 
\* (i.e., excluding Switch and NetAgg components).
\*Servers == Server \ {switchIndex, netAggIndex} \* see Init
VARIABLE Servers

\* All variables; used for stuttering (asserting state hasn't changed).
\* Hovercraft++ brings hovercraftVars for Switch and netAggVars for NetAgg.
vars == <<messages, serverVars, candidateVars, leaderVars, logVars, 
          instrumentationVars, hovercraftVars, netAggVars, Servers>>

\************************* HELPERS *********************************************

\* Defines the set of all possible quorums. A quorum is any subset of Servers 
\* (Raft participants) forming a simple majority. The critical property is that 
\* any two quorums must overlap.
Quorum == {i \in SUBSET(Servers) : Cardinality(i) * 2 > Cardinality(Servers)}

\* The term of the last entry in a log, or 0 if the log is empty.
LastTerm(xlog) == IF Len(xlog) = 0 THEN 0 ELSE xlog[Len(xlog)].term

WithMessage(m, msgs) ==
    IF m \in DOMAIN msgs THEN
        msgs \* avoiding duplicates
    ELSE
        msgs @@ (m :> 1)

\* to allow duplicates use: WithMessage(m, msgs) ==
\* [msgs EXCEPT ![m] = IF m \in DOMAIN msgs THEN msgs[m] + 1 ELSE 1]

WithoutMessage(m, msgs) ==
    IF m \in DOMAIN msgs THEN
        [msgs EXCEPT ![m] = IF msgs[m] > 0 THEN msgs[m] - 1 ELSE 0 ]
    ELSE
        msgs

\* Add a message to the bag of messages.
Send(m) == messages' = WithMessage(m, messages)

\* Remove a message from the bag of messages. Used when a server is done
\* processing a message.
Discard(m) == messages' = WithoutMessage(m, messages)

\* Helper for Send and Reply. Given a message m and bag of messages, return a
\* Combination of Send and Discard
Reply(response, request) ==
    messages' = WithoutMessage(request, WithMessage(response, messages))

\* Return the minimum value from a set, or undefined if the set is empty.
Min(s) == CHOOSE x \in s : \A y \in s : x <= y

\* Return the maximum value from a set, or undefined if the set is empty.
Max(s) == CHOOSE x \in s : \A y \in s : x >= y

\* Convert a sequence to a set of its elements
SeqToSet(seq) == { seq[i] : i \in DOMAIN seq }

min(a, b) == IF a < b THEN a ELSE b

ValidMessage(msgs) ==
    { m \in DOMAIN messages : msgs[m] > 0 }

\* The prefix of the log of server i that has been committed up to term x
CommittedTermPrefix(i, x) ==
\* Only if log of i is non-empty, and if there exists an entry up to the term x
    IF Len(log[i]) /= 0 /\ \E y \in DOMAIN log[i] : log[i][y].term <= x
    THEN
\* then, we use the subsequence up to the maximum committed term of the leader
      LET maxTermIndex ==
          CHOOSE y \in DOMAIN log[i] :
            /\ log[i][y].term <= x
            /\ \A z \in DOMAIN log[i] : log[i][z].term <= x  => y >= z
      IN SubSeq(log[i], 1, min(maxTermIndex, commitIndex[i]))
    \* Otherwise the prefix is the empty tuple
    ELSE << >>

CheckIsPrefix(seq1, seq2) ==
    /\ Len(seq1) <= Len(seq2)
    /\ \A i \in 1..Len(seq1) : seq1[i] = seq2[i]

\* The prefix of the log of server i that has been committed
Committed(i) ==
    IF commitIndex[i] = 0
    THEN << >>
    ELSE SubSeq(log[i],1,commitIndex[i])

MyConstraint == (\A i \in Servers: currentTerm[i] <= MaxTerm 
                 /\ Len(log[i]) <= MaxClientRequests ) 
                 /\ (\A m \in DOMAIN messages: messages[m] <= 1)

\************************* INIT ************************************************

InitHistoryVars == voterLog  = [i \in Servers |-> [j \in {} |-> <<>>]]

InitServerVars == /\ currentTerm = [i \in Servers |-> 1]
                  /\ state       = [i \in Servers |-> Follower]
                  /\ votedFor    = [i \in Servers |-> Nil]

InitCandidateVars == /\ votesResponded = [i \in Servers |-> {}]
                     /\ votesGranted   = [i \in Servers |-> {}]

\* The values nextIndex[i][i] and matchIndex[i][i] are never read, since the
\* leader does not send itself messages. It's still easier to include these
\* in the functions.
InitLeaderVars == /\ nextIndex  = [i \in Servers |-> [j \in Servers |-> 1]]
                  /\ matchIndex = [i \in Servers |-> [j \in Servers |-> 0]]

InitLogVars == /\ log          = [i \in Servers |-> << >>]
               /\ commitIndex  = [i \in Servers |-> 0]
               
Init == 
    /\ messages = [m \in {} |-> 0]
    /\ switchIndex = CHOOSE s \in Server : TRUE  \* Pick any server as switch
    /\ netAggIndex = CHOOSE n \in Server \ {switchIndex} : TRUE  \* Pick another as NetAgg
    /\ Servers = Server \ {switchIndex, netAggIndex}  \* Remaining are Raft servers
    
    \* Initialize all server state
    /\ currentTerm = [i \in Server |-> 1]
    /\ state = [i \in Server |-> 
                  IF i = switchIndex THEN Switch
                  ELSE IF i = netAggIndex THEN NetAgg
                  ELSE Follower]
    /\ votedFor = [i \in Server |-> Nil]
    
    \* Initialize empty logs and indices
    /\ log = [i \in Server |-> << >>]
    /\ commitIndex = [i \in Server |-> 0]
    /\ nextIndex = [i \in Server |-> [j \in Server |-> 1]]
    /\ matchIndex = [i \in Server |-> [j \in Server |-> 0]]
    
    \* Initialize candidate variables
    /\ votesResponded = [i \in Server |-> {}]
    /\ votesGranted = [i \in Server |-> {}]
    /\ voterLog = [i \in Server |-> [j \in {} |-> <<>>]]
    
    \* Initialize HoverCraft Switch variables
    /\ switchBuffer = [vt \in {} |-> {}]
    /\ unorderedRequests = [s \in Server |-> {}]
    /\ switchSentRecord = [s \in Server |-> {}]
    
    \* Initialize HoverCraft NetAgg variables
    /\ netAggCurrentLeaderTerm = Nil
    /\ netAggMatchIndex = [s \in {} |-> 0]
    /\ netAggPendingEntries = {}
    /\ netAggCommitIndex = 0
    
    \* Initialize instrumentation
    /\ maxc = 0
    /\ leaderCount = [i \in Server |-> 0]
    /\ entryCommitStats = [idx_term \in {} |-> 
                          [sentCount |-> 0, ackCount |-> 0, committed |-> FALSE]]

\* Used to start from a state with a Leader.
\* We may verify just the normal case excluding leader election and crashes.
MyInit ==
    LET ServerSet5 == CHOOSE S \in SUBSET(Server) : Cardinality(S) = 5
        TheSwitchId == CHOOSE s \in ServerSet5 : TRUE
        TempSet == ServerSet5 \ {TheSwitchId}
        TheNetAggId == CHOOSE n \in TempSet : TRUE
        TempSet2 == TempSet \ {TheNetAggId}
        TheLeaderId == CHOOSE l \in TempSet2 : TRUE
        FollowerIds == TempSet2 \ {TheLeaderId}

        TheState == [ s \in Server |->
                        IF s = TheSwitchId THEN Switch
                        ELSE IF s = TheNetAggId THEN NetAgg
                        ELSE IF s = TheLeaderId THEN Leader
                        ELSE IF s \in FollowerIds THEN Follower
                        ELSE Follower
                    ]
        TheSwitchIndex == TheSwitchId
        TheNetAggIndex == TheNetAggId
        TheServersSet == Server \ {TheSwitchIndex, TheNetAggIndex}
        Voters == TheServersSet \ {TheLeaderId}
    IN
    \* Constraint: Ensure Server has enough elements
    /\ Cardinality(Server) >= 5
    /\ PrintT("MyInit: switchIndex=" \o ToString(TheSwitchIndex))
    /\ PrintT("MyInit: netAggIndex=" \o ToString(TheNetAggIndex))
    /\ PrintT("MyInit: Leader is=" \o ToString(TheLeaderId))
    /\ PrintT("MyInit: Servers=" \o ToString(TheServersSet))
\*    /\ PrintT("MyInit: state[switchIndex]=" \o ToString(TheState[TheSwitchIndex]))
\*    /\ PrintT("MyInit: state[LeaderId]=" \o ToString(TheState[TheLeaderId]))
\*    /\ PrintT("MyInit: switchBuffer Domain=" \o ToString(DOMAIN [vt \in {} |-> {}]))

    /\ commitIndex = [s \in Server |-> 0]
    /\ currentTerm = [s \in Server |-> 2]
    /\ leaderCount = [s \in Server |-> IF s = TheLeaderId THEN 1 ELSE 0]
    /\ log = [s \in Server |-> << >>]
    /\ matchIndex = [s \in Server |-> [t \in Server |-> 0]]
    /\ maxc = 0
    /\ messages = [m \in {} |-> 0]
    /\ nextIndex = [s \in Server |-> [t \in Server |-> 1]]
    /\ state = TheState
    /\ votedFor = [s \in Server |-> 
                   IF s = TheLeaderId THEN Nil ELSE TheLeaderId]
    /\ voterLog = [s \in Server |-> 
                   IF s = TheLeaderId THEN 
                   [ v \in Voters |-> <<>> ] ELSE [ v \in {} |-> <<>> ] ]
    /\ votesGranted = [s \in Server |-> 
                       IF s = TheLeaderId THEN Voters ELSE {}]
    /\ votesResponded = [s \in Server |-> 
                         IF s = TheLeaderId THEN Voters ELSE {}]
    /\ entryCommitStats = [ idx_term \in {} |-> 
                           [ sentCount |-> 0, 
                             ackCount |-> 0, 
                             committed |-> FALSE ] ]
    /\ switchBuffer = [vt \in {} |-> {}]
    /\ unorderedRequests = [s \in Server |-> {}]
    /\ switchSentRecord = [s \in Server |-> {}] 
    /\ switchIndex = TheSwitchIndex
    /\ netAggIndex = TheNetAggIndex
    /\ netAggMatchIndex = [s \in TheServersSet |-> 0]
    /\ netAggPendingEntries = {}
    /\ netAggCommitIndex = 0
    /\ netAggCurrentLeaderTerm = [leader |-> TheLeaderId, term |-> 2]
    /\ Servers = TheServersSet

\*********************** Actions ********************************

\* Modified to limit Restarts only for Leaders.
\* Server i restarts from stable storage.
\* It loses everything but its currentTerm, votedFor, and log.
\* Also persists messages, instrumentation and Switch/NetAgg variables.
Restart(i) ==
    /\ state[i] = Leader
    /\ (\A srv \in Servers : leaderCount[srv] < MaxBecomeLeader)
    /\ state'          = [state EXCEPT ![i] = Follower]
    /\ votesResponded' = [votesResponded EXCEPT ![i] = {}]
    /\ votesGranted'   = [votesGranted EXCEPT ![i] = {}]
    /\ voterLog'       = [voterLog EXCEPT ![i] = [j \in {} |-> <<>>]]
    /\ nextIndex'      = [nextIndex EXCEPT ![i] = [j \in Server |-> 1]]
    /\ matchIndex'     = [matchIndex EXCEPT ![i] = [j \in Server |-> 0]]
    /\ commitIndex'    = [commitIndex EXCEPT ![i] = 0]
    /\ unorderedRequests' = [unorderedRequests EXCEPT ![i] = {}]
    /\ switchSentRecord' = [switchSentRecord EXCEPT ![i] = {}]
    /\ IF netAggCurrentLeaderTerm /= Nil /\ netAggCurrentLeaderTerm.leader = i
       THEN /\ netAggCurrentLeaderTerm' = Nil \* Deactivate NetAgg
            /\ netAggPendingEntries' = {}    \* Flush pending entries
            \* netAggCommitIndex and netAggMatchIndex could be left or reset;
            \* they become irrelevant until a new leader activates NetAgg.
       ELSE /\ UNCHANGED netAggCurrentLeaderTerm
            /\ UNCHANGED netAggPendingEntries
    /\ UNCHANGED <<messages, currentTerm, votedFor, log, instrumentationVars, 
                   switchIndex, switchBuffer, Servers, 
                   netAggIndex, netAggMatchIndex, netAggCommitIndex>>

\* Server i times out and starts a new election. Follower -> Candidate
Timeout(i) == /\ state[i] \in {Follower, Candidate}
              /\ (\A srv \in Servers : leaderCount[srv] < MaxBecomeLeader)
              /\ currentTerm[i] < MaxTerm
              /\ state' = [state EXCEPT ![i] = Candidate]
              /\ currentTerm' = [currentTerm EXCEPT ![i] = currentTerm[i] + 1]
              \* Most implementations would probably just set the local vote
              \* atomically, but messaging localhost for it is weaker.
              /\ votedFor' = [votedFor EXCEPT ![i] = Nil]
              /\ votesResponded' = [votesResponded EXCEPT ![i] = {}]
              /\ votesGranted'   = [votesGranted EXCEPT ![i] = {}]
              /\ voterLog'       = [voterLog EXCEPT ![i] = [j \in {} |-> <<>>]]
              /\ UNCHANGED <<messages, leaderVars, logVars, 
                             instrumentationVars, hovercraftVars, 
                             Servers, netAggVars>>

\* Modified to restrict Leader transitions, bounded by MaxBecomeLeader
\* Candidate i transitions to leader. Candidate -> Leader
BecomeLeader(i) ==
    /\ state[i] = Candidate
    /\ votesGranted[i] \in Quorum
    /\ leaderCount[i] < MaxBecomeLeader
    /\ state'      = [state EXCEPT ![i] = Leader]
    /\ nextIndex'  = [nextIndex EXCEPT ![i] =
                         [j \in Server |-> Len(log[i]) + 1]]
    /\ matchIndex' = [matchIndex EXCEPT ![i] =
                         [j \in Server |-> 0]]
    /\ leaderCount' = [leaderCount EXCEPT ![i] = leaderCount[i] + 1]
    /\ netAggCurrentLeaderTerm' = [leader |-> i, term |-> currentTerm[i]]
    /\ netAggPendingEntries' = {}  \* Flush pending entries for the new leader
    /\ netAggCommitIndex' = commitIndex[i]
    /\ netAggMatchIndex' = [s \in Servers |-> 0]
    /\ UNCHANGED <<messages, currentTerm, votedFor, candidateVars, 
                   logVars, maxc, entryCommitStats, hovercraftVars, 
                   Servers, netAggIndex>> 

\* Modified up to MaxTerm; Back To Follower.
\* Any RPC with a newer term causes the recipient to advance its term first.
UpdateTerm(i, j, m) ==
    /\ state[i] \notin {Switch, NetAgg} /\ state[j] \notin {Switch, NetAgg}
    /\ m.mterm > currentTerm[i]
    /\ m.mterm < MaxTerm
    /\ LET wasLeader == state[i] = Leader
       IN
       /\ currentTerm'    = [currentTerm EXCEPT ![i] = m.mterm]
       /\ state'          = [state       EXCEPT ![i] = Follower]
       /\ votedFor'       = [votedFor    EXCEPT ![i] = Nil]
       /\ IF wasLeader /\ netAggCurrentLeaderTerm /= Nil /\ netAggCurrentLeaderTerm.leader = i
          THEN /\ netAggCurrentLeaderTerm' = Nil \* Deactivate NetAgg
               /\ netAggPendingEntries' = {}    \* Flush pending entries
          ELSE /\ UNCHANGED netAggCurrentLeaderTerm
               /\ UNCHANGED netAggPendingEntries
       \* messages is unchanged so m can be processed further.
    /\ UNCHANGED <<messages, candidateVars, leaderVars, logVars, 
                   instrumentationVars, hovercraftVars, Servers, 
                   netAggIndex, netAggMatchIndex, netAggCommitIndex>>

\***************************** REQUEST VOTE ***********************************

\* Message handlers
\* i = recipient, j = sender, m = message

\* Candidate i sends j a RequestVote request.
RequestVote(i, j) ==
    /\ state[i] = Candidate 
    /\ state[j] \notin {Switch, NetAgg}
    /\ j \notin votesResponded[i]
    /\ Send([mtype         |-> RequestVoteRequest,
             mterm         |-> currentTerm[i],
             mlastLogTerm  |-> LastTerm(log[i]),
             mlastLogIndex |-> Len(log[i]),
             msource       |-> i,
             mdest         |-> j])
    /\ UNCHANGED <<serverVars, candidateVars, leaderVars, logVars, 
                   instrumentationVars, hovercraftVars, Servers, netAggVars>>

\* Server i receives a RequestVote request from server j with
\* m.mterm <= currentTerm[i].
HandleRequestVoteRequest(i, j, m) ==
    LET logOk == \/ m.mlastLogTerm > LastTerm(log[i])
                 \/ /\ m.mlastLogTerm = LastTerm(log[i])
                    /\ m.mlastLogIndex >= Len(log[i])
        grant == /\ m.mterm = currentTerm[i]
                 /\ logOk
                 /\ votedFor[i] \in {Nil, j}
    IN /\ m.mterm <= currentTerm[i]
       /\ \/ grant  /\ votedFor' = [votedFor EXCEPT ![i] = j]
          \/ ~grant /\ UNCHANGED votedFor
       /\ Reply([mtype        |-> RequestVoteResponse,
                 mterm        |-> currentTerm[i],
                 mvoteGranted |-> grant,
                 \* mlog is used just for the `elections' history variable for
                 \* the proof. It would not exist in a real implementation.
                 mlog         |-> log[i],
                 msource      |-> i,
                 mdest        |-> j],
                 m)
       /\ UNCHANGED <<state, currentTerm, candidateVars, leaderVars, logVars, 
                      instrumentationVars, hovercraftVars, Servers, netAggVars>>

\* Server i receives a RequestVote response from server j with
\* m.mterm = currentTerm[i].
HandleRequestVoteResponse(i, j, m) ==
    \* This tallies votes even when the current state is not Candidate, but
    \* they won't be looked at, so it doesn't matter.
    /\ m.mterm = currentTerm[i]
    /\ votesResponded' = [votesResponded EXCEPT ![i] =
                              votesResponded[i] \cup {j}]
    /\ \/ /\ m.mvoteGranted
          /\ votesGranted' = [votesGranted EXCEPT ![i] =
                                  votesGranted[i] \cup {j}]
          /\ voterLog' = [voterLog EXCEPT ![i] =
                              voterLog[i] @@ (j :> m.mlog)]
       \/ /\ ~m.mvoteGranted
          /\ UNCHANGED <<votesGranted, voterLog>>
    /\ Discard(m)
    /\ UNCHANGED <<serverVars, votedFor, leaderVars, logVars, 
                   instrumentationVars, hovercraftVars, Servers, netAggVars>>

\* Responses with stale terms are ignored.
DropStaleResponse(i, j, m) ==
    /\ m.mterm < currentTerm[i]
    /\ Discard(m)
    /\ UNCHANGED <<serverVars, candidateVars, leaderVars, logVars, 
                   instrumentationVars, hovercraftVars, Servers, netAggVars>>

\***************************** AppendEntries **********************************

\* Leader i ingests a request v that has been replicated to its unordered set.
LeaderIngestHovercRaftRequest(i, vt) ==
    /\ state[i] = Leader
    /\ vt \in unorderedRequests[i]      \* Request ID is pending for the leader
    /\ vt \in DOMAIN switchBuffer       \* use switch buffer to reduce payload duplication 
    /\ maxc < MaxClientRequests
    /\ LET entryFromBuffer == switchBuffer[vt]
           v == vt[1]  \* Extract value from <<value, term>> pair
           \* Use leader's current term, keep value and payload from buffer
           newEntry == [term |-> currentTerm[i], 
                        value |-> v, 
                        payload |-> entryFromBuffer.payload]
           entryExists == \E k \in DOMAIN log[i] : 
                          log[i][k].value = v /\ log[i][k].term = newEntry.term
           newLog == IF entryExists THEN log[i] ELSE Append(log[i], newEntry)
           newEntryIndex == Len(log[i]) + 1
           newEntryKey == <<newEntryIndex, newEntry.term>>
       IN
        /\ log' = [log EXCEPT ![i] = newLog]
        /\ maxc' = IF entryExists THEN maxc ELSE maxc + 1
        /\ entryCommitStats' =
              IF ~entryExists /\ newEntryIndex > 0
              THEN entryCommitStats @@ (newEntryKey :> [ sentCount |-> 0, 
                                   ackCount |-> 0, committed |-> FALSE ])
              ELSE entryCommitStats
        /\ unorderedRequests' = [unorderedRequests EXCEPT ![i] = @ \ {vt}]
    /\ UNCHANGED <<messages, serverVars, candidateVars, leaderVars, 
                   commitIndex, leaderCount, switchIndex, switchBuffer, 
                   Servers, switchSentRecord, netAggVars>>


\* Client sends a request to the Switch, which buffers it, 
\* not yet replicated to servers
\* s is the Switch, i is the leader, v is the request value 
\* (along with term will represent a request ID in this model)
SwitchClientRequest(s, i, v) ==
    /\ state[s] = Switch  \* Only the switch server can process client requests
    /\ state[i] = Leader
    /\ LET vt == <<v, currentTerm[i]>>  \* Create <<value, term>> pair
       IN
       /\ vt \notin DOMAIN switchBuffer  \* Only process new requests
       /\ LET entryWithPayload == [term |-> currentTerm[i], 
                                   value |-> v, payload |-> v]
          IN
          /\ switchBuffer' = switchBuffer @@ (vt :> entryWithPayload)
          /\ unorderedRequests' = 
             [unorderedRequests EXCEPT ![s] = unorderedRequests[s] \cup {vt}]
    /\ UNCHANGED <<messages, serverVars, candidateVars, leaderVars, logVars, 
                   leaderCount, entryCommitStats, switchIndex, maxc, 
                   Servers, switchSentRecord, netAggVars>>

\* check that Server i is not a Switch or NetAgg
RaftState(i) == state[i] \notin {Switch, NetAgg}

\* The Switch replicates vt to ALL servers at once (except those that already have it).
\* This reduces state space by avoiding intermediate states 
\*where only some servers have received the request.
SwitchClientRequestReplicateAll(s, vt) ==
    /\ state[s] = Switch  \* Only the switch server can replicate requests
    /\ vt \in unorderedRequests[s] \* Request must be pending at the switch
    /\ LET \* Find all servers that haven't received this v/term pair yet
        
        targetServers == {i \in Server : RaftState(i) /\ vt \notin switchSentRecord[i]}
       IN
       /\ targetServers /= {}  \* At least one server needs the request
       /\ unorderedRequests' = [i \in Server |->
            IF i \in targetServers 
            THEN unorderedRequests[i] \cup {vt}
            ELSE unorderedRequests[i]]
       /\ switchSentRecord' = [i \in Server |->
            IF i \in targetServers
            THEN switchSentRecord[i] \cup {vt}
            ELSE switchSentRecord[i]]
    /\ UNCHANGED <<messages, serverVars, candidateVars, leaderVars, logVars,
                   leaderCount, entryCommitStats, switchIndex, switchBuffer, 
                   maxc, Servers, netAggVars>>

\* Follower i drops/loses one request vt from its unordered requests
\* This simulates network loss or follower crash scenarios
FollowerDropRequest(i, vt) ==
    /\ state[i] = Follower  \* Only followers can drop requests
    /\ vt \in unorderedRequests[i]  \* Request must exist in follower's buffer
    /\ vt \in DOMAIN switchBuffer   \* Request must still exist in switch buffer
    /\ unorderedRequests' = [unorderedRequests EXCEPT ![i] = @ \ {vt}]
    /\ UNCHANGED <<messages, serverVars, candidateVars, leaderVars, logVars,
                   instrumentationVars, switchIndex, switchBuffer, 
                   switchSentRecord, Servers, netAggVars>>

\* Leader i sends AppendEntries to NetAgg instead of directly to followers
AppendEntriesToNetAgg(i) ==
    /\ state[i] = Leader
    /\ state[netAggIndex] = NetAgg
    /\ Len(log[i]) > 0
    /\ LET nextIndexMin == Min({nextIndex[i][j] : j \in Servers \ {i}})
       IN nextIndexMin <= Len(log[i])
    /\ LET entryIndex == Min({nextIndex[i][j] : j \in Servers \ {i}})
           entry == log[i][entryIndex]
           entryMetadata == [term |-> entry.term, value |-> entry.value]
           entries == << entryMetadata >>
           prevLogIndex == entryIndex - 1
           prevLogTerm == IF prevLogIndex > 0 THEN
                              log[i][prevLogIndex].term
                          ELSE 0
       IN Send([mtype          |-> AppendEntriesNetAggRequest,
                mterm          |-> currentTerm[i],
                mprevLogIndex  |-> prevLogIndex,
                mprevLogTerm   |-> prevLogTerm,
                mentries       |-> entries,
                mentryIndex    |-> entryIndex,
                mlog           |-> log[i],
                mcommitIndex   |-> Min({commitIndex[i], entryIndex}),
                msource        |-> i,
                mdest          |-> netAggIndex])
    /\ UNCHANGED <<serverVars, candidateVars, leaderVars, logVars, 
                   instrumentationVars, hovercraftVars, netAggVars, Servers, netAggVars>>

\* NetAgg receives AppendEntries from leader and forwards to ALL followers atomically
NetAggForwardAppendEntriesAll(m) ==
    /\ m.mdest = netAggIndex
    /\ m.mtype = AppendEntriesNetAggRequest
    /\ netAggCurrentLeaderTerm /= Nil                 \* NetAgg must be active
    /\ netAggCurrentLeaderTerm.leader = m.msource     \* Request from current assigned leader
    /\ netAggCurrentLeaderTerm.term = m.mterm 
    /\ LET leaderId == m.msource
           followers == Servers \ {leaderId}  \* All servers except the leader
           \* Create the set of messages to send to all followers
           followerMessages == { [ mtype          |-> AppendEntriesRequest,
                                  mterm          |-> m.mterm,
                                  mprevLogIndex  |-> m.mprevLogIndex,
                                  mprevLogTerm   |-> m.mprevLogTerm,
                                  mentries       |-> m.mentries,
                                  mlog           |-> m.mlog,
                                  mcommitIndex   |-> m.mcommitIndex,
                                  msource        |-> netAggIndex,
                                  mdest          |-> f,
                                  moriginalLeader |-> leaderId ]
                                : f \in followers }
           \* Remove the processed message and add all new messages
           RemainingActiveMessages == ValidMessage(WithoutMessage(m, messages))
           \* Update sentCount for this entry
           entryIndex == m.mentryIndex
           entryTerm == m.mentries[1].term
           entryKey == <<entryIndex, entryTerm>>
       IN
       /\ messages' = [ msgRec \in RemainingActiveMessages \cup followerMessages |-> 1 ]
       /\ netAggPendingEntries' = netAggPendingEntries \cup
             {[entryIndex |-> m.mentryIndex,
               entryTerm  |-> m.mentries[1].term,
               leaderId   |-> leaderId,
               ackCount   |-> 0,
               acksFrom   |-> {} ]}
       /\ entryCommitStats' = 
            IF entryKey \in DOMAIN entryCommitStats /\ ~entryCommitStats[entryKey].committed
            THEN [entryCommitStats EXCEPT ![entryKey].sentCount = @ + Cardinality(followers)]
            ELSE entryCommitStats
    /\ UNCHANGED <<serverVars, candidateVars, leaderVars, logVars,
                   leaderCount, maxc, hovercraftVars,
                   netAggIndex, netAggMatchIndex, netAggCommitIndex, 
                   netAggCurrentLeaderTerm, Servers>>

\* NetAgg receives AppendEntries response from follower
NetAggHandleAppendEntriesResponse(m) ==
    /\ m.mtype = AppendEntriesResponse
    /\ m.mdest = netAggIndex   \* Message is for NetAgg
    /\ netAggCurrentLeaderTerm /= Nil \* NetAgg must be active
    /\ m.msuccess     \* Process successful ACKs, NACKs go to Leader for point to point recovery
    /\ \E pending \in netAggPendingEntries :
        /\ m.msource \in (Servers \ {pending.leaderId})  
        \* Response is from a follower (in Servers) of the leader for this pending entry
        /\ m.mmatchIndex >= pending.entryIndex           
        \* Follower acknowledged this entry (or beyond)
        /\ m.msource \notin pending.acksFrom             
        \* This is a new ACK from this follower for this item
        /\ (IF m.msource \notin DOMAIN netAggMatchIndex
            THEN PrintT("DEBUG: m.msource NOT IN DOMAIN netAggMatchIndex"
                       \o "\n  m.msource = " \o ToString(m.msource)
                       \o "\n  m = " \o ToString(m)
                       \o "\n  pending = " \o ToString(pending)
                       \o "\n  netAggMatchIndex = " \o ToString(netAggMatchIndex)
                       \o "\n  DOMAIN netAggMatchIndex = " \o ToString(DOMAIN netAggMatchIndex)
                       \o "\n  Servers = " \o ToString(Servers)
                       \o "\n  netAggCurrentLeaderTerm = " \o ToString(netAggCurrentLeaderTerm)
                       \o "\n  netAggPendingEntries = " \o ToString(netAggPendingEntries)
                       \o "\n  currentTerm = " \o ToString(currentTerm)
                       \o "\n  state = " \o ToString(state)
                       \o "\n  log length for m.msource = " \o ToString(Len(log[m.msource]))
                       \o "\n  commitIndex for m.msource = " \o ToString(commitIndex[m.msource])
                       \o "\n  All messages = " \o ToString(messages) \* Might be very verbose
                      )
            ELSE TRUE)
        /\ LET updatedPending == [pending EXCEPT !.acksFrom = @ \cup {m.msource} ]
               RequiredFollowerAcks == Cardinality(Servers) \div 2 
               \* Leader has one, need this many more from followers
           IN
           /\ m.msource \in DOMAIN netAggMatchIndex
           /\ netAggMatchIndex' = [netAggMatchIndex EXCEPT ![m.msource] = m.mmatchIndex]
           /\ IF Cardinality(updatedPending.acksFrom) >= RequiredFollowerAcks
              THEN \* Majority reached, send AGG_COMMIT to all Raft Servers
               LET AggCommitMsgsSet == { [ mtype        |-> AggCommit,
                                           mcommitIndex |-> pending.entryIndex,
                                           msource      |-> netAggIndex,
                                           mdest        |-> srv,
                                           mterm        |-> pending.entryTerm ]
                                         : srv \in Servers } \* Send to all Raft servers
                   \* Messages that were valid, excluding the one we just processed
                   RemainingActiveMessages == ValidMessage(WithoutMessage(m, messages))
               IN
               /\ messages' = [ msgRec \in RemainingActiveMessages \cup AggCommitMsgsSet |-> 1 ]
   \* This creates the new message bag:
   \* - 'm' is effectively removed (as it's not in RemainingActiveMessages).
   \* - All messages in AggCommitMsgsSet are added (or kept if already there by chance).
   \* - All other previously active messages are preserved.
   \* - All messages in the resulting bag have count 1, respecting MyConstraint.
               /\ netAggPendingEntries' = netAggPendingEntries \ {pending} 
               \* Remove committed entry from pending
               /\ netAggCommitIndex' = Max({netAggCommitIndex, pending.entryIndex})
               /\ UNCHANGED <<serverVars, candidateVars, leaderVars, logVars,
                              instrumentationVars, hovercraftVars, netAggIndex, 
                              netAggCurrentLeaderTerm, Servers>>
              ELSE \* Majority not yet reached
               /\ Discard(m) \* This defines messages' = WithoutMessage(m, messages)
               /\ netAggPendingEntries' = (netAggPendingEntries \ {pending}) \cup {updatedPending}
               /\ UNCHANGED netAggCommitIndex
               /\ UNCHANGED <<serverVars, candidateVars, leaderVars, logVars,
                              instrumentationVars, hovercraftVars, netAggIndex, Servers,
                              netAggCurrentLeaderTerm >>


\* Server receives AGG_COMMIT from NetAgg
HandleAggCommit(i, m) ==
    /\ m.mtype = AggCommit
    /\ m.mdest = i
    /\ state[i] \in {Leader, Follower}
    /\ ( (state[i] = Leader    /\ m.mterm = currentTerm[i]) \/    
    \* Leader: entry's term must match leader's current term
         (state[i] = Follower  /\ m.mterm <= currentTerm[i]) )     
         \* Follower: entry's term can be current or older (but not newer)
    /\ LET receivedCommitIndex == m.mcommitIndex  \* Added for clarity
           currentLogLen == Len(log[i])          \* Added: get current log length
           newAdvancedCommitIndex == Max({commitIndex[i], receivedCommitIndex}) 
           \* Renamed & Logic: advance if m.mcommitIndex is higher
           newCommitIndex == Min({newAdvancedCommitIndex, currentLogLen})     
           \* Modified: cap at current log length
           
           committedIndexes == { k \in Nat : /\ k > commitIndex[i]
                                             /\ k <= newCommitIndex }
           keysToUpdate == IF state[i] = Leader 
                          THEN { key \in DOMAIN entryCommitStats : 
                                 key[1] \in committedIndexes }
                          ELSE {}
       IN
       /\ commitIndex' = [commitIndex EXCEPT ![i] = newCommitIndex]
       /\ entryCommitStats' = IF state[i] = Leader
                              THEN [ key \in DOMAIN entryCommitStats |->
                                     IF key \in keysToUpdate
                                     THEN [ entryCommitStats[key] EXCEPT !.committed = TRUE ] 
                                     ELSE entryCommitStats[key] ]
                              ELSE entryCommitStats
       /\ IF state[i] = Leader
          THEN /\ nextIndex' = [nextIndex EXCEPT ![i] = 
                                 [j \in Server |-> 
                                   IF j \in Servers \ {i} 
                                   THEN Max({nextIndex[i][j], newCommitIndex + 1})
                                   ELSE nextIndex[i][j]]]
               /\ matchIndex' = [matchIndex EXCEPT ![i] = 
                                 [j \in Server |-> 
                                   IF j \in Servers \ {i}
                                   THEN Max({matchIndex[i][j], newCommitIndex})
                                   ELSE matchIndex[i][j]]]
          ELSE UNCHANGED <<nextIndex, matchIndex>>
    /\ Discard(m)
    /\ UNCHANGED <<serverVars, candidateVars, log, maxc, leaderCount,
                   hovercraftVars, netAggVars, Servers, netAggVars>>
                                      
\* Server i receives an AppendEntries request from server j with
\* m.mterm <= currentTerm[i]. This just handles m.entries of length 0 or 1, but
\* implementations could safely accept more by treating them the same as
\* multiple independent requests of 1 entry.
\* fails when Leader restarts
HandleAppendEntriesRequest(i, j, m) ==
    LET logOk == \/ m.mprevLogIndex = 0
                 \/ /\ m.mprevLogIndex > 0
                    /\ m.mprevLogIndex <= Len(log[i])
                    /\ m.mprevLogTerm = log[i][m.mprevLogIndex].term
        rejectHovercraftMismatchCondition == 
            /\ m.mentries /= << >>
            /\ LET entry == m.mentries[1]
                   v == entry.value 
                   msgTerm == entry.term 
               IN \lnot ( /\ <<v, msgTerm>> \in unorderedRequests[i]      
                          /\ <<v, msgTerm>> \in DOMAIN switchBuffer
                          /\ switchBuffer[<<v, msgTerm>>].term = msgTerm )
        
        \* Condition that triggers the CHOOSE for the leader; corner case NACK
        isReplyToLeaderCase == rejectHovercraftMismatchCondition \/ ~logOk
        
        \* Check if a leader exists to be chosen for the reply
        canChooseLeaderForReply == \E l_exists \in Servers : state[l_exists] = Leader
            
    IN /\ m.mterm <= currentTerm[i]
       /\ \/ /\ \* reject request branch
                /\ ( \* conditions for rejecting the request
                     \/ m.mterm < currentTerm[i]
                     \/ /\ m.mterm = currentTerm[i]
                        /\ state[i] = Follower
                        /\ \lnot logOk
                     \/ /\ m.mterm = currentTerm[i]
                        /\ state[i] = Follower
                        /\ rejectHovercraftMismatchCondition
                   )
                /\ LET respondTo == IF isReplyToLeaderCase
                                    THEN CHOOSE l \in Servers : state[l] = Leader 
                                    ELSE m.msource
                   IN Reply([mtype           |-> AppendEntriesResponse,
                             mterm           |-> currentTerm[i],
                             msuccess        |-> FALSE,
                             mmatchIndex     |-> 0,
                             msource         |-> i,
                             mdest           |-> respondTo],
                             m)
                /\ UNCHANGED <<serverVars, logVars, unorderedRequests>>

          \/ \* return to follower state
             /\ m.mterm = currentTerm[i]
             /\ state[i] = Candidate
             /\ state' = [state EXCEPT ![i] = Follower]
             /\ UNCHANGED <<currentTerm, votedFor, logVars, messages, 
                            unorderedRequests>>

          \/ \* accept request
             /\ m.mterm = currentTerm[i]
             /\ state[i] = Follower
             /\ logOk
             /\ LET index == m.mprevLogIndex + 1
                   respondToIfAccepted == m.msource \* respondTo will be m.msource here
                IN \/ \* already done with request or empty entries
                       /\ \/ m.mentries = << >>
                          \/ /\ m.mentries /= << >>
                             /\ Len(log[i]) >= index
                             /\ log[i][index].term = m.mentries[1].term
                       /\ commitIndex' = [commitIndex EXCEPT ![i] = m.mcommitIndex]   
                       /\ Reply([mtype           |-> AppendEntriesResponse,
                                 mterm           |-> currentTerm[i],
                                 msuccess        |-> TRUE,
                                 mmatchIndex     |-> m.mprevLogIndex + Len(m.mentries),
                                 msource         |-> i,
                                 mdest           |-> respondToIfAccepted], 
                                 m)
                       /\ UNCHANGED <<serverVars, log, unorderedRequests>>
                   \/ \* conflict: remove 1 entry 
                       /\ m.mentries /= << >>
                       /\ Len(log[i]) >= index
                       /\ log[i][index].term /= m.mentries[1].term
                       /\ LET newLog == SubSeq(log[i], 1, index - 1)
                          IN log' = [log EXCEPT ![i] = newLog]
                       /\ UNCHANGED <<serverVars, commitIndex, messages, unorderedRequests>>
                   \/ \* no conflict: append entry
                       /\ m.mentries /= << >>
                       /\ Len(log[i]) = m.mprevLogIndex
                       /\ \lnot rejectHovercraftMismatchCondition
                       /\ LET entryMetadata == m.mentries[1]
                              vt == <<entryMetadata.value, entryMetadata.term>>
                              fullEntryFromCache == switchBuffer[vt]
                              entryForLocalLog == [ term  |-> entryMetadata.term, 
                                         value |-> entryMetadata.value, 
                                         payload |-> fullEntryFromCache.payload ]
                          IN log' = [log EXCEPT ![i] = Append(log[i], entryForLocalLog)]
                             /\ unorderedRequests' = [unorderedRequests EXCEPT ![i] = @ \ {vt}]
                       /\ UNCHANGED <<serverVars, commitIndex, messages>>
       /\ UNCHANGED <<candidateVars, leaderVars, instrumentationVars, 
           switchBuffer, switchIndex, switchSentRecord, Servers, netAggVars>>

\* Server i receives an AppendEntries response from server j with
\* m.mterm = currentTerm[i].
HandleAppendEntriesResponse(i, j, m) ==
    /\ m.mterm = currentTerm[i]
    /\ \/ /\ m.msuccess \* successful
          /\ LET
                 newMatchIndex == m.mmatchIndex
                 entryKey == IF newMatchIndex > 0 /\ newMatchIndex <= Len(log[i])
                              THEN <<newMatchIndex, log[i][newMatchIndex].term>>
                              ELSE <<0, 0>> \* Invalid index or empty log
             IN /\ nextIndex'  = [nextIndex  EXCEPT ![i][j] = m.mmatchIndex + 1]
                /\ matchIndex' = [matchIndex EXCEPT ![i][j] = m.mmatchIndex]
                /\ entryCommitStats' =
                     IF /\ entryKey /= <<0, 0>>
                        /\ entryKey \in DOMAIN entryCommitStats
                        /\ ~entryCommitStats[entryKey].committed
                     THEN [entryCommitStats EXCEPT ![entryKey].ackCount = @ + 1]
                     ELSE entryCommitStats                     
       \/ /\ \lnot m.msuccess \* not successful
          /\ nextIndex' = [nextIndex EXCEPT ![i][j] =
                               Max({nextIndex[i][j] - 1, 1})]
          /\ UNCHANGED <<matchIndex, entryCommitStats>>
    /\ Discard(m)
    /\ UNCHANGED <<serverVars, candidateVars, logVars, maxc, leaderCount, 
                   hovercraftVars, Servers, netAggVars>>

\* Network state transitions

\* The network duplicates a message
\*DuplicateMessage(m) ==
\*    /\ Send(m)
\*    /\ UNCHANGED <<serverVars, candidateVars, leaderVars, logVars, 
\*                   instrumentationVars, hovercraftVars, Servers, netAggVars>>

\* The network drops a message
\*DropMessage(m) ==
\*    /\ Discard(m)
\*    /\ UNCHANGED <<serverVars, candidateVars, leaderVars, logVars, 
\*                   instrumentationVars, hovercraftVars, Servers, netAggVars>>

\* Receive a message.
Receive(m) ==
    LET i == m.mdest
        j == m.msource
    IN \* Any RPC with a newer term causes the recipient to advance
       \* its term first. Responses with stale terms are ignored.
       \/ UpdateTerm(i, j, m)
       \/ /\ m.mtype = RequestVoteRequest
          /\ HandleRequestVoteRequest(i, j, m)
       \/ /\ m.mtype = RequestVoteResponse
          /\ \/ DropStaleResponse(i, j, m)
             \/ HandleRequestVoteResponse(i, j, m)
       \/ /\ m.mtype = AppendEntriesRequest
          /\ HandleAppendEntriesRequest(i, j, m)
       \/ /\ m.mtype = AppendEntriesResponse
          /\ \/ DropStaleResponse(i, j, m)
             \/ HandleAppendEntriesResponse(i, j, m)

\* Modified. Leader i sends j an AppendEntries request containing exactly 1 entry.
\* While implementations may want to send more than 1 at a time, this spec uses
\* just 1 because it minimizes atomic regions without loss of generality.
\* Sending empty entries is done for telling followers Leader is alive.
AppendEntries(i, j) ==
    /\ i /= j
    /\ state[i] = Leader
    /\ Len(log[i]) > 0  
    \* Only proceed if the leader has entries to send
    /\ nextIndex[i][j] <= Len(log[i])  
    \*  Only proceed if there are entries to send to this follower
    /\ matchIndex[i][j] < nextIndex[i][j] 
    \* Only send if follower hasn't already acknowledged this index
    /\ LET entryIndex == nextIndex[i][j]
           entry == log[i][entryIndex]
           entryMetadata == [term |-> entry.term, value |-> entry.value]
           entries == << entryMetadata >>
           entryKey == <<entryIndex, entry.term>>
           prevLogIndex == entryIndex - 1
           prevLogTerm == IF prevLogIndex > 0 THEN
                              log[i][prevLogIndex].term
                          ELSE
                              0
           \* Send up to 1 entry, constrained by the end of the log.
           \* lastEntry == Min({Len(log[i]), nextIndex[i][j]})
           \* entries == SubSeq(log[i], nextIndex[i][j], lastEntry)
           
       IN Send([mtype          |-> AppendEntriesRequest,
                mterm          |-> currentTerm[i],
                mprevLogIndex  |-> prevLogIndex,
                mprevLogTerm   |-> prevLogTerm,
                mentries       |-> entries,
                \* mlog is used as a history variable for the proof.
                \* It would not exist in a real implementation.
                mlog           |-> log[i],
                mcommitIndex   |-> Min({commitIndex[i], entryIndex}),
                msource        |-> i,
                mdest          |-> j])
       /\ entryCommitStats' =
            IF entryKey \in DOMAIN entryCommitStats 
               /\ ~entryCommitStats[entryKey].committed
            THEN [entryCommitStats EXCEPT ![entryKey].sentCount = @ + 1]
            ELSE entryCommitStats         
    /\ UNCHANGED <<serverVars, candidateVars, leaderVars, logVars, maxc, 
                   leaderCount, hovercraftVars, Servers, netAggVars>>

MySwitchPlusPlusNext == 
   \* Switch actions (client request handling)
   \/ \E i \in Servers, v \in Value : 
        state[i] = Leader /\ SwitchClientRequest(switchIndex, i, v)

   \/ \E v \in DOMAIN switchBuffer : 
        SwitchClientRequestReplicateAll(switchIndex, v)

   \/ \E i \in Servers, v \in DOMAIN switchBuffer : 
        state[i] = Leader /\ LeaderIngestHovercRaftRequest(i, v)

   \* NetAgg path: Leader sends to NetAgg instead of direct AppendEntries
   \/ \E i \in Servers : 
        state[i] = Leader /\ AppendEntriesToNetAgg(i)

   \/ \E m \in {msg \in ValidMessage(messages) : 
        msg.mtype = AppendEntriesNetAggRequest} : NetAggForwardAppendEntriesAll(m)

   \* Regular message handling (for AppendEntries from NetAgg to followers)
   \* Our spec assumes an aggregator NetAgg is available
   \/ \E m \in {msg \in ValidMessage(messages) : 
        msg.mtype \in {AppendEntriesRequest}} : 
        Receive(m)

   \/ \E m \in {msg \in ValidMessage(messages) : 
        msg.mtype = AppendEntriesResponse /\ 
        msg.mdest = netAggIndex} :
        NetAggHandleAppendEntriesResponse(m)
   
   \* Handle AGG_COMMIT
   \/ \E i \in Servers, m \in {msg \in ValidMessage(messages) : 
        msg.mtype = AggCommit} : m.mdest = i /\ HandleAggCommit(i, m)
   
   \* Handle AppendEntriesResponse failing messages that go to leader
   \* to be enabled for point to point recovery todo!
   \/ \E m \in {msg \in ValidMessage(messages) : 
        msg.mtype = AppendEntriesResponse /\ 
        msg.mdest \in Servers /\ state[msg.mdest] = Leader} :
        LET i == m.mdest
            j == m.msource
        IN AppendEntries(i, j)
      
   \* Leader doesn't use AdvanceCommitIndex in HovercRaft++
   \* Commit advancement happens via AGG_COMMIT
   
   \* Server crash and recovery actions for the bonus exercise
   \/ \E i \in Servers : Restart(i)  \* Allow servers to crash and restart
   
   \* Leader election actions (optional for handling leader crashes)
   \/ \E i \in Servers : Timeout(i)  \* Allow followers to timeout and start elections
   \/ \E i \in Servers : BecomeLeader(i)  \* Allow candidates to become leaders
   \/ \E i,j \in Servers : RequestVote(i, j)  \* Allow vote requests
   \/ \E m \in {msg \in ValidMessage(messages) :
        msg.mtype \in {RequestVoteRequest, RequestVoteResponse}} :
        Receive(m)  \* Handle vote messages

   \* Follower request dropping (for testing failure scenarios)
   \/ \E i \in Servers: \E vt \in unorderedRequests[i] : 
        state[i] = Follower /\ FollowerDropRequest(i, vt)

MySwitchPlusPlusSpec == MyInit /\ [][MySwitchPlusPlusNext]_vars
Spec == Init /\ [][MySwitchPlusPlusNext]_vars


\* -------------------- Invariants --------------------

MoreThanOneLeaderInv ==
    \A i,j \in Server :
        (/\ currentTerm[i] = currentTerm[j]
         /\ state[i] = Leader
         /\ state[j] = Leader)
        => i = j

\* Every (index, term) pair determines a log prefix.
\* From page 8 of the Raft paper: "If two logs contain an entry with the 
\*same index and term, then the logs are identical in all preceding entries."
LogMatchingInv ==
    \A i, j \in Server : i /= j =>
        \A n \in 1..min(Len(log[i]), Len(log[j])) :
            log[i][n].term = log[j][n].term =>
            SubSeq(log[i],1,n) = SubSeq(log[j],1,n)

\* The committed entries in every log are a prefix of the
\* leader's log up to the leader's term (since a next Leader may already be
\* elected without the old leader stepping down yet)
LeaderCompletenessInv ==
    \A i \in Server :
        state[i] = Leader =>
        \A j \in Server : i /= j =>
            CheckIsPrefix(CommittedTermPrefix(j, currentTerm[i]),log[i])
            
    
\* Committed log entries should never conflict between servers
LogInv ==
    \A i, j \in Server :
        \/ CheckIsPrefix(Committed(i),Committed(j)) 
        \/ CheckIsPrefix(Committed(j),Committed(i))

\* Note that LogInv checks for safety violations across space
\* This is a key safety invariant and should always be checked
THEOREM MySwitchPlusPlusSpec => ([]LogInv /\ []LeaderCompletenessInv 
                         /\ []LogMatchingInv /\ []MoreThanOneLeaderInv) 

\* fake inv to obtain a trace and observe progress for client requests advancing to committed.
LeaderCommitted ==
    \E i \in Servers : commitIndex[i] /= 2

=============================================================================
