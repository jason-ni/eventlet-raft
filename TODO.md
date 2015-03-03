# Feature TODO list

## Apply committed log

  - In leader node, we should apply log entries immediately after that we check majoritory polls and change committed value. 
  - In each applying, we reply ack to client immediately, before processing next log entry.
  - In follower node, we should apply log entries right after commit index change.
  - (DONE)

## Log persistence

### WAL journal
  - Node can recover from the journal file.
     - WAL journal monotonically increases. The trunking of conflict mem_log should write WAL journal record to mark deletion of trunked log records.
     - Raft role change should write journal to persistent node role and term information. 

## Leader Election 
 
### Make sure the candidate with the latest log elected
  - Need to add check in follower handling on vote request. (DONE)

## Client

### Client register. 
  - Per client command sequence counter should be maintained in STM. (DONE)
  - Reply clients in 2 places:
    1. Before we put client request into the _client_req_queue, reject it because of not leader, or the queue is full.
    2. After applied the command. 
    (DONE)
  - In tick loop, we reply client by put result into the per client ret_queue. (DONE)
  - If leader crushed before reply, new leader node may not have the ret_queue for this client. To solve this, the easiest way is to let client re-send the request. 
  - Command sequence counter mechanism makes sure the applied (by the previous term follower, now the new leader) command is dropped. (DONE)

### Client Session
  - Add a session mechanism to client. The register command creates a client session. The client session has a idle timeout. After timeout, Raft nodes can recycle the resource allocated to this client.

### Client Update Request


### Client Query

  - Linearizability of read.
    - See https://aphyr.com/posts/316-call-me-maybe-etcd-and-consul and https://github.com/coreos/etcd/issues/741 
    - Accumulate read reqs before the leader node make sure it get the qurom, that means to wait for a cycle of heartbeat. 
    - Leader -> Follower change should clear accumulated read reqs. It can return error to client with leader hint.
    - (DONE, without returning error to client when leader switch)

### Client Side Retry
  - Discover leader according to fixed configurations. 
  - Because we maintain the TCP connection after register, we would hit exception if leader crush. Need to handle exception and discover leader again. After finding the new leader, retry request.
  - Also need to set timeout when waiting request reply, as the leader may hang for unknown reason, but without breaking the TCP connection.


### Leader Hint
  - Each member should know the leader id of current term.
  - Return leader hint if the node is not leader.
  - (DONE)


## State Machine

### In Memory State Machine persistence
  - Snapshot/Recovery 

### SQLite as storage backend

## Integration test framework

## In memory log optimization

  - Remove old enought log entry from left end.
  - Add index to log entry position to support more efficient access of mem_log.

## Message packaging efficiency

  - Refactor message fields to use int not string.
