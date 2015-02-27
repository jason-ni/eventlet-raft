# Feature TODO list

## Apply committed log

  - In leader node, we should apply log entries immediately after that we check majoritory polls and change committed value. 
  - In each applying, we reply ack to client immediately, before processing next log entry.
  - In follower node, we should apply log entries right after commit index change.

## Log persistence

### WAL journal
  - Node can recover from the journal file.
     - WAL journal is monotonic increasing. The trunking of conflict mem_log should write WAL journal record to mark deletion of trunked log records.
     - Raft role change should write journal to persistent node role and term information. 

## Client

### Client register. 
  - Per client command sequence counter should be maintained in STM. (DONE)
  - Reply clients in 2 places:
    1. Before we put client request into the _client_req_queue, reject it because of not leader, or the queue is full.
    2. After applied the command. 
  - In tick loop, we reply client by put result into the per client ret_queue. 
  - If leader crushed before reply, new leader node may not have the ret_queue for this client. To solve this, the easiest way is to let client re-send the request. 
  - Command sequence counter mechanism makes sure the applied (by the previous term follower, now the new leader) command is dropped.

### Client Query

### Leader Hint
  - Each member should know the leader id of current term.
  - Return leader hint if the node is not leader.

## State Machine

### In Memory State Machine persistence
  - Snapshot/Recovery 

### SQLite as storage backend

## Integration test framework

## In memory log optimization

  - Remove old enought log entry from left end.
  - Add index to log entry position to support more efficient access of mem_log.

