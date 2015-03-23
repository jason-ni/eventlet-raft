# Implementation Design 

## Raft log implementation

  - Im eventlet-raft, the raft log has two parts. 
    1. One is the in-memory log, which is implemented by a list object. 
    2. Another is the persistency of in-memory log, WAL journal. 

## WAL journal

  - The only goal of WAL journal is to allow raft node recover from cold restart or crush.
  - The WAL does not only record log entries of state-machine key/value update. It also record log entries of term initialization and commit index change. So we can save the effort to persist raft state like current term, vote and last commited index.

### Rules of writing WAL journal  

  - As the state-machine is in-memory, we need to persist log entries from last snapshot to current state.
  - Followers write WAL when they receive AppendEntries message, and before commit log entries.
  - The change of commited index should be written to WAL.
  - When merging AppendEntries, we may need to delete some conflict log entries at the end. In order to avoid trunk file, we write CANCEL WAL record. In recovery stage, CANCEL WAL record can indicate that some previous log entries should be removed from in-memory log.
  - Wrinting WAL failure should cause raft node stop running.

