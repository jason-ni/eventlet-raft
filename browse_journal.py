import sys
from eventlet_raft.log import DiskJournal, RaftLog
from eventlet_raft.stm.state_machine import DictStateMachine
from eventlet_raft.node import Peer


def main():
    peer = Peer(ip_address='192.168.0.1', port=5000)
    members = {peer.id: peer}
    raft_log = RaftLog(
        progress=members,
        disk_journal=DiskJournal('{0}_journal'.format(sys.argv[1])),
    )
    raft_log.populate_mem_log_and_apply_stm(DictStateMachine())
    for entry in raft_log.mem_log:
        print entry['log_index'], entry['log_term'], entry['log_type'], entry['cmd']

    print '---', raft_log.last_log_index, raft_log.last_log_term, raft_log.commited
    print '===', raft_log.destaged_index, raft_log.destaged_term

if __name__ == '__main__':
    main()
