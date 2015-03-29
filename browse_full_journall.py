import sys
from eventlet_raft.log import DiskJournal, RaftLog
from eventlet_raft import settings


def main():
    raft_log = RaftLog(disk_journal=DiskJournal('{0}_journal'.format(sys.argv[1])))
    for entry in raft_log.disk_journal.browse_journal():
        if entry['log_type'] == settings.LOG_TYPE_COMMIT_CHG:
            print entry


if __name__ == '__main__':
    main()
