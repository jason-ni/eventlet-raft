from collections import deque
from glob import glob
import itertools
import logging
import msgpack
import os
from os import path
from struct import pack, unpack

from . import settings
from .errors import DiskJournalFileDoesNotExist
from .errors import DiskJournalFileCrushed

LOG = logging.getLogger('Log')


class DiskJournal(object):

    def __init__(self, journal_path):
        if not path.exists(journal_path):
            raise DiskJournalFileDoesNotExist(file_path=journal_path)
        self._journal_path = journal_path
        self._mem_log = deque()
        self._load_journal()

    @classmethod
    def create(cls, journal_prefix, work_dir=os.getcwd()):
        journal_path = path.join(work_dir, journal_prefix + '.0')
        with open(journal_path, 'w+b'):
            pass
        return cls(journal_path)

    @classmethod
    def get_latest_journal_path(cls, journal_prefix, work_dir):
        journal_path_list = glob(path.join(
            work_dir,
            journal_prefix + '*',
        ))
        if len(journal_path_list) == 0:
            return None
        return sorted(
            journal_path_list,
            cmp=lambda x, y: cmp(int(x.split('.')[-1]), int(y.split('.')[-1]))
        )[-1]

    @classmethod
    def resume(cls, journal_prefix, work_dir=None):
        if work_dir is None:
            work_dir = os.getcwd()
        return cls(cls.get_latest_journal_path(
            journal_prefix,
            work_dir,
        ))

    def _load_journal(self):
        self._journal_file = open(self._journal_path, 'r+b')
        entry_size_raw = self._journal_file.read(8)
        if 0 < len(entry_size_raw) < 8:
            raise DiskJournalFileCrushed(
                reason='first entry length not 0 but < 8'
            )
        while entry_size_raw:
            entry_size = unpack('L', entry_size_raw)[0]
            content_raw = self._journal_file.read(entry_size)
            if len(content_raw) != entry_size:
                raise DiskJournalFileCrushed(
                    reason='log entry size does not match'
                )
            log_index, log_term, log_value = unpack(
                '2L{0}s'.format(entry_size - 16),
                content_raw,
            )
            self._mem_log.append((log_index, log_term, log_value))
            entry_size_raw = self._journal_file.read(8)

    def append(self, entry):
        """ param: entry - (index, term, value)"""
        log_index, log_term, log_value = entry
        log_value_len = len(log_value)
        self._journal_file.write(
            pack(
                '3L{0}s'.format(log_value_len),
                log_value_len + 16,
                log_index,
                log_term,
                log_value,
            )
        )
        self._journal_file.flush()

    def close(self):
        self._journal_file.close()


class RaftLog(object):

    def __init__(self, node_id=None, progress=None):
        self.mem_log = deque()
        self.commited = 0
        self.last_applied = 0
        # TODO: need to implement recovery from journal file.
        self.mem_log.append(self._build_init_log_entry())
        self.progress = progress
        self.node_id = node_id

    def _build_init_log_entry(self):
        return dict(
            log_index=0,
            log_term=0,
            log_type=settings.LOG_TYPE_SERVER_CMT,
            cmd=msgpack.packb(dict(op=settings.STM_OP_INT)),
        )

    def build_log_entry(self,
                        log_term,
                        log_type,
                        cmd,
                        log_index=0,
                        client_id=None,
                        seq=None,
                        ):
        return dict(
            log_index=self.last_log_index + 1,
            log_term=log_term,
            log_type=log_type,
            client_id=client_id,
            seq=seq,
            cmd=cmd,
        )

    def append(self, log_entry):
        """ log_entry should be in format of:
            {
                'log_index': <index of this log entry>,
                'log_term': <term of this log entry>,
                'log_type': <type of this log entry>,
                'client_id': <client_id of this log entry>,
                'seq': <commend sequence number>,
                'cmd': <command that will be applied to statemachine>,
            }
            The 'command' field is encoded in msgpack format.
        """
        LOG.info('****** get log entry: %s' % log_entry)
        self._journal_write(log_entry)
        self.mem_log.append(log_entry)
        LOG.info("****** mem log \n%s" % self.mem_log)
        return log_entry

    @property
    def first_log_index(self):
        return self.mem_log[0]['log_index']

    @property
    def last_log_index(self):
        return self.mem_log[-1]['log_index']

    @property
    def last_log_term(self):
        return self.mem_log[-1]['log_term']

    def get_entries_for_replication(self, peer):
        entries_for_replication = []
        LOG.debug("last_log_index %s" % self.last_log_index)
        prev_index = self.last_log_index
        prev_term = self.last_log_term
        if peer.next_idx <= self.last_log_index:
            # Most of the time, the peer.next_idx is near if not equal the
            # mem_log end. So we'd better fetch new log entries from the end.
            tmp_deque = deque()
            for entry in reversed(self.mem_log):
                # We need retain enough log entries in mem_log, so we can get
                # prev_index preceed peer.next_idx. And we can never set
                # peer.next_idx to the first item of mem_log. This should be
                # checked when handling append entries rejection.
                if entry['log_index'] >= peer.next_idx:
                    tmp_deque.appendleft(entry)
                else:
                    prev_index = entry['log_index']
                    prev_term = entry['log_term']
                    break
            entries_for_replication = list(tmp_deque)
        return entries_for_replication, prev_index, prev_term

    def append_entries_to_follower(self, prev_index, prev_term, append_entries):
        prev_matched = False
        for idx, entry in enumerate(reversed(self.mem_log)):
            if entry['log_index'] == prev_index and \
                    entry['log_term'] == prev_term:
                prev_matched = True
                break
        if not prev_matched:
            LOG.debug('has conflict')
            return False

        append_entries_len = len(append_entries)
        if append_entries_len > 0:
            mem_log_len = len(self.mem_log)
            match_start = mem_log_len - idx
            LOG.debug('log match_start: %d' % match_start)

            match_cnt = 0
            for idx, entry in enumerate(
                itertools.islice(self.mem_log, match_start, mem_log_len)
            ):
                if idx == append_entries_len:
                    break
                if entry != append_entries[idx]:
                    break
                match_cnt += 1

            conflict_start = match_start + match_cnt
            if conflict_start < mem_log_len:
                for i in range(mem_log_len - conflict_start):
                    cancelled_log_entry = self.mem_log.pop()
                    LOG.debug(
                        'pop conflict log entry: %s' % str(cancelled_log_entry)
                    )
            if match_cnt < append_entries_len:
                self.mem_log.extend(append_entries[match_cnt:])

            # for debug
            for entry in self.mem_log:
                LOG.info("---- %s" % str(entry))

        return True

    def check_and_update_commits(self, term, majority):
        for entry in reversed(self.mem_log):
            log_index = entry['log_index']
            log_term = entry['log_term']
            if log_term < term or log_index <= self.commited:
                break
            poll = 0
            for peer_id, peer in self.progress.items():
                LOG.debug('---- peer_id %s' % str(peer_id))
                LOG.debug('---- node_id %s' % str(self.node_id))
                if peer_id == self.node_id:
                    poll += 1  # We do not maintain peer info of self node.
                if peer.match_idx >= log_index:
                    poll += 1
            if poll >= majority:
                self.commited = log_index
                break

    def get_commited_entries_for_apply(self):
        ret = []
        for entry in self.mem_log:
            log_index = entry['log_index']
            if log_index > self.last_applied and \
                    log_index <= self.commited:
                ret.append(entry)
        return ret

    def _journal_write(self, log_entry):
        pass
