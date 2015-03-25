from collections import deque
from glob import glob
import itertools
import logging
import msgpack
import os
from os import path

from . import settings
# from .errors import DiskJournalFileCrushed

LOG = logging.getLogger('Log')


class DiskJournal(object):

    def __init__(self,
                 journal_prefix,
                 work_dir=os.getcwd(),
                 cut_limit=settings.JOURNAL_CUT_LIMIT,
                 ):
        self.journal_prefix = journal_prefix
        self.work_dir = work_dir

        self.journal_path = self.get_or_create_latest_journal_path()
        self.journal_file = None
        self.last_log_index = None
        self.cut_limit = cut_limit
        self._need_flush = False

    def get_or_create_latest_journal_path(self):
        journal_path_list = self.get_journal_path_list()
        if journal_path_list:
            return journal_path_list[-1]
        else:
            journal_path = path.join(self.work_dir, self.journal_prefix + '.0')
            with open(journal_path, 'w+b'):
                pass
            return journal_path

    def get_journal_path_list(self):
        journal_path_list = glob(path.join(
            self.work_dir,
            self.journal_prefix + '*',
        ))
        return sorted(
            journal_path_list,
            cmp=lambda x, y: cmp(int(x.split('.')[-1]), int(y.split('.')[-1]))
        )

    def is_blank(self):
        journal_path_list = self.get_journal_path_list()
        if journal_path_list:
            if os.stat(journal_path_list[-1]).st_size == 0:
                return True
            else:
                return False
        else:
            return True

    def browse_journal(self):
        for journal_path in self.get_journal_path_list():
            with open(journal_path, 'r+b') as journal_file:
                unpacker = msgpack.Unpacker()
                while True:
                    chunk = journal_file.read(settings.BUF_LEN)
                    if len(chunk) == 0:
                        break
                    unpacker.feed(chunk)
                    for element in unpacker:
                        yield element

    def append(self, entry):
        """ param: entry - dict type log entry"""
        if self.journal_file is None:
            self.journal_file = open(self.journal_path, 'a+b')
        self.journal_file.write(msgpack.packb(entry))
        self._need_flush = True
        self.last_log_index = entry['log_index']

    def flush(self):
        if self._need_flush and self.journal_file:
            self.journal_file.flush()
            self._need_flush = False
            if os.stat(self.journal_path).st_size > self.cut_limit:
                self.cut()

    def cut(self):
        if self.journal_file is None:
            return
        old_journal_path = self.journal_path
        destaged_journal_path = "{0}.{1}".format(
            self.journal_path, self.last_log_index
        )
        self.journal_file.close()
        self.journal_path = path.join(
            self.work_dir,
            "{0}.{1}".format(self.journal_prefix, self.last_log_index + 1),
        )
        self.journal_file = open(self.journal_path, 'a+b')
        os.rename(old_journal_path, destaged_journal_path)

    def close(self):
        if self.journal_file:
            self.flush()
            self.journal_file.close()
            self.journal_file = None


class RaftLog(object):

    def __init__(self, node_id=None, progress=None, disk_journal=None):
        self.mem_log = deque()
        self.commited = 0
        self.last_applied = 0
        self.last_log_index = 0
        self.last_log_term = 0
        self.progress = progress
        self.node_id = node_id
        self.disk_journal = disk_journal

    def recover_or_init(self, stm):
        if self.disk_journal.is_blank():
            self.append(self._build_init_log_entry())
        else:
            self.populate_mem_log_and_apply_stm(stm)

    def populate_mem_log_and_apply_stm(self, stm):
        for entry in self.disk_journal.browse_journal():
            log_type = entry['log_type']
            if log_type == settings.LOG_TYPE_CLIENT_REG or \
                    log_type == settings.LOG_TYPE_CLIENT_REQ or \
                    log_type == settings.LOG_TYPE_SERVER_CMT:
                self.mem_log.append(entry)
                self.last_log_term = entry['log_term']
                self.last_log_index = entry['log_index']
            elif log_type == settings.LOG_TYPE_COMMIT_CHG:
                self.commited = entry['log_index']
                self._apply_recovered_cmd_to_stm(stm)
            elif log_type == settings.LOG_TYPE_CANCEL_IDX:
                self.mem_log.pop()
                self.last_log_index = self.mem_log[-1]['log_index']
                self.last_log_term = self.mem_log[-1]['log_term']

    def _apply_recovered_cmd_to_stm(self, stm):
        for entry in self.mem_log:
            log_index = entry['log_index']
            if log_index > self.last_applied and \
                    log_index <= self.commited:
                log_type = entry['log_type']
                if log_type == settings.LOG_TYPE_CLIENT_REG or \
                        log_type == settings.LOG_TYPE_CLIENT_REQ:
                    stm.apply_cmd(entry)
                    self.last_applied = log_index

    @property
    def first_log_index(self):
        return self.mem_log[0]['log_index']

    def _build_init_log_entry(self):
        return dict(
            log_index=0,
            log_term=0,
            log_type=settings.LOG_TYPE_SERVER_CMT,
            cmd=msgpack.packb(dict(op=settings.STM_OP_INT)),
        )

    def _build_cancel_log_entry(self, log_index, log_term):
        return dict(
            log_index=log_index,
            log_term=log_term,
            log_type=settings.LOG_TYPE_CANCEL_IDX,
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
        self.disk_journal.append(log_entry)
        self.mem_log.append(log_entry)
        self.last_log_index = log_entry['log_index']
        self.last_log_term = log_entry['log_term']
        LOG.info("****** mem log \n%s" % self.mem_log)
        return log_entry

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
                    self.disk_journal.append(
                        self._build_cancel_log_entry(
                            cancelled_log_entry['log_index'],
                            cancelled_log_entry['log_term'],
                        )
                    )
                    LOG.debug(
                        'pop conflict log entry: %s' % str(cancelled_log_entry)
                    )
            if match_cnt < append_entries_len:
                for entry in append_entries[match_cnt:]:
                    self.disk_journal.append(entry)
                self.mem_log.extend(append_entries[match_cnt:])
            if match_cnt <= append_entries_len:
                self.last_log_index = append_entries[-1]['log_index']
                self.last_log_term = append_entries[-1]['log_term']

        return True

    def write_commit_log(self, commit_to_log_index, term):
        self.disk_journal.append(dict(
            log_index=commit_to_log_index,
            log_term=term,
            log_type=settings.LOG_TYPE_COMMIT_CHG,
        ))

    def check_and_update_commits(self, term, majority):
        old_commited = self.commited
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
        if old_commited < self.commited:
            self.write_commit_log(self.commited, term)

    def get_commited_entries_for_apply(self):
        ret = []
        for entry in self.mem_log:
            log_index = entry['log_index']
            if log_index > self.last_applied and \
                    log_index <= self.commited:
                ret.append(entry)
        return ret
