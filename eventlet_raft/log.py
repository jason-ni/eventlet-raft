from collections import deque
from glob import glob
import msgpack
import os
from os import path
from struct import pack, unpack

from .errors import DiskJournalFileDoesNotExist
from .errors import DiskJournalFileCrushed


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

    def append_kv(self, index, term, op, key, value):
        self.append((
            index,
            term,
            msgpack.packb({'key': key, 'value': value, 'op': op}),
        ))

    def close(self):
        self._journal_file.close()


class RaftLog(object):

    def __init__(self):
        self.mem_log = deque()
        self.last_log_index = 0
        self.last_term = 0
        self.commited = 0
        self.last_applied = 0

    def append(self, log_entry):
        """ log_entry should be in format of:
            {
                'log_index': <index of this log entry>,
                'log_term': <term of this log entry>,
                'log_type': <type of this log entry>,
                'command': <command that will be applied to statemachine>,
            }
            The 'command' field is encoded in msgpack format.
        """
        self._journal_write(log_entry)
        self.mem_log.append(log_entry)
        if log_entry['log_type'] == 'stm':
            self.last_log_index = log_entry['log_index']
            self.last_term = log_entry['term']
        if log_entry['log_type'] == 'commit':
            self.commited = log_entry['log_index']
        if log_entry['log_type'] == 'apply':
            self.last_applied = log_entry['log_index']

    def _journal_write(self, log_entry):
        pass
