from cPickle import dump, load
from glob import glob
import os

from .errors import StateMachineKeyDoesNotExist
from .log import DiskJournal
from .settings import ROLE_MEMBER
from .settings import SNAPSHOT_FILE_NAME_PREFIX
from .settings import JOURNAL_PREFIX


class DictStateMachine(object):

    def __init__(self, snapshot_path):
        self._snapshot_path = snapshot_path
        self._stm = {}

    def set(self, key, value):
        self._stm[key] = value

    def get(self, key):
        try:
            return self._stm[key]
        except KeyError:
            raise StateMachineKeyDoesNotExist(key=key)

    def snapshot(self, **kwargs):
        kwargs['_stm'] = self._stm
        with open(self._snapshot_path, 'wb') as snapshot_file:
            dump(kwargs, snapshot_file)

    def restore_from_snapshot(self):
        with open(self._snapshot_path, 'rb') as snapshot_file:
            snap_data = load(snapshot_file)
            self._stm = snap_data['_stm']
            return snap_data


class StateMachineManager(object):

    def __init__(self,
                 journal_prefix=JOURNAL_PREFIX,
                 snapshot_prefix=SNAPSHOT_FILE_NAME_PREFIX,
                 work_dir=os.getcwd()):
        self.work_dir = work_dir
        self._snapshot_path = os.path.join(
            self.work_dir,
            snapshot_prefix,
        )
        self._journal_prefix = journal_prefix
        self.log_index = 0
        self.commit_index = 0
        self.term = 0
        self._role = ROLE_MEMBER
        self.stm = DictStateMachine(self._snapshot_path)
        self.journal = None

    def resume_from_snapshot(self):
        if os.path.exists(self._snapshot_path):
            snap_data = self.stm.restore_from_snapshot()
            self.term = snap_data['term']
            self._role = snap_data['role']
            self.commit_index = snap_data['commit_index']
            self.log_index = snap_data['log_index']

    def redo_journal_log(self):
        num_of_journal_file = len(
            glob(os.path.join(self.work_dir, self._journal_prefix + '*'))
        )
        if num_of_journal_file > 0:
            self.journal = DiskJournal.resume(
                self._journal_prefix,
                self.work_dir,
            )
        else:
            self.journal = DiskJournal.create(
                self._journal_prefix,
                self.work_dir,
            )

    def resume_or_initialize(self, init=False):
        if init:
            self.journal = DiskJournal.create(self._journal_prefix)
        else:
            self.resume_from_snapshot()
            self.redo_journal_log()

    def set_stm(self, op, key, value):
        self.log_index += 1
        self.journal.append_kv(self.log_index, self.term, op, key, value)
