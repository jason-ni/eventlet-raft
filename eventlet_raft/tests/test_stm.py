# -*- coding: utf-8 -*-
from .common import RaftBaseTestCase
from ..errors import StateMachineKeyDoesNotExist
from ..settings import ROLE_MEMBER
from ..state_machine import DictStateMachine, StateMachineManager


class DictStateMachinelTest(RaftBaseTestCase):

    def test_stm_set_and_get(self):
        stm = DictStateMachine('~test_file_snapshot')
        stm.set('x', 4)
        self.assertEqual(stm.get('x'), 4)

    def test_stm_snapshot_and_restore(self):
        stm = DictStateMachine('~test_file_snapshot')
        stm.set('x', 4)
        stm.set('x', 5)
        stm.set('y', 6)
        self.assertEqual(stm.get('x'), 5)
        self.assertEqual(stm.get('y'), 6)
        stm.snapshot(term=1, log_index=100)
        stm.set('z', 7)
        snap_data = stm.restore_from_snapshot()
        self.assertEqual(snap_data['term'], 1)
        self.assertEqual(snap_data['log_index'], 100)
        self.assertRaises(StateMachineKeyDoesNotExist, stm.get, 'z')
        self.assertEqual(stm.get('x'), 5)
        self.assertEqual(stm.get('y'), 6)


class StateMachineManagerTest(RaftBaseTestCase):

    def test_stm_manager_init(self):
        journal_prefix = '~test_file_event_raft_journal'
        snapshot_prefix = '~test_file_event_raft_snapshot'
        stm_manager = StateMachineManager(
            journal_prefix=journal_prefix,
            snapshot_prefix=snapshot_prefix,
        )
        self.assertEqual(stm_manager.log_index, 0)
        self.assertEqual(stm_manager.commit_index, 0)
        self.assertEqual(stm_manager.term, 0)
        self.assertEqual(stm_manager._role, ROLE_MEMBER)
        self.assertEqual(stm_manager.journal, None)
