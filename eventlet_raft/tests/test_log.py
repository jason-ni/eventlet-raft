# -*- coding: utf-8 -*-
from glob import glob
import msgpack
import os
from os import path

from .common import RaftBaseTestCase
from .common import fake_populate_members
from .common import FAKE_NODE_IP_PREFIX, FAKE_NODE_PORT
from ..log import DiskJournal
from ..log import RaftLog
from ..errors import DiskJournalFileDoesNotExist
from .. import cmds
from .. import settings


TEST_LOG_INDEX = 100
TEST_LOG_TERM = 1


class DiskJournalTest(RaftBaseTestCase):

    @classmethod
    def tearDownClass(cls):
        map(os.remove, glob('./~test_file*'))

    def test_file_path_check(self):
        self.assertRaises(DiskJournalFileDoesNotExist, DiskJournal, 'not_exist')

    def test_journal_create(self):
        journal_prefix = '~test_file_event_raft_journal'
        journal = DiskJournal.create(journal_prefix)
        self.assertTrue(path.exists(journal._journal_path))
        os.remove(journal._journal_path)


def _fake_client_reg_log_entry(raft_log, term):
    return raft_log.build_log_entry(
        term,
        settings.LOG_TYPE_CLIENT_REG,
        cmds.get_client_register_cmd(),
    )


def _fake_server_commit_log_entry(raft_log,
                                  node_id,
                                  term,
                                  ):
    return raft_log.build_log_entry(
        term,
        settings.LOG_TYPE_SERVER_CMT,
        msgpack.packb(dict(op=settings.STM_OP_INT)),
        client_id=node_id,
    )


def _fake_client_update_log_entry(raft_log,
                                  term,
                                  client_id,
                                  seq,
                                  key,
                                  value
                                  ):
    return raft_log.build_log_entry(
        term,
        settings.LOG_TYPE_CLIENT_REQ,
        cmds.get_client_update_cmd(key, value),
        client_id=client_id,
        seq=seq,
    )


def _build_follower_raft_log(node_id, progress, index_list):
    raft_log = RaftLog(
        node_id=node_id,
        progress=progress,
    )
    for index in index_list:
        raft_log.append(_fake_client_update_log_entry(
            raft_log,
            0,
            0,
            index,
            'index',
            index,
        ))
    return raft_log


def _build_append_entries(index_list):
    return [
        dict(
            log_index=x,
            log_term=0,
            log_type=settings.LOG_TYPE_CLIENT_REQ,
            client_id=0,
            seq=x,
            cmd=cmds.get_client_update_cmd('index', x),
        )
        for x in index_list
    ]


class RaftLogTest(RaftBaseTestCase):

    def setUp(self):
        self.members = fake_populate_members()
        self.current_node_id = (FAKE_NODE_IP_PREFIX + str(0), FAKE_NODE_PORT)

    def test_creation(self):
        raft_log = RaftLog(
            node_id=self.current_node_id,
            progress=self.members,
        )
        self.assertEquals(len(raft_log.mem_log), 1)

    def test_append_log_entry(self):
        raft_log = RaftLog(
            node_id=self.current_node_id,
            progress=self.members,
        )
        current_term = 3
        raft_log.append(_fake_client_reg_log_entry(raft_log, current_term))
        self.assertEquals(len(raft_log.mem_log), 2)
        new_term = 4
        raft_log.append(
            _fake_server_commit_log_entry(
                raft_log,
                self.current_node_id,
                new_term,
            )
        )
        self.assertEqual(raft_log.last_log_term, new_term)
        self.assertEqual(raft_log.last_log_index, 2)

    def test_get_entries_for_replication(self):
        raft_log = RaftLog(
            node_id=self.current_node_id,
            progress=self.members,
        )
        current_term = 3
        raft_log.append(
            _fake_server_commit_log_entry(
                raft_log,
                self.current_node_id,
                current_term,
            )
        )
        entry = raft_log.append(
            _fake_client_reg_log_entry(raft_log, current_term)
        )
        client_id = entry['log_index']
        for i in range(5):
            raft_log.append(
                _fake_client_update_log_entry(
                    raft_log, current_term, client_id, i, 'counter', i
                )
            )
        peer = self.members.values()[-1]
        peer.next_idx = raft_log.last_log_index - 4
        entries_for_replication, prev_index, prev_term = \
            raft_log.get_entries_for_replication(peer)
        self.assertEqual(prev_index, 2)
        self.assertEqual(prev_term, 3)
        self.assertEqual(len(entries_for_replication), 5)

    def test_append_entiries_to_follower_case_1(self):
        """
        - prev log entry matched
        - there are conflict entries
        - there are newly appended entries
        """
        follower_log = _build_follower_raft_log(
            self.current_node_id,
            self.members,
            [1, 2, 3, 4, 5, 6],
        )
        leader_append_entries = _build_append_entries([3, 4, 7, 8, 9])
        success, last_index = follower_log.append_entries_to_follower(
            2, 0, leader_append_entries,
        )
        self.assertTrue(success)
        self.assertEquals(
            [entry['log_index'] for entry in follower_log.mem_log],
            [0, 1, 2, 3, 4, 7, 8, 9],
        )

    def test_append_entiries_to_follower_case_2(self):
        """
        - can not find prev match
        """
        follower_log = _build_follower_raft_log(
            self.current_node_id,
            self.members,
            [1, 2],
        )
        leader_append_entries = _build_append_entries([4, 7, 8])
        success, last_index = follower_log.append_entries_to_follower(
            3, 0, leader_append_entries,
        )
        self.assertTrue(success is False)
        self.assertEqual(last_index, -1)

    def test_append_entiries_to_follower_case_3(self):
        """
        - prev log entry matched
        - there are no conflict, but have common elements
        - there are new append entries
        """
        follower_log = _build_follower_raft_log(
            self.current_node_id,
            self.members,
            [1, 2, 3],
        )
        leader_append_entries = _build_append_entries([4, 7, 8])
        success, last_index = follower_log.append_entries_to_follower(
            3, 0, leader_append_entries,
        )
        self.assertTrue(success)
        self.assertEquals(
            [entry['log_index'] for entry in follower_log.mem_log],
            [0, 1, 2, 3, 4, 7, 8],
        )

    def test_append_entiries_to_follower_case_4(self):
        """
        - prev log entry matched
        - there are no conflict, and have no common element
        - there are new append entries
        """
        follower_log = _build_follower_raft_log(
            self.current_node_id,
            self.members,
            [1, 2, 3],
        )
        leader_append_entries = _build_append_entries([5])
        success, last_index = follower_log.append_entries_to_follower(
            3, 0, leader_append_entries,
        )
        self.assertTrue(success)
        self.assertEquals(
            [entry['log_index'] for entry in follower_log.mem_log],
            [0, 1, 2, 3, 5],
        )

    def test_append_entiries_to_follower_case_5(self):
        """
        - prev log entry matched
        - there are no conflict, just need to trunk
        """
        follower_log = _build_follower_raft_log(
            self.current_node_id,
            self.members,
            [1, 2, 3, 4, 5, 6],
        )
        leader_append_entries = _build_append_entries([4, 5])
        success, last_index = follower_log.append_entries_to_follower(
            3, 0, leader_append_entries,
        )
        self.assertTrue(success)
        self.assertEquals(
            [entry['log_index'] for entry in follower_log.mem_log],
            [0, 1, 2, 3, 4, 5],
        )

    def test_append_entiries_to_follower_case_6(self):
        """
        - prev log entry matched
        - there are no conflict, no need to trunk
        """
        follower_log = _build_follower_raft_log(
            self.current_node_id,
            self.members,
            [1, 2, 3, 4, 5, 6],
        )
        leader_append_entries = _build_append_entries([4, 5, 6])
        success, last_index = follower_log.append_entries_to_follower(
            3, 0, leader_append_entries,
        )
        self.assertTrue(success)
        self.assertEquals(
            [entry['log_index'] for entry in follower_log.mem_log],
            [0, 1, 2, 3, 4, 5, 6],
        )
