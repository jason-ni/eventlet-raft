# -*- coding: utf-8 -*-
import msgpack
import os
from os import path

from .common import RaftBaseTestCase
from .common import fake_populate_members
from .common import FAKE_NODE_IP_PREFIX, FAKE_NODE_PORT
from ..log import DiskJournal
from ..log import RaftLog
from ..stm.state_machine import DictStateMachine
from .. import cmds
from .. import settings


TEST_LOG_INDEX = 100
TEST_LOG_TERM = 1


class DiskJournalTest(RaftBaseTestCase):

    def setUp(self):
        self.journal_prefix = '~test_file_event_raft_journal'

    def test_journal_create(self):
        journal = DiskJournal(self.journal_prefix)
        self.assertTrue(path.exists(journal.journal_path))

    def test_journal_append_and_browse(self):
        journal = DiskJournal(self.journal_prefix)
        write_entries = _build_append_entries(list(range(10)))
        for entry in write_entries:
            journal.append(entry)
        journal.close()

        read_entries = [entry for entry in journal.browse_journal()]
        self.assertEquals(write_entries, read_entries)

    def test_journal_cut(self):
        journal = DiskJournal(self.journal_prefix, cut_limit=256)
        write_entries = _build_append_entries(list(range(20)))
        for idx, entry in enumerate(write_entries):
            journal.append(entry)
            if idx > 0 and idx % 10 == 0:
                journal.flush()
        journal.close()

        destaged_journal_path = "{0}.{1}".format(self.journal_prefix, '0.10')
        self.assertTrue(path.exists(destaged_journal_path))
        file_info = os.stat(destaged_journal_path)
        self.assertTrue(file_info.st_size > 256)

        current_journal_path = "{0}.{1}".format(self.journal_prefix, '11.19')
        self.assertTrue(path.exists(current_journal_path))
        file_info = os.stat(current_journal_path)
        self.assertTrue(file_info.st_size > 0)


def _fake_client_reg_log_entry(raft_log, term):
    log_entry = raft_log.build_log_entry(
        term,
        settings.LOG_TYPE_CLIENT_REG,
        cmds.get_client_register_cmd(),
    )
    log_entry['client_id'] = log_entry['log_index']
    return log_entry


def _get_log_index_array(log_entry_list):
    return [entry['log_index'] for entry in log_entry_list]


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


def _build_follower_raft_log(node_id, progress, index_list, disk_journal):
    raft_log = RaftLog(
        node_id=node_id,
        progress=progress,
        disk_journal=disk_journal,
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
        self.journal_prefix = '~test_file_event_raft_journal'
        self.disk_journal = DiskJournal(self.journal_prefix)

    def test_creation(self):
        raft_log = RaftLog(
            node_id=self.current_node_id,
            progress=self.members,
            disk_journal=self.disk_journal,
        )
        self.assertEquals(len(raft_log.mem_log), 0)

    def test_append_log_entry(self):
        raft_log = RaftLog(
            node_id=self.current_node_id,
            progress=self.members,
            disk_journal=self.disk_journal,
        )
        current_term = 3
        raft_log.append(_fake_client_reg_log_entry(raft_log, current_term))
        self.assertEquals(len(raft_log.mem_log), 1)
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
            self.disk_journal,
        )
        leader_append_entries = _build_append_entries([3, 4, 7, 8, 9])
        success = follower_log.append_entries_to_follower(
            2, 0, leader_append_entries,
        )
        self.assertTrue(success)
        self.assertEquals(
            [entry['log_index'] for entry in follower_log.mem_log],
            [1, 2, 3, 4, 7, 8, 9],
        )
        self.assertEqual(follower_log.last_log_index, 9)

    def test_append_entiries_to_follower_case_2(self):
        """
        - can not find prev match
        """
        follower_log = _build_follower_raft_log(
            self.current_node_id,
            self.members,
            [1, 2],
            self.disk_journal,
        )
        leader_append_entries = _build_append_entries([4, 7, 8])
        success = follower_log.append_entries_to_follower(
            3, 0, leader_append_entries,
        )
        self.assertTrue(success is False)
        self.assertEqual(follower_log.last_log_index, 2)

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
            self.disk_journal,
        )
        leader_append_entries = _build_append_entries([4, 7, 8])
        success = follower_log.append_entries_to_follower(
            3, 0, leader_append_entries,
        )
        self.assertTrue(success)
        self.assertEquals(
            [entry['log_index'] for entry in follower_log.mem_log],
            [1, 2, 3, 4, 7, 8],
        )
        self.assertEqual(follower_log.last_log_index, 8)

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
            self.disk_journal,
        )
        leader_append_entries = _build_append_entries([5])
        success = follower_log.append_entries_to_follower(
            3, 0, leader_append_entries,
        )
        self.assertTrue(success)
        self.assertEquals(
            [entry['log_index'] for entry in follower_log.mem_log],
            [1, 2, 3, 5],
        )
        self.assertEqual(follower_log.last_log_index, 5)

    def test_append_entiries_to_follower_case_5(self):
        """
        - prev log entry matched
        - there are no conflict, just need to trunk
        """
        follower_log = _build_follower_raft_log(
            self.current_node_id,
            self.members,
            [1, 2, 3, 4, 5, 6],
            self.disk_journal,
        )
        leader_append_entries = _build_append_entries([4, 5])
        success = follower_log.append_entries_to_follower(
            3, 0, leader_append_entries,
        )
        self.assertTrue(success)
        self.assertEquals(
            [entry['log_index'] for entry in follower_log.mem_log],
            [1, 2, 3, 4, 5],
        )
        self.assertEqual(follower_log.last_log_index, 5)

    def test_append_entiries_to_follower_case_6(self):
        """
        - prev log entry matched
        - there are no conflict, no need to trunk
        """
        follower_log = _build_follower_raft_log(
            self.current_node_id,
            self.members,
            [1, 2, 3, 4, 5, 6],
            self.disk_journal,
        )
        leader_append_entries = _build_append_entries([4, 5, 6])
        success = follower_log.append_entries_to_follower(
            3, 0, leader_append_entries,
        )
        self.assertTrue(success)
        self.assertEquals(
            [entry['log_index'] for entry in follower_log.mem_log],
            [1, 2, 3, 4, 5, 6],
        )
        self.assertEqual(follower_log.last_log_index, 6)

    def test_recover_or_init(self):
        raft_log = RaftLog(
            node_id=self.current_node_id,
            progress=self.members,
            disk_journal=self.disk_journal,
        )
        stm = DictStateMachine()
        raft_log.recover_or_init(stm)
        self.assertEqual(len(raft_log.mem_log), 1)
        self.assertTrue(path.exists(raft_log.disk_journal.journal_path))
        raft_log.disk_journal.flush()
        self.assertTrue(
            os.stat(raft_log.disk_journal.journal_path).st_size > 0
        )

        raft_log.append(_fake_client_reg_log_entry(raft_log, 1))
        raft_log.append(_fake_client_update_log_entry(
            raft_log,
            1,
            1,
            1,
            'index',
            2,
        ))
        raft_log.append(_fake_client_update_log_entry(
            raft_log,
            1,
            1,
            2,
            'index',
            3,
        ))
        raft_log.append(raft_log._build_cancel_log_entry(3, 1))
        raft_log.write_commit_log(2, 1)
        raft_log.disk_journal.close()

        raft_log = RaftLog(
            node_id=self.current_node_id,
            progress=self.members,
            disk_journal=self.disk_journal,
        )
        stm = DictStateMachine()
        raft_log.recover_or_init(stm)
        self.assertEqual(len(raft_log.mem_log), 3)
        self.assertEqual(raft_log.last_log_index, 2)
        self.assertEqual(raft_log.last_log_term, 1)
        self.assertEqual(stm.get('index'), 2)
        self.assertEqual(stm._client_seq[1], 1)


class LeaderAndFollowerRaftLogTest(RaftBaseTestCase):

    current_term = 3

    def _gen_raft_log(self, ip_sub_no):
        members = fake_populate_members(member_count=5)
        node_id = (FAKE_NODE_IP_PREFIX + str(ip_sub_no), FAKE_NODE_PORT)
        journal_prefix = '~test_file_event_raft_journal_node_{0}'.format(
            ip_sub_no,
        )
        return RaftLog(
            node_id=node_id,
            progress=members,
            disk_journal=DiskJournal(journal_prefix)
        )

    def _prepare_leader_raft_log(self):
        leader_raft_log = self._gen_raft_log(0)
        leader_raft_log.append(
            _fake_server_commit_log_entry(
                leader_raft_log,
                leader_raft_log.node_id,
                self.current_term,
            )
        )
        entry = leader_raft_log.append(
            _fake_client_reg_log_entry(leader_raft_log, self.current_term)
        )
        client_id = entry['log_index']
        for i in range(5):
            leader_raft_log.append(
                _fake_client_update_log_entry(
                    leader_raft_log,
                    self.current_term,
                    client_id,
                    i,
                    'counter',
                    i
                )
            )
        return leader_raft_log

    def test_replicate_log_to_follower(self):
        leader_raft_log = self._prepare_leader_raft_log()
        follower_raft_log = self._gen_raft_log(1)

        peer = leader_raft_log.progress[follower_raft_log.node_id]
        peer.next_idx = leader_raft_log.last_log_index - 4

        entries_for_replication, prev_index, prev_term = \
            leader_raft_log.get_entries_for_replication(peer)
        self.assertEquals(
            _get_log_index_array(entries_for_replication),
            [3, 4, 5, 6, 7],
        )
        self.assertEqual(prev_index, 2)
        self.assertEqual(prev_term, 3)

        follower_raft_log.mem_log.extend(
            [x for x in leader_raft_log.mem_log][:2]
        )
        follower_raft_log.last_log_index = \
            follower_raft_log.mem_log[-1]['log_index']
        follower_raft_log.last_log_term = \
            follower_raft_log.mem_log[-1]['log_term']
        success = follower_raft_log.append_entries_to_follower(
            prev_index,
            prev_term,
            entries_for_replication,
        )
        self.assertTrue(success)
        self.assertEquals(
            follower_raft_log.mem_log,
            leader_raft_log.mem_log,
        )

    def test_check_and_update_commit_then_get_entries_for_apply(self):
        leader_raft_log = self._prepare_leader_raft_log()
        follower_1_raft_log = self._gen_raft_log(1)
        follower_2_raft_log = self._gen_raft_log(2)
        self.assertEqual(leader_raft_log.commited, 0)
        follower_1_peer = leader_raft_log.progress[follower_1_raft_log.node_id]
        follower_2_peer = leader_raft_log.progress[follower_2_raft_log.node_id]
        follower_1_peer.match_idx = 3
        follower_2_peer.match_idx = 4
        majority = len(leader_raft_log.progress) / 2 + 1
        leader_raft_log.check_and_update_commits(3, majority)
        self.assertEqual(leader_raft_log.commited, 3)

        leader_raft_log.last_applied = 2
        entries_for_apply = leader_raft_log.get_commited_entries_for_apply()
        self.assertEquals(
            _get_log_index_array(entries_for_apply),
            [3, ],
        )

    def test_destage_fully_replicated_log(self):
        leader_raft_log = self._prepare_leader_raft_log()
        follower_peers = [leader_raft_log.progress[x]
                          for x in sorted(leader_raft_log.progress.keys())]
        for peer in follower_peers:
            peer.match_idx = 4

        follower_peers[-1].match_idx = 2

        majority = len(leader_raft_log.progress) / 2 + 1
        leader_raft_log.check_and_update_commits(3, majority)
        leader_raft_log.last_applied = 3

        leader_raft_log.destage_fully_replicated_log()
        self.assertEquals(
            _get_log_index_array(leader_raft_log.mem_log),
            [2, 3, 4, 5, 6, 7],
        )
        leader_raft_log.destage_fully_replicated_log()
        self.assertEquals(
            _get_log_index_array(leader_raft_log.mem_log),
            [2, 3, 4, 5, 6, 7],
        )
        follower_peers[-1].match_idx = 4
        leader_raft_log.last_applied = 4
        leader_raft_log.destage_fully_replicated_log()
        self.assertEquals(
            _get_log_index_array(leader_raft_log.mem_log),
            [4, 5, 6, 7],
        )
