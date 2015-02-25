# -*- coding: utf-8 -*-
from glob import glob
import msgpack
import os
from os import path

from .common import RaftBaseTestCase
from ..log import DiskJournal
from ..errors import DiskJournalFileDoesNotExist
from ..settings import STM_OP_SET


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

    def test_journal_append(self):
        journal_prefix = '~test_file_event_raft_journal_append'
        journal = DiskJournal.create(journal_prefix)
        journal.append_kv(
            TEST_LOG_INDEX, TEST_LOG_TERM, STM_OP_SET, 'type', u'手机'
        )
        journal.close()

    def test_journal_loading(self):
        journal_prefix = '~test_file_event_raft_journal_load'
        journal = DiskJournal.create(journal_prefix)
        journal.append_kv(
            TEST_LOG_INDEX, TEST_LOG_TERM, STM_OP_SET, 'type', u'手机'
        )
        journal.append_kv(
            TEST_LOG_INDEX + 1, TEST_LOG_TERM, STM_OP_SET, 'location', u'上海'
        )
        journal.append_kv(
            TEST_LOG_INDEX + 2, TEST_LOG_TERM, STM_OP_SET, 'name', 'tom'
        )
        journal.close()

        journal = DiskJournal(journal._journal_path)
        self.assertEqual(
            journal._mem_log[0][0], 100
        )
        self.assertEqual(
            msgpack.unpackb(journal._mem_log[0][2])['value'].decode('utf-8'),
            u'手机',
        )
        self.assertEqual(
            msgpack.unpackb(journal._mem_log[1][2])['value'].decode('utf-8'),
            u'上海',
        )
        self.assertEqual(msgpack.unpackb(journal._mem_log[2][2])['key'], 'name')

        journal.close()

    def test_journal_resume(self):
        cwd = os.getcwd()
        journal_prefix = '~test_file_event_raft_journal_resume'
        journal_path_set = set(
            [journal_prefix + '.' + str(x) for x in range(3)]
        )
        journal_file_list = map(
            open,
            journal_path_set,
            ['wb' for x in journal_path_set])
        [x.close() for x in journal_file_list]
        self.assertEqual(
            set(glob('~test_file_event_raft_journal_resume*')),
            journal_path_set,
        )
        latest_journal_path = DiskJournal.get_latest_journal_path(
            journal_prefix,
            cwd,
        )
        self.assertEqual(latest_journal_path.split('.')[-1], '2')

        latest_journal = DiskJournal(latest_journal_path)
        latest_journal.append_kv(
            TEST_LOG_INDEX, TEST_LOG_TERM, STM_OP_SET, 'x', 1
        )
        latest_journal.append_kv(
            TEST_LOG_INDEX + 1, TEST_LOG_TERM, STM_OP_SET, 'x', 2
        )
        latest_journal.close()

        resumed_journal = DiskJournal.resume(journal_prefix, cwd)
        self.assertEqual(resumed_journal._journal_path, latest_journal_path)
        self.assertEqual(resumed_journal._mem_log[-1][0], 101)
