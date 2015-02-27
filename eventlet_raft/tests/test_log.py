# -*- coding: utf-8 -*-
from glob import glob
import os
from os import path

from .common import RaftBaseTestCase
from ..log import DiskJournal
from ..errors import DiskJournalFileDoesNotExist


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
