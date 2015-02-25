from glob import glob
import os
from unittest import TestCase


class RaftBaseTestCase(TestCase):

    @classmethod
    def tearDownClass(cls):
        map(os.remove, glob('./~test_file*'))
