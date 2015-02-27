from glob import glob
import os
from unittest import TestCase
from nose.plugins.attrib import attr


from ..client import RaftClient


class RaftClientTest(TestCase):

    @classmethod
    def tearDownClass(cls):
        map(os.remove, glob('./~test_file*'))

    @attr("integration")
    def test_client_init(self):
        client = RaftClient(('127.0.0.1', 4000))
        client.register()
