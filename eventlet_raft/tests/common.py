from glob import glob
import os
from unittest import TestCase

from ..node import Peer


class RaftBaseTestCase(TestCase):

    def tearDown(self):
        map(os.remove, glob('./~test_file*'))


FAKE_NODE_IP_PREFIX = '192.168.0.'
FAKE_NODE_PORT = 5000


def fake_populate_members(member_count=3):
    members = dict()
    for mem_no in range(member_count):
        ip, port = (FAKE_NODE_IP_PREFIX + str(mem_no), FAKE_NODE_PORT)
        peer = Peer(ip, port)
        members[peer.id] = peer

    return members
