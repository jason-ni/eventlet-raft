from unittest import TestCase

from ..node import Peer


class PeerTest(TestCase):

    def test_peer_init(self):
        peer = Peer('127.0.0.1', 6000)
        self.assertEqual(peer.id, ('127.0.0.1', 6000))
        self.assertEqual(peer.alive, True)
        self.assertEqual(str(peer), "('127.0.0.1', 6000)")
