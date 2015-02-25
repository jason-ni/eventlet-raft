import logging
import time
import eventlet
import msgpack
import random

from .server import Server
from .msg import get_vote_msg
from .msg import get_vote_return_msg
from .msg import get_append_entry_msg

LOG = logging.getLogger('Node')
from .settings import VOTE_CYCLE, PING_CYCLE, ELECTION_TIMEOUT, TICK_CYCLE


class Peer(object):

    def __init__(self,
                 ip_address,
                 port,
                 next_idx=1,
                 match_idx=0,
                 alive=True,
                 sock=None,
                 ):
        self.ip_address = ip_address
        self.port = port
        self.id = (ip_address, port)
        self.next_idx = next_idx
        self.match_idx = match_idx
        self.alive = alive
        self.sock = sock
        self._msg_queue = eventlet.queue.Queue()

    def __str__(self):
        return str(self.id)


class Node(Server):

    def __init__(self, conf):
        super(Node, self).__init__(conf)

        self.id = (self._node_listen_ip, self._node_listen_port)
        self._clients = {}
        self._members = {}
        self._populate_members(conf)
        self._last_timestamp = time.time()
        self._is_leader = False
        self._is_follower = True
        self._is_candidate = False
        self._term = 0
        self._voted = 0
        self._vote_for = None
        self._ping_counter = 0

    def _populate_members(self, conf):
        for peer_section in conf.get('server', 'peers').split(','):
            peer_section_name = peer_section.strip()
            peer_ip = conf.get(peer_section_name, 'peer_ip')
            peer_port = int(conf.get(peer_section_name, 'peer_port'))
            peer = Peer(peer_ip, peer_port)
            self._members[peer.id] = peer
            if peer.id != self.id:
                peer_sock = self._try_connect_to_peer((peer_ip, peer_port))
                if peer_sock is not None:
                    peer.sock = peer_sock
                else:
                    peer.alive = False

        self.num_members = len(self._members)

    def _try_connect_to_peer(self, address):
        LOG.debug("Trying to establish connection to %s" % str(address))
        try:
            sock = eventlet.connect(address)
            return sock
        except Exception as e:
            LOG.debug("Can not establish connection to %s" % str(address))
            LOG.debug("Error msg: %s" % str(e))
        return None

    def _on_start(self):
        self._threads.append(
            eventlet.spawn(self._try_to_vote)
        )
        self._threads.append(
            eventlet.spawn(self._tick_loop)
        )

    def _tick_loop(self):
        last_ping_timestamp = time.time()
        while True:
            if self._is_follower:
                pass

            current_timestamp = time.time()
            if current_timestamp - last_ping_timestamp > PING_CYCLE:
                last_ping_timestamp = current_timestamp
                if self._is_leader:
                    self._broadcast_append_entry_msg()
                    continue

            eventlet.sleep(TICK_CYCLE)

    def _broadcast_append_entry_msg(self):
        LOG.debug('trying to send append entry')
        msg = get_append_entry_msg(
            self.id,
            self._term,
            0,
            0,
            self._ping_counter,
            None)
        self._ping_counter += 1
        self._broadcast_msg(msg)

    def msg_append_entry(self, msg):
        LOG.debug("++++ %s vs %s" % (self._term, msg['term']))
        LOG.debug("=== self._is_follower %s" % self._is_follower)
        LOG.debug("=== self._is_leader %s" % self._is_leader)
        LOG.debug("=== self._is_candidate %s" % self._is_candidate)
        if self._term > msg['term']:
            return
        # reset follower's timer
        self._last_timestamp = time.time()
        LOG.debug('follower timer is reset')

        if self._is_candidate:
            self._become_follower()
        # TODO: process entry

    def _handle_election_timeout(self):
        # sleep random time
        sleep_time = random.choice([
            ELECTION_TIMEOUT,
            ELECTION_TIMEOUT * 2,
            ELECTION_TIMEOUT * 3,
            ELECTION_TIMEOUT * 4
        ])
        eventlet.sleep(sleep_time)
        if self._is_candidate:
            self._vote()

    def _try_to_vote(self):
        while True:
            eventlet.sleep(VOTE_CYCLE)
            current_timestamp = time.time()
            LOG.debug(
                "---- trying to vote, is follower: %s" % self._is_follower
            )
            LOG.debug(
                "---- trying to vote, is candidate: %s" % self._is_candidate
            )
            LOG.debug("%s" % self._members)
            if self._is_follower:
                LOG.debug('trying to vote, timestamp: %s' % current_timestamp)
                LOG.debug('current_timestamp %s' % current_timestamp)
                LOG.debug('last_timestamp %s' % self._last_timestamp)
                if (current_timestamp - self._last_timestamp) > VOTE_CYCLE:
                    self._is_follower = False
                    self._is_candidate = True
                    self._vote()

    def _vote(self):
        LOG.debug("************* voting")
        LOG.debug("is follower %s" % self._is_follower)
        LOG.debug("is candidate %s" % self._is_candidate)
        LOG.debug("is leader %s" % self._is_leader)
        self._term += 1
        LOG.debug('generate vote msg and broadcast.')
        vote_msg = get_vote_msg(self.id, self._term)
        self._voted = 1
        self._vote_for = self.id
        eventlet.spawn(self._handle_election_timeout)
        self._broadcast_msg(vote_msg)
        if self._is_candidate and self._voted >= self.majority:
            self._become_leader()

    def _broadcast_msg(self, msg):
        for peer_id, peer in self._members.items():
            LOG.debug('sending msg to %s' % str(peer_id))
            self._try_connect_and_send_msg(peer_id, msg)

    def _on_handle_node_msg(self, msg):
        msg_name = msg['name']
        if self._term < msg['term']:
            self._term = msg['term']
            self._become_follower()
        method = getattr(self, 'msg_%s' % msg_name)
        method(msg)

    def _become_follower(self):
        self._is_follower = True
        self._is_leader = False
        self._is_candidate = False
        self._voted = 0
        self._vote_for = None
        self._last_timestamp = time.time()

    def msg_vote(self, msg):
        LOG.debug('Get vote message %s' % msg)
        LOG.debug('self._term: %s' % self._term)
        LOG.debug('msg term: %s' % msg['term'])
        node_id = tuple(msg['node_id'])
        if self._term > msg['term']:
            self._return_vote_msg(
                node_id, False)
        LOG.debug('node_id %s' % str(node_id))
        LOG.debug('self.id %s' % str(self.id))
        if self._is_follower and (self._vote_for is None):
            self._return_vote_msg(
                node_id, True)

    def _return_vote_msg(self, peer_id, accept):
        LOG.debug('Return vote msg.')
        msg = get_vote_return_msg(self.id, self._term, accept)
        self._try_connect_and_send_msg(peer_id, msg)

    def msg_vote_return(self, msg):
        LOG.debug('Get vote return message %s' % msg)
        if self._term > msg['term']:
            return
        if self._is_candidate:
            if msg['accept']:
                self._voted += 1
            LOG.debug('Get voted %s' % self._voted)
            if self._voted >= self.majority:
                self._become_leader()

    def _become_leader(self):
        if self._is_candidate:
            self._is_candidate = False
            self._is_leader = True
            self._is_follower = False
            self._last_timestamp = time.time()
            LOG.info('Node %s become leader.' % str(self.id))

    @property
    def num_live_members(self):
        return len([x for _, x in self._members.items() if x.alive])

    @property
    def majority(self):
        return self.num_members / 2 + 1

    def _try_connect_and_send_msg(self, peer_id, msg):
        LOG.debug('Try connect to %s and send msg' % str(peer_id))
        if peer_id == self.id:
            msg = msgpack.unpackb(msg)
            self._on_handle_node_msg(msg)
        else:
            try:
                self._send_msg_through_sock(
                    self._members[peer_id].sock,
                    msg
                )
            except Exception as e:
                # Try to re-establish connection
                peer_sock = self._try_connect_to_peer(peer_id)
                alive = False
                if peer_sock:
                    alive = True
                    try:
                        self._send_msg_through_sock(
                            peer_sock,
                            msg
                        )
                    except Exception as err:
                        pass
                self._members[peer_id].alive = alive
                self._members[peer_id].sock = peer_sock

    def _send_msg_through_sock(self, node_sock, msg):
        node_sock.send(msg)

    def _on_client_connect(self, client_sock, address):
            self._clients[address] = client_sock

    def _on_exit(self):
        for sock in self._clients:
            try:
                sock.send('_____ exiting')
            except Exception as e:
                LOG.debug('error on sending exit msg to client: %s' % str(e))

    def _on_handle_client_msg(self, msg):
        LOG.debug('notificting clients')
        for address, sock in self._clients.items():
            try:
                sock.send('_____ ' + msg)
            except Exception as e:
                LOG.debug('error on sending exit msg to client: %s' % str(e))


def main():
    from util import config_log
    from conf import set_conf
    import sys
    if len(sys.argv) < 2:
        print "Input error: insufficient argument."
        print "Usage: python node.py <conf path>"
        sys.exit(1)
    set_conf(sys.argv[1])
    from conf import CONF
    config_log(CONF=CONF)
    node = Node(CONF)
    node.start()
    node.wait()

if __name__ == '__main__':
    main()
