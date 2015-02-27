import logging
import time
import eventlet
import msgpack
import random

from . import msgs
from .log import RaftLog

from .server import Server
from .settings import VOTE_CYCLE, PING_CYCLE, ELECTION_TIMEOUT, TICK_CYCLE
# from .settings import MSG_TYPE_VOTE_REQ, MSG_TYPE_VOTE_RET
# from .settings import MSG_TYPE_LOG_ENTRY_APPEND_REQ
# from .settings import MSG_TYPE_LOG_ENTRY_APPEND_RET
from .settings import MSG_TYPE_CLIENT_REGISTER_REQ
from .settings import LOG_TYPE_CLIENT_REQ
from .settings import STM_OP_REG
from .stm.state_machine import DictStateMachine


LOG = logging.getLogger('Node')


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
        self._client_req_queue = eventlet.queue.Queue()
        self._ret_queue_map = {}

        self._stm = DictStateMachine('DictStateMachine.snap')
        self._raft_log = RaftLog(node_id=self.id, progress=self._members)

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
            if self._is_leader:
                # client requests processing
                client_reqs = self._batch_client_req_from_queue()
                self._process_client_reqs(client_reqs)

                # check and update commit
                self._raft_log.check_and_update_commits(
                    self._term, self.majority
                )

            self._apply_commits()

            if self._is_leader:
                # Broadcast append log entry messages
                current_timestamp = time.time()
                if current_timestamp - last_ping_timestamp > PING_CYCLE:
                    last_ping_timestamp = current_timestamp
                    self._broadcast_append_entry_msg()

            eventlet.sleep(TICK_CYCLE)

    def _apply_commits(self):
        for entry in self._raft_log.get_commited_entries_for_apply():
            try:
                cmd = msgpack.unpackb(entry['cmd'])
                log_index = entry['log_index']
                stm_ret = self._stm.apply_cmd(log_index, cmd)
                if self._is_leader:
                    self._reply_client(log_index, cmd, stm_ret)
                self._raft_log.last_applied = log_index
            except Exception:
                LOG.exception(
                    'Apply entry(%s) error.' % log_index,
                )

    def _reply_client(self, log_index, cmd, stm_ret):
        ret_msg = None
        if cmd['op'] == STM_OP_REG:
            ret_msg = msgs.get_client_register_ret_msg(
                True,
                log_index,
                None,
            )
        if (ret_msg is not None) and (log_index in self._ret_queue_map):
            self._ret_queue_map[log_index].put(ret_msg)

    def _process_client_reqs(self, client_reqs):
        for req in client_reqs:
            LOG.info("Processing request: %s" % req)
            log_entry = self._raft_log.build_log_entry(
                self._term,
                LOG_TYPE_CLIENT_REQ,
                req['cmd'],
            )
            self._raft_log.append(log_entry)
            if req['type'] == MSG_TYPE_CLIENT_REGISTER_REQ:
                log_index = log_entry['log_index']
                if log_index not in self._ret_queue_map:
                    self._ret_queue_map[log_index] = req['ret_queue']
            else:
                # TODO: To support other commands
                pass

    def _batch_client_req_from_queue(self):
        client_reqs = []
        try:
            client_reqs.append(self._client_req_queue.get(block=False))
        except eventlet.queue.Empty:
            pass
        finally:
            return client_reqs

    def _broadcast_append_entry_msg(self):
        for peer_id, peer in self._members.items():
            if peer_id == self.id:
                continue
            LOG.debug('trying to send append entry for peer %s' % peer)
            # TODO: Need to check if peer log is too old that we need to send
            # snapshot to the peer.
            if peer.next_idx <= self._raft_log.first_log_index:
                LOG.error(
                    "Need to send snapshot to this peer %s." % str(peer_id)
                )
                continue
            need_replicate_entries, prev_log_index, prev_log_term = \
                self._raft_log.get_need_replicate_entries_for_peer(peer)
            msg = msgs.get_append_entry_msg(
                self.id,
                self._term,
                prev_log_index,
                prev_log_term,
                self._ping_counter,
                need_replicate_entries,
                self._raft_log.commited,
            )
            self._ping_counter += 1
            self._try_connect_and_send_msg(peer_id, msg)

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

        if len(msg['entries']) > 0:
            LOG.info("^^^ receive log entries:\n %s" % str(msg['entries']))
            LOG.info("^^^ %s is follower: %s" % (self.id, self._is_follower))

        # TODO: We rely on eventlet single thread peculiarity for thread safty.
        # Log merging operations can be moved to tick loop routine. But I don't
        # think we have problem to put it here.
        if self._is_follower:
            LOG.debug("prev_log_index: %s" % msg['prev_log_index'])
            LOG.debug("prev_log_term: %s" % msg['prev_log_term'])
            if self._raft_log.can_append(
                msg['prev_log_index'], msg['prev_log_term']
            ):
                self._raft_log.trunk_append_entries(
                    msg['prev_log_index'],
                    msg['entries'],
                    msg['leader_commit'],
                )
                append_entry_return_msg = msgs.get_append_entry_ret_msg(
                    self.id,
                    self._term,
                    self._raft_log.last_log_index,
                    True,
                )
            else:
                append_entry_return_msg = msgs.get_append_entry_ret_msg(
                    self.id,
                    self._term,
                    self._raft_log.last_log_index,
                    False,
                )
            self._try_connect_and_send_msg(
                msg['node_id'],
                append_entry_return_msg,
            )

    def msg_append_entry_return(self, msg):
        if msg['success']:
            self._members[msg['node_id']].next_idx = msg['last_log_index'] + 1
        else:
            # TODO: if a follower's log is too old, all it's in memory log
            # entries might not match with leader's. At this time, this follower
            # should be restored from leader's snapshot.
            LOG.info('*** msg_node_id: %s' % str(msg['node_id']))
            LOG.info('*** msg_last_log_index: %s' % msg['last_log_index'])
            LOG.info(
                '*** peer next: %s' % self._members[msg['node_id']].next_idx
            )
            if msg['last_log_index'] < self._members[msg['node_id']].next_idx:
                self._members[msg['node_id']].next_idx = \
                    msg['last_log_index'] + 1
            else:
                self._members[msg['node_id']].next_idx -= 1

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
        vote_msg = msgs.get_vote_msg(self.id, self._term)
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
        msg['node_id'] = tuple(msg['node_id'])
        msg_name = msgs.MSG_TYPE_NAME_MAP[msg['type']]
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
        msg = msgs.get_vote_return_msg(self.id, self._term, accept)
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
        # TODO: at the beginning of a term, we should issue a blank operation
        # that makes sure previous term log entries are commited in time.
        if self._is_candidate:
            LOG.info('Node %s become leader.' % str(self.id))
            self._is_candidate = False
            self._is_leader = True
            self._is_follower = False
            self._last_timestamp = time.time()
            # reset log replication progress
            for peer in self._members.values():
                peer.next_idx = self._raft_log.last_log_index + 1

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
            except Exception as err:
                LOG.debug(
                    "connecting to %s error \n%s" % (str(peer_id), str(err))
                )
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
                    except Exception:
                        pass
                self._members[peer_id].alive = alive
                self._members[peer_id].sock = peer_sock

    def _send_msg_through_sock(self, node_sock, msg):
        node_sock.send(msg)

    def _on_client_connect(self, client_sock, address):
        pass

    def _on_exit(self):
        pass

    def _on_handle_client_msg(self, client_sock, msg):
        LOG.info('***** get client request: %s' % msg)
        # reject if not leader
        if not self._is_leader:
            # TODO: return leaderHint
            client_sock.sendall(
                msgs.get_client_register_ret_msg(False, None, None)
            )
        else:
            if msg['type'] == MSG_TYPE_CLIENT_REGISTER_REQ:
                self._handle_client_msg_register(client_sock, msg)
        eventlet.sleep()

    def _handle_client_msg_register(self, client_sock, msg):
        LOG.info('***** handling client register')
        ret_queue = eventlet.queue.Queue()
        msg['ret_queue'] = ret_queue
        self._client_req_queue.put(msg)
        # wait and reply client sock
        # TODO: handle timeout
        client_sock.sendall(ret_queue.get())


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
