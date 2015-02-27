import eventlet
import msgpack

import cmds
import msgs


class RaftClient(object):

    def __init__(self, leader_address):
        self._leader_address = leader_address
        self._leader_sock = eventlet.connect(leader_address)
        self.status = 'init'
        self._cmd_queue = eventlet.queue.Queue()
        self.msg_seq = 0

    def register(self):
        cmd = cmds.get_client_register_cmd()
        return self.execute_command(msgs.get_client_register_req_msg(cmd))

    def send_command_req(self, command):
        self._leader_sock.sendall(command)

    def wait_command_ret(self):
        return msgpack.unpackb(self._leader_sock.recv(1024))

    def execute_command(self, command):
        self.send_command_req(command)
        return self.wait_command_ret()
