import eventlet
import msgpack

from . import cmds
from . import msgs


class RaftClient(object):

    def __init__(self, leader_address):
        self._leader_address = leader_address
        self._leader_sock = eventlet.connect(leader_address)
        self.status = 'init'
        self._cmd_queue = eventlet.queue.Queue()
        self.cmd_seq = 0
        self.client_id = None

    def register(self):
        cmd = cmds.get_client_register_cmd()
        cmd_msg = msgs.get_client_register_req_msg(cmd)
        ret = self.execute_command(cmd_msg)
        self.cmd_seq = 0
        self.client_id = ret['client_id']
        return ret

    def get_next_seq(self):
        self.cmd_seq += 1
        return self.cmd_seq

    def send_command_req(self, command_msg):
        self._leader_sock.sendall(command_msg)

    def set_value(self, key, value):
        cmd = cmds.get_client_update_cmd(
            key,
            value,
        )
        cmd_msg = msgs.get_client_update_req_msg(
            self.client_id,
            self.get_next_seq(),
            cmd,
        )
        return self.execute_command(cmd_msg)

    def get_value(self, key):
        cmd = cmds.get_client_query_cmd(key)
        cmd_msg = msgs.get_client_query_req_msg(
            self.client_id,
            cmd,
        )
        return self.execute_command(cmd_msg)

    def wait_command_ret(self):
        return msgpack.unpackb(self._leader_sock.recv(1024))

    def execute_command(self, command_msg):
        self.send_command_req(command_msg)
        ret = self.wait_command_ret()
        if ret['success']:
            return ret
        else:
            raise Exception(str(ret))
