import eventlet
import msgpack
import random
from copy import copy

from . import cmds
from . import msgs


class RaftClient(object):

    def __init__(self, server_address_list):
        self.server_address_list = server_address_list
        self._leader_address = None
        self._leader_sock = None
        self.status = 'init'
        self.cmd_seq = 0
        self.client_id = None

    @classmethod
    def select_server(cls, server_address_list):
        return random.choice(server_address_list)

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
        unpacker = msgpack.Unpacker()
        while True:
            chunk = self._leader_sock.recv(1024)
            if len(chunk) == 0:
                break
            unpacker.feed(chunk)
            try:
                return unpacker.next()
            except StopIteration:
                pass
        return None

    def execute_command(self, command_msg):
        s_addr_list = copy(self.server_address_list)
        while len(s_addr_list) > 0:
            try:
                if self._leader_address is None:
                    self._leader_address = RaftClient.select_server(s_addr_list)
                if self._leader_sock is None:
                    self._leader_sock = eventlet.connect(self._leader_address)
                self.send_command_req(command_msg)
                ret = self.wait_command_ret()
                if ret is not None:
                    if ret['success']:
                        return ret
                    else:
                        if 'leader_hint' in ret:
                            self._leader_sock.close()
                            self._leader_sock = None
                            self._leader_address = (
                                ret['leader_hint'][0],
                                ret['leader_hint'][1] + 1000,
                            )
                            continue
            except Exception:
                pass
            if self._leader_address in s_addr_list:
                s_addr_list.remove(self._leader_address)
            if len(s_addr_list) == 0:
                s_addr_list = copy(self.server_address_list)
            self._leader_address = None
            self._leader_sock = None
