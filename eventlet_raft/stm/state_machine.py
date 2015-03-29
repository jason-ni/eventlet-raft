import logging
import msgpack

from .. import settings
from ..errors import StateMachineKeyDoesNotExist


LOG = logging.getLogger('STM')


class MemoryStateMachine(object):

    def __init__(self):
        self._stm = {}

    def set(self, key, value):
        self._stm[key] = value

    def get(self, key):
        try:
            return self._stm[key]
        except KeyError:
            raise StateMachineKeyDoesNotExist(key=key)

    def apply(self, command):
        pass


class DictStateMachine(object):

    def __init__(self):
        self._stm = {}
        self._client_seq = {}

    def set(self, key, value):
        self._stm[key] = value

    def get(self, key):
        try:
            return self._stm[key]
        except KeyError:
            raise StateMachineKeyDoesNotExist(key=key)

    def apply_cmd(self, log_entry):
        client_id = log_entry['client_id']
        cmd = msgpack.unpackb(log_entry['cmd'])
        if cmd['op'] == settings.STM_OP_GET:
            try:
                return (
                    settings.STM_RET_CODE_OK,
                    self._stm[cmd['key']],
                    log_entry['seq'],
                )
            except KeyError:
                return (
                    settings.STM_RET_CODE_ERROR_EXT_KEY_ERROR,
                    None,
                    log_entry['seq'],
                )
        elif cmd['op'] == settings.STM_OP_SET:
            if self._client_seq[client_id] >= log_entry['seq']:
                LOG.info(
                    "Ignoring the client request (%s, %s)" % (
                        client_id, log_entry['seq'])
                )
            else:
                LOG.info(
                    "Setting key(%s) = value(%s)" % (cmd['key'], cmd['value'])
                )
                self._stm[cmd['key']] = cmd['value']
                self._client_seq[client_id] = log_entry['seq']
            return (
                settings.STM_RET_CODE_OK,
                cmd['value'],
                log_entry['seq'],
            )
        elif cmd['op'] == settings.STM_OP_REG:
            self._client_seq[client_id] = 0
            LOG.info("Set client cmd sequence for client_id %s" % client_id)
            return (
                settings.STM_RET_CODE_OK,
                client_id,
                0,
            )
