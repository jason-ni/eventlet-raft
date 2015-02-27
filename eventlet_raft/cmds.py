from msgpack import packb

from .settings import STM_OP_REG

def get_client_register_cmd():
    return packb({'op': STM_OP_REG})
