from msgpack import packb

from . import settings


def get_client_register_cmd():
    return packb({'op': settings.STM_OP_REG})


def get_client_update_cmd(key, value):
    return packb({
        'op': settings.STM_OP_SET,
        'key': key,
        'value': value,
    })


def get_client_query_cmd(key):
    return packb({
        'op': settings.STM_OP_GET,
        'key': key,
    })
