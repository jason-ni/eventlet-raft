import msgpack

from .settings import MSG_TYPE_VOTE_REQ, MSG_TYPE_VOTE_RET
from .settings import MSG_TYPE_LOG_ENTRY_APPEND_REQ
from .settings import MSG_TYPE_LOG_ENTRY_APPEND_RET
from .settings import MSG_TYPE_CLIENT_REGISTER_REQ
from .settings import MSG_TYPE_CLIENT_REGISTER_RET


MSG_TYPE_NAME_MAP = {
    MSG_TYPE_VOTE_REQ: 'vote',
    MSG_TYPE_VOTE_RET: 'vote_return',
    MSG_TYPE_LOG_ENTRY_APPEND_REQ: 'append_entry',
    MSG_TYPE_LOG_ENTRY_APPEND_RET: 'append_entry_return',
    MSG_TYPE_CLIENT_REGISTER_REQ: 'client_register',
    MSG_TYPE_CLIENT_REGISTER_RET: 'client_register_return',
}


def get_vote_msg(node_id, term):
    return msgpack.packb(
        {
            'type': MSG_TYPE_VOTE_REQ,
            'node_id': node_id,
            'term': term
        }
    )


def get_vote_return_msg(node_id, term, accept):
    return msgpack.packb(
        {
            'type': MSG_TYPE_VOTE_RET,
            'node_id': node_id,
            'term': term,
            'accept': accept
        }
    )


def get_append_entry_msg(node_id,
                         term,
                         prev_log_index,
                         prev_log_term,
                         ping_id,
                         entries,
                         leader_commit,
                         ):
    return msgpack.packb(
        {
            'type': MSG_TYPE_LOG_ENTRY_APPEND_REQ,
            'node_id': node_id,
            'term': term,
            'prev_log_index': prev_log_index,
            'prev_log_term': prev_log_term,
            'ping_id': ping_id,
            'entries': entries,
            'leader_commit': leader_commit,
        }
    )


def get_append_entry_ret_msg(node_id,
                             term,
                             last_log_index,
                             success,
                             ):
    return msgpack.packb(
        {
            'type': MSG_TYPE_LOG_ENTRY_APPEND_RET,
            'node_id': node_id,
            'term': term,
            'last_log_index': last_log_index,
            'success': success,
        }
    )


def get_client_register_req_msg(cmd):
    return msgpack.packb(
        {
            'type': MSG_TYPE_CLIENT_REGISTER_REQ,
            'cmd': cmd,
        }
    )


def get_client_register_ret_msg(success,
                                client_id,
                                leader_hint,
                                ):
    return msgpack.packb(
        {
            'type': MSG_TYPE_CLIENT_REGISTER_RET,
            'success': success,
            'client_id': client_id,
            'leader_hint': leader_hint,
        }
    )
