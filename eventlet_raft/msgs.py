import msgpack

from . import settings


MSG_TYPE_NAME_MAP = {
    settings.MSG_TYPE_VOTE_REQ: 'vote',
    settings.MSG_TYPE_VOTE_RET: 'vote_return',
    settings.MSG_TYPE_LOG_ENTRY_APPEND_REQ: 'append_entry',
    settings.MSG_TYPE_LOG_ENTRY_APPEND_RET: 'append_entry_return',
    settings.MSG_TYPE_CLIENT_REGISTER_REQ: 'client_register',
    settings.MSG_TYPE_CLIENT_REGISTER_RET: 'client_register_return',
    settings.MSG_TYPE_TERM_INIT: 'term_init',
}


def get_vote_msg(node_id,
                 term,
                 last_log_index,
                 last_log_term,
                 ):
    return msgpack.packb(
        {
            'type': settings.MSG_TYPE_VOTE_REQ,
            'node_id': node_id,
            'term': term,
            'last_log_index': last_log_index,
            'last_log_term': last_log_term,
        }
    )


def get_vote_return_msg(node_id, term, accept):
    return msgpack.packb(
        {
            'type': settings.MSG_TYPE_VOTE_RET,
            'node_id': node_id,
            'term': term,
            'accept': accept
        }
    )


def get_append_entry_msg(node_id,
                         term,
                         prev_log_index,
                         prev_log_term,
                         entries,
                         leader_commit,
                         ):
    return msgpack.packb(
        {
            'type': settings.MSG_TYPE_LOG_ENTRY_APPEND_REQ,
            'node_id': node_id,
            'term': term,
            'prev_log_index': prev_log_index,
            'prev_log_term': prev_log_term,
            'entries': entries,
            'leader_commit': leader_commit,
        }
    )


def get_append_entry_ret_msg(node_id,
                             term,
                             last_log_index,
                             success,
                             leader_commit,
                             ):
    return msgpack.packb(
        {
            'type': settings.MSG_TYPE_LOG_ENTRY_APPEND_RET,
            'node_id': node_id,
            'term': term,
            'last_log_index': last_log_index,
            'success': success,
            'leader_commit': leader_commit,
        }
    )


def get_client_register_req_msg(cmd):
    return msgpack.packb(
        {
            'type': settings.MSG_TYPE_CLIENT_REGISTER_REQ,
            'cmd': cmd,
        }
    )


def get_client_register_ret_msg(success,
                                client_id,
                                leader_hint,
                                ):
    return msgpack.packb(
        {
            'type': settings.MSG_TYPE_CLIENT_REGISTER_RET,
            'success': success,
            'client_id': client_id,
            'leader_hint': leader_hint,
        }
    )


def get_client_update_req_msg(client_id,
                              sequence_num,
                              command,
                              ):
    return msgpack.packb(
        {
            'type': settings.MSG_TYPE_CLIENT_UPDATE_REQ,
            'client_id': client_id,
            'seq': sequence_num,
            'cmd': command,
        }
    )


def get_client_update_or_query_ret_msg(success,
                                       response,
                                       leader_hint,
                                       ):
    return msgpack.packb(
        {
            'type': settings.MSG_TYPE_CLIENT_UPDATE_RET,
            'success': success,
            'resp': response,
            'leader_hint': leader_hint,
        }
    )


def get_client_query_req_msg(client_id,
                             command,
                             ):
    return msgpack.packb(
        {
            'type': settings.MSG_TYPE_CLIENT_QUERY_REQ,
            'client_id': client_id,
            'cmd': command,
        }
    )
