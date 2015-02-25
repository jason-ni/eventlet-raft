import msgpack


def get_vote_msg(node_id, term):
    return msgpack.packb(
        {
            'name': 'vote',
            'node_id': node_id,
            'term': term
        }
    )


def get_vote_return_msg(node_id, term, accept):
    return msgpack.packb(
        {
            'name': 'vote_return',
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
                         ):
    return msgpack.packb(
        {
            'name': 'append_entry',
            'node_id': node_id,
            'term': term,
            'prev_log_index': prev_log_index,
            'prev_log_term': prev_log_term,
            'ping_id': ping_id,
            'entries': entries,
        }
    )
