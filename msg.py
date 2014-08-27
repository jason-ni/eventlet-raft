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


def get_append_entry_msg(node_id, term, entry):
    return msgpack.packb(
        {
            'name': 'append_entry',
            'node_id': node_id,
            'term': term,
            'entry': entry
        }
    )
