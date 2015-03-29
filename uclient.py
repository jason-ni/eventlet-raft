from eventlet_raft.client import RaftClient

server_address_list = [
    ('127.0.0.1', 4000),
    ('127.0.0.1', 4001),
    ('127.0.0.1', 4002),
    ('127.0.0.1', 4003),
    ('127.0.0.1', 4004),
]


def write_log(log, data, msg):
    log.write("{0}: {1}\n".format(
        msg,
        str(data),
    ))

client = RaftClient(server_address_list)
print client.register()

with open('uclient.log', 'w') as log:
    for i in range(2000):
        write_log(log, client.set_value('age', '%s' % i),
                  "set value({0})".format(i),
                  )
        write_log(log, client.get_value('age'),
                  "get value({0})".format(i),
                  )
