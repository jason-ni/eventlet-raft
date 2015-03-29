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

with open('counter_test.log', 'w') as log:
    ret = client.set_value('counter', 0)
    if not ret['success']:
        raise Exception("failed to reset counter")
    write_log(log, ret, 'reset counter')
    accu = 0
    for i in range(1000):
        ret = client.set_value('counter', i)
        if not ret['success']:
            raise Exception("failed to set counter")
        write_log(log, ret, 'set counter:')

        ret = client.get_value('counter')
        write_log(log, ret, 'get counter:')
        if not ret['success']:
            raise Exception("failed to get counter")
        accu += ret['resp'][1]
        write_log(log, accu, i)
    print 'result: ', accu
