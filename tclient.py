from eventlet_raft.client import RaftClient

server_address_list = [
    ('127.0.0.1', 4000),
    ('127.0.0.1', 4001),
    ('127.0.0.1', 4002),
]

client = RaftClient(server_address_list)
print client.register()

for i in range(10):
    print client.set_value('name', 'Jason %s' % i)
    print client.get_value('name')
