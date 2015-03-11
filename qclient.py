from datetime import datetime
from eventlet_raft.client import RaftClient

server_address_list = [
    ('127.0.0.1', 4000),
    ('127.0.0.1', 4001),
    ('127.0.0.1', 4002),
]

client = RaftClient(server_address_list)
print client.register()

before = datetime.now()
for i in range(200):
    client.set_value('name', 'jason' + str(i))
print before
print datetime.now() - before
