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
for i in range(2000):
    print client.get_value('name')
print before
print datetime.now() - before
