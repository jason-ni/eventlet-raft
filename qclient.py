from datetime import datetime
from eventlet_raft.client import RaftClient

server_address_list = [
    ('127.0.0.1', 4000),
    ('127.0.0.1', 4001),
    ('127.0.0.1', 4002),
]

client = RaftClient(server_address_list)
print client.register()

print client.get_value('name')
print client.get_value('age')
