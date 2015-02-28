import sys
from datetime import datetime
from eventlet_raft.client import RaftClient

port = int(sys.argv[1])

client = RaftClient(('127.0.0.1', port))
print client.register()

before = datetime.now()
for i in range(4000):
    client.get_value('name')
print before
print datetime.now() - before
