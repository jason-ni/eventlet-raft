import sys
from datetime import datetime

from eventlet_raft.client import RaftClient

port = int(sys.argv[1])

client = RaftClient(('127.0.0.1', port))
print client.register()

before = datetime.now()
for i in range(200):
    client.set_value('name', 'Jason')
    if i % 10 == 0:
        print i
print datetime.now() - before
