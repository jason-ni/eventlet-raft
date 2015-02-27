import sys
from eventlet_raft.client import RaftClient

port = int(sys.argv[1])

client = RaftClient(('127.0.0.1', port))
print client.register()
