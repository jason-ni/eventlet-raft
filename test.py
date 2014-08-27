import eventlet
from eventlet import backdoor


q = eventlet.queue.Queue()
eventlet.spawn(backdoor.backdoor_server, eventlet.listen(('localhost', 3000)), globals())
q.get()
