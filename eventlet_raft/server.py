import sys
import eventlet
from eventlet import event
import logging
import msgpack

from .settings import BUF_LEN


LOG = logging.getLogger('Server')


class Server(object):

    exit_event = event.Event()

    def __init__(self, conf):
        super(Server, self).__init__()
        self._node_listen_ip = conf.get('server', 'node_listen_ip')
        self._node_listen_port = int(conf.get('server', 'node_listen_port'))
        self._node_listen_sock = None
        self._client_listen_ip = conf.get('server', 'client_listen_ip')
        self._client_listen_port = int(conf.get('server', 'client_listen_port'))
        self._client_listen_sock = None
        self._threads = []

    def _handle_node_sock(self, node_sock):
        LOG.debug("Get a node socket")
        unpacker = msgpack.Unpacker()
        while True:
            try:
                chunk = node_sock.recv(BUF_LEN)
                if not chunk:
                    break
                unpacker.feed(chunk)
                for unpacked_msg in unpacker:
                    self._on_handle_node_msg(unpacked_msg)
            except Exception as e:
                LOG.debug("node sock error: %s" % str(e))
                break

    def _on_handle_node_msg(self, msg):
        pass

    def _handle_client_sock(self, client_sock):
        LOG.debug("Get a client socket")
        while True:
            try:
                c = client_sock.recv(BUF_LEN)
                if not c:
                    break
                LOG.debug("Get client message: %s" % c)
                msg = c.strip()
                if msg == 'close':
                    client_sock.close()
                    break
                elif msg == 'EXIT':
                    self.exit_event.send('EXIT')
                else:
                    self._on_handle_client_msg(msg)
            except Exception as e:
                LOG.debug("client sock error: %s" % str(e))
                break

    def _on_handle_client_msg(self, msg):
        pass

    def _on_node_connect(self, node_sock, address):
        pass

    def _handle_node_accept(self):
        while True:
            node_sock, address = self._node_listen_sock.accept()
            self._on_node_connect(node_sock, address)
            self._threads.append(
                eventlet.spawn(self._handle_node_sock, node_sock)
            )

    def _on_client_connect(self, client_sock, address):
        pass

    def _handle_client_accept(self):
        while True:
            client_sock, address = self._client_listen_sock.accept()
            self._on_client_connect(client_sock, address)
            self._threads.append(
                eventlet.spawn(self._handle_client_sock, client_sock)
            )

    def _on_start(self):
        pass

    def start(self):
        self._node_listen_sock = eventlet.listen(
            (self._node_listen_ip, self._node_listen_port)
        )
        self._threads.append(eventlet.spawn(self._handle_node_accept))
        self._client_listen_sock = eventlet.listen(
            (self._client_listen_ip, self._client_listen_port)
        )
        self._threads.append(eventlet.spawn(self._handle_client_accept))
        self._on_start()

    def _shutdown(self):
        LOG.debug("Exiting...")
        self._on_exit()
        for thread in self._threads:
            if thread:
                thread.kill()
            else:
                LOG.debug("--- none thread")
        sys.exit(0)

    def _on_exit(self):
        pass

    def wait(self):
        LOG.debug("Waiting for msg to exit")
        self.exit_event.wait()
        LOG.debug("Received exit event")
        self._shutdown()


def main():
    from util import config_log
    from conf import set_conf
    set_conf('test.conf')
    from .conf import CONF
    config_log()
    server = Server(CONF)
    server.start()
    server.wait()

if __name__ == '__main__':
    main()
