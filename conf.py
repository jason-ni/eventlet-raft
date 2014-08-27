import ConfigParser

CONF = None


def set_conf(conf_path):
    global CONF
    CONF = ConfigParser.SafeConfigParser()
    CONF.read(conf_path)


def main():
    set_conf('test.conf')
    print CONF.get('server', 'node_listen_ip')
    for peer_section in CONF.get('server', 'peers').split(','):
        print CONF.get(peer_section.strip(), 'peer_port')

if __name__ == '__main__':
    main()
