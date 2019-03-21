# -*- coding: utf-8 -*-
'''
This module provides MeshConnection class with automatic switch
between tarantool instances and basic Round-Robin strategy.
'''

import time
from tarantool.connection import Connection
from tarantool.error import NetworkError
from tarantool.utils import ENCODING_DEFAULT
from tarantool.const import (
    SOCKET_TIMEOUT,
    RECONNECT_MAX_ATTEMPTS,
    RECONNECT_DELAY,
    NODES_REFRESH_INTERVAL
)


class RoundRobinStrategy(object):
    def __init__(self, addrs):
        self.addrs = addrs
        self.pos = 0

    def getnext(self):
        tmp = self.pos
        self.pos = (self.pos + 1) % len(self.addrs)
        return self.addrs[tmp]


class MeshConnection(Connection):
    def __init__(self, addrs,
                 user=None,
                 password=None,
                 socket_timeout=SOCKET_TIMEOUT,
                 reconnect_max_attempts=RECONNECT_MAX_ATTEMPTS,
                 reconnect_delay=RECONNECT_DELAY,
                 connect_now=True,
                 encoding=ENCODING_DEFAULT,
                 strategy_class=RoundRobinStrategy,
                 nodes_refresh_interval=NODES_REFRESH_INTERVAL):
        self.nattempts = 2 * len(addrs) + 1
        self.strategy = strategy_class(addrs)
        self.strategy_class = strategy_class
        addr = self.strategy.getnext()
        host = addr['host']
        port = addr['port']
        self.nodes_refresh_interval = nodes_refresh_interval
        self.last_nodes_refresh = 0
        super(MeshConnection, self).__init__(host=host,
                                             port=port,
                                             user=user,
                                             password=password,
                                             socket_timeout=socket_timeout,
                                             reconnect_max_attempts=reconnect_max_attempts,
                                             reconnect_delay=reconnect_delay,
                                             connect_now=connect_now,
                                             encoding=encoding)

    def _opt_reconnect(self):
        nattempts = self.nattempts
        while nattempts > 0:
            try:
                super(MeshConnection, self)._opt_reconnect()
                break
            except NetworkError:
                nattempts -= 1
                addr = self.strategy.getnext()
                self.host = addr['host']
                self.port = addr['port']
        else:
            raise NetworkError

        if self.authenticated:
            now = time.time()
            if now - self.last_nodes_refresh > self.nodes_refresh_interval:
                self.refresh_nodes(now)

    def refresh_nodes(self, cur_time):
        resp = super(MeshConnection, self).eval_ex('return get_nodes ~= nil',
                                                   [], reconnect=False)
        if not (resp.data and resp.data[0]):
            return

        resp = super(MeshConnection, self).call_ex('get_nodes', [],
                                                   reconnect=False)

        if not (resp.data and resp.data[0]):
            return

        addrs = resp.data[0]
        if type(addrs) is list:
            self.strategy = self.strategy_class(addrs)
            self.last_nodes_refresh = cur_time
            if not {'host': self.host, 'port': self.port} in addrs:
                addr = self.strategy.getnext()
                self.host = addr['host']
                self.port = addr['port']
                self.close()
                self._opt_reconnect()
