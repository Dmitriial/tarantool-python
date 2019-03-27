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
    '''
    Represents a connection to a cluster of Tarantool servers.

    This class uses Connection to connect to one of the nodes of the cluster.
    The initial list of nodes is passed to the constructor in 'addrs' parameter.
    The class set in 'strategy_class' parameter is used to select a node from
    the list and switch nodes in case of unavailability of the current node.

    'get_nodes_function_name' param of the constructor sets the name of a stored
    Lua function used to refresh the list of available nodes. A generic function
    for getting the list of nodes looks like this:

    .. code-block:: lua

        function get_nodes()
            return {
                {
                    host = '192.168.0.1',
                    port = 3301
                },
                {
                    host = '192.168.0.2',
                    port = 3302
                },

                -- ...
            }
        end

    You may put in this list whatever you need depending on your cluster
    topology. Chances are you'll want to make the list of nodes from nodes'
    replication config. So here is an example for it:

    .. code-block:: lua

        local uri_lib = require('uri')

        function get_nodes()
            local nodes = {}

            local replicas = box.cfg.replication

            for i = 1, #replicas do
                local uri = uri_lib.parse(replicas[i])
                local port = tonumber(uri.service)

                if uri.host and port then
                    table.insert(nodes, { host = uri.host, port = port })
                end
            end

            -- if your replication config doesn't contain the current node
            -- you have to add it manually like this:
            table.insert(nodes, { host = '192.168.0.1', port = 3301 })

            return nodes
        end
    '''

    def __init__(self, addrs,
                 user=None,
                 password=None,
                 socket_timeout=SOCKET_TIMEOUT,
                 reconnect_max_attempts=RECONNECT_MAX_ATTEMPTS,
                 reconnect_delay=RECONNECT_DELAY,
                 connect_now=True,
                 encoding=ENCODING_DEFAULT,
                 strategy_class=RoundRobinStrategy,
                 get_nodes_function_name=None,
                 nodes_refresh_interval=NODES_REFRESH_INTERVAL):
        self.nattempts = 2 * len(addrs) + 1
        self.strategy = strategy_class(addrs)
        self.strategy_class = strategy_class
        addr = self.strategy.getnext()
        host = addr['host']
        port = addr['port']
        self.get_nodes_function_name = get_nodes_function_name
        self.nodes_refresh_interval = nodes_refresh_interval >= 30 and nodes_refresh_interval or 30
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

        if self.authenticated and self.get_nodes_function_name:
            now = time.time()
            if now - self.last_nodes_refresh > self.nodes_refresh_interval:
                self.refresh_nodes(now)

    def refresh_nodes(self, cur_time):
        '''
        Refreshes nodes list by calling Lua function with name
        self.get_nodes_function_name on the current node. If this field is None
        no refresh occurs. Usually you don't need to call this function manually
        since it's called automatically during reconnect every
        self.nodes_refresh_interval seconds.
        '''
        resp = super(MeshConnection, self).call_ex(self.get_nodes_function_name,
                                                   [], reconnect=False)

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
