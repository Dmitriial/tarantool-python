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
    CONNECTION_TIMEOUT,
    SOCKET_TIMEOUT,
    RECONNECT_MAX_ATTEMPTS,
    RECONNECT_DELAY,
    DEFAULT_CLUSTER_DISCOVERY_DELAY_MILLIS,
)

from tarantool.request import (
    RequestCall
)


class RoundRobinStrategy(object):
    """
    Simple roundrobin address rotation
    """
    def __init__(self, addrs):
        self.addrs = addrs
        self.pos = 0

    def getnext(self):
        tmp = self.pos
        self.pos = (self.pos + 1) % len(self.addrs)
        return self.addrs[tmp]


def parse_uri(uri_str):
    if not uri_str or ':' not in uri_str:
        return None
    uri = uri_str.split(':')
    host = uri[0]
    try:
        port = int(uri[1])
    except ValueError:
        return None
    
    if host and port:
        return {'host': host, 'port': port}
    else:
        return None


class MeshConnection(Connection):
    '''
    Represents a connection to a cluster of Tarantool servers.

    This class uses Connection to connect to one of the nodes of the cluster.
    The initial list of nodes is passed to the constructor in 'addrs' parameter.
    The class set in 'strategy_class' parameter is used to select a node from
    the list and switch nodes in case of unavailability of the current node.

    'get_nodes_function_name' param of the constructor sets the name of a stored
    Lua function used to refresh the list of available nodes. The function takes
    no parameters and returns a list of strings in format 'host:port'. A generic
    function for getting the list of nodes looks like this:

    .. code-block:: lua

        function get_cluster_nodes()
            return {
                '192.168.0.1:3301',
                '192.168.0.2:3302',
                -- ...
            }
        end

    You may put in this list whatever you need depending on your cluster
    topology. Chances are you'll want to make the list of nodes from nodes'
    replication config. Here is an example for it:

    .. code-block:: lua

        local uri_lib = require('uri')

        function get_cluster_nodes()
            local nodes = {}

            local replicas = box.cfg.replication

            for i = 1, #replicas do
                local uri = uri_lib.parse(replicas[i])

                if uri.host and uri.service then
                    table.insert(nodes, uri.host .. ':' .. uri.service)
                end
            end

            -- if your replication config doesn't contain the current node
            -- you have to add it manually like this:
            table.insert(nodes, '192.168.0.1:3301')

            return nodes
        end
    '''

    def __init__(self, host, port,
                 user=None,
                 password=None,
                 socket_timeout=SOCKET_TIMEOUT,
                 reconnect_max_attempts=RECONNECT_MAX_ATTEMPTS,
                 reconnect_delay=RECONNECT_DELAY,
                 connect_now=True,
                 encoding=ENCODING_DEFAULT,
                 call_16=False,
                 connection_timeout=CONNECTION_TIMEOUT,
                 cluster_list=None,
                 strategy_class=RoundRobinStrategy,
                 get_nodes_function_name=None,
                 nodes_refresh_interval=DEFAULT_CLUSTER_DISCOVERY_DELAY_MILLIS):

        addrs = [{"host": host, "port": port}]
        if cluster_list:
            for i in cluster_list:
                if i["host"] == host or i["port"] == port:
                    continue
                addrs.append(i)

        self.strategy = strategy_class(addrs)
        self.strategy_class = strategy_class
        addr = self.strategy.getnext()
        host = addr['host']
        port = addr['port']
        self.get_nodes_function_name = get_nodes_function_name
        self.nodes_refresh_interval = nodes_refresh_interval
        self.last_nodes_refresh = time.time()
        super(MeshConnection, self).__init__(host=host,
                                             port=port,
                                             user=user,
                                             password=password,
                                             socket_timeout=socket_timeout,
                                             reconnect_max_attempts=reconnect_max_attempts,
                                             reconnect_delay=reconnect_delay,
                                             connect_now=connect_now,
                                             encoding=encoding,
                                             call_16=call_16,
                                             connection_timeout=connection_timeout)

    def _opt_refresh_instances(self):
        """
        Refresh list of cluster instances.
        If current connection not in server list will change connection.
        """
        now = time.time()

        if self.connected and now - self.last_nodes_refresh > self.nodes_refresh_interval/1000:
            request = RequestCall(self, self.get_nodes_function_name, (), self.call_16)
            resp = self._send_request_wo_reconnect(request)

            # got data to refresh
            if resp.data and resp.data[0]:
                addrs = list(parse_uri(i) for i in resp.data[0])
                self.strategy = self.strategy_class(addrs)
                self.last_nodes_refresh = now

            if {'host': self.host, 'port': self.port} not in addrs:
                addr = self.strategy.getnext()
                self.host = addr['host']
                self.port = addr['port']
                self.close()

        if not self.connected:

            nattempts = (len(self.strategy.addrs) * 2) + 1

            while nattempts >= 0:
                try:
                    addr = self.strategy.getnext()
                    if addr['host'] != self.host or addr['port'] != self.port:
                        self.host = addr['host']
                        self.port = addr['port']
                        self._opt_reconnect()
                        break
                    else:
                        nattempts -= 1
                except NetworkError:
                    continue
            else:
                raise NetworkError

    def _send_request(self, request):
        '''
        Send the request to the server through the socket.
        Return an instance of `Response` class.

        Update instances list from server `get_nodes_function_name` function.

        :param request: object representing a request
        :type request: `Request` instance

        :rtype: `Response` instance
        '''
        if self.get_nodes_function_name:
            self._opt_refresh_instances()

        try:
            return super(MeshConnection, self)._send_request(request)
        except NetworkError:
            self.connected = False
            self._opt_refresh_instances()
        finally:
            return super(MeshConnection, self)._send_request(request)
