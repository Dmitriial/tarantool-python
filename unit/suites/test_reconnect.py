# -*- coding: utf-8 -*-

from __future__ import print_function

import os
import sys
import unittest
import warnings
import tarantool
from .lib.tarantool_server import TarantoolServer


class TestSuite_Reconnect(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        print(' RECONNECT '.center(70, '='), file=sys.stderr)
        print('-' * 70, file=sys.stderr)
        self.srv = TarantoolServer()
        self.srv.script = 'unit/suites/box.lua'
        self.srv2 = None

    def setUp(self):
        # prevent a remote tarantool from clean our session
        if self.srv.is_started():
            self.srv.touch_lock()

    def test_01_simple(self):
        # Create a connection, but don't connect it.
        con = tarantool.Connection(self.srv.host, self.srv.args['primary'],
                                   connect_now=False)

        # Trigger a reconnection due to server unavailability.
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with self.assertRaises(tarantool.error.NetworkError):
                con.ping()

        # Start a server and verify that the reconnection
        # succeeds.
        self.srv.start()
        self.assertIs(con.ping(notime=True), "Success")

        # Close the connection and stop the server.
        con.close()
        self.srv.stop()

    def test_02_wrong_auth(self):
        # Create a connection with wrong credentials, but don't
        # connect it.
        con = tarantool.Connection(self.srv.host, self.srv.args['primary'],
                                   connect_now=False, user='not_exist')

        # Start a server.
        self.srv.start()

        # Trigger a reconnection due to wrong credentials.
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with self.assertRaises(tarantool.error.DatabaseError):
                con.ping()

        # Set right credentials and verify that the reconnection
        # succeeds.
        con.user = None
        self.assertIs(con.ping(notime=True), "Success")

        # Close the connection and stop the server.
        con.close()
        self.srv.stop()

    def test_03_mesh(self):
        # Multiple servers are not supported on Windows
        if os.name == 'nt':
            return

        # Start two servers
        self.srv.start()
        self.srv.admin("box.schema.user.create('test', { password = 'test', if_not_exists = true })")
        self.srv.admin("box.schema.user.grant('test', 'execute', 'universe')")

        self.srv2 = TarantoolServer()
        self.srv2.script = 'unit/suites/box.lua'
        self.srv2.start()
        self.srv2.admin("box.schema.user.create('test', { password = 'test', if_not_exists = true })")
        self.srv2.admin("box.schema.user.grant('test', 'execute', 'universe')")

        # get_nodes function contains both servers' addresses
        get_nodes = " \
            function get_nodes() \
                return { \
                    { \
                        host = '%s', \
                        port = tonumber(%d) \
                    }, \
                    { \
                        host = '%s', \
                        port = tonumber(%d) \
                    } \
                } \
            end" % (self.srv.host, self.srv.args['primary'], self.srv2.host, self.srv2.args['primary'])

        # Create get_nodes function on servers
        self.srv.admin(get_nodes)
        self.srv2.admin(get_nodes)

        # Create srv_id function (for testing purposes)
        self.srv.admin("function srv_id() return 1 end")
        self.srv2.admin("function srv_id() return 2 end")

        # Create a mesh connection, pass only the first server address
        con = tarantool.MeshConnection([{
            'host': self.srv.host, 'port': self.srv.args['primary']}],
            user='test',
            password='test',
            get_nodes_function_name='get_nodes',
            connect_now=True)

        # Check we work with the first server
        resp = con.call('srv_id')
        self.assertIs(resp.data and resp.data[0] == 1, True)

        # Stop the first server
        self.srv.stop()

        # Check we work with the second server
        resp = con.call('srv_id')
        self.assertIs(resp.data and resp.data[0] == 2, True)

        # Stop the second server
        self.srv2.stop()

        # Close the connection
        con.close()

    def test_04_mesh_exclude_node(self):
        # Multiple servers are not supported on Windows
        if os.name == 'nt':
            return

        # Start two servers
        self.srv.start()
        self.srv.admin("box.schema.user.create('test', { password = 'test', if_not_exists = true })")
        self.srv.admin("box.schema.user.grant('test', 'execute', 'universe')")

        self.srv2 = TarantoolServer()
        self.srv2.script = 'unit/suites/box.lua'
        self.srv2.start()
        self.srv2.admin("box.schema.user.create('test', { password = 'test', if_not_exists = true })")
        self.srv2.admin("box.schema.user.grant('test', 'execute', 'universe')")

        # get_nodes function contains only the second server address
        get_nodes = " \
            function get_nodes() \
                return { \
                    { \
                        host = '%s', \
                        port = tonumber(%d) \
                    } \
                } \
            end" % (self.srv2.host, self.srv2.args['primary'])

        # Create get_nodes function on servers
        self.srv.admin(get_nodes)
        self.srv2.admin(get_nodes)

        # Create srv_id function (for testing purposes)
        self.srv.admin("function srv_id() return 1 end")
        self.srv2.admin("function srv_id() return 2 end")

        # Create a mesh connection, pass only the first server address
        con = tarantool.MeshConnection([{
            'host': self.srv.host, 'port': self.srv.args['primary']}],
            user='test',
            password='test',
            get_nodes_function_name='get_nodes',
            connect_now=True)

        # Check we work with the second server
        resp = con.call('srv_id')
        self.assertIs(resp.data and resp.data[0] == 2, True)

        # Stop servers
        self.srv.stop()
        self.srv2.stop()

        # Close the connection
        con.close()

    @classmethod
    def tearDownClass(self):
        self.srv.clean()
        if self.srv2:
            self.srv2.clean()
