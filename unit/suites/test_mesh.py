# -*- coding: utf-8 -*-

from __future__ import print_function

import os
import sys
import unittest
import warnings
import tarantool
from .lib.tarantool_server import TarantoolServer

@unittest.skipIf(sys.platform.startswith("win"), 'Mesh tests on windows platform not supported')
class TestSuite_Mesh(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        print(' MESH '.center(70, '='), file=sys.stderr)
        print('-' * 70, file=sys.stderr)
        
        # Start first server
        self.srv = TarantoolServer()
        self.srv.script = 'unit/suites/box.lua'
        self.srv.start()
        self.srv.admin("box.schema.user.create('test', { password = 'test', if_not_exists = true })")
        self.srv.admin("box.schema.user.grant('test', 'execute', 'universe')")
        
        # Start second server
        self.srv2 = TarantoolServer()
        self.srv2.script = 'unit/suites/box.lua'
        self.srv2.start()
        self.srv2.admin("box.schema.user.create('test', { password = 'test', if_not_exists = true })")
        self.srv2.admin("box.schema.user.grant('test', 'execute', 'universe')")

        # get_nodes function contains both servers' addresses
        get_nodes = " \
            function get_cluster_nodes() \
                return { '%s:%d', '%s:%d' } \
            end" % (self.srv.host, self.srv.args['primary'], self.srv2.host, self.srv2.args['primary'])

        # Create get_nodes function on servers
        self.srv.admin(get_nodes)
        self.srv2.admin(get_nodes)

        # Create srv_id function (for testing purposes)
        self.srv.admin("function srv_id() return 1 end")
        self.srv2.admin("function srv_id() return 2 end")


    def setUp(self):
        # prevent a remote tarantool from clean our session
        if self.srv.is_started():
            self.srv.touch_lock()
        
        if self.srv2.is_started():
            self.srv2.touch_lock()

    def test_01_connect_and_refresh(self):

        # Create a mesh connection, pass only the first server address
        con = tarantool.MeshConnection(self.srv.host, self.srv.args['primary'],
            user='test',
            password='test',
            get_nodes_function_name='get_cluster_nodes',
            connect_now=True)

        # Check strategy have one addr from config
        self.assertIs(len(con.strategy.addrs), 1) 

        con.last_nodes_refresh = 0

        # Check we work with the first server
        resp = con.call('srv_id')
        self.assertIs(resp.data and resp.data[0] == 1, True)
        
        # Check refresh is successful and strategy have 2 nodes
        self.assertIs(len(con.strategy.addrs), 2)

        con.close()

    def test_02_mesh_exclude_node(self):
        # Create a mesh connection, pass only the first server address
        con = tarantool.MeshConnection(self.srv.host, self.srv.args['primary'],
            user='test',
            password='test',
            get_nodes_function_name='get_cluster_nodes',
            connect_now=True)

        con.last_nodes_refresh = 0
        resp = con.call('srv_id')
        self.assertIs(resp.data and resp.data[0] == 1, True)
        
        # Check refresh is successful and strategy have 2 nodes
        self.assertIs(len(con.strategy.addrs), 2)
        self.srv.stop()
        
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            resp = con.call('srv_id')
        
        self.assertIs(resp.data and resp.data[0] == 2, True)

        con.close()

    @classmethod
    def tearDownClass(self):
        self.srv.stop()
        self.srv.clean()
        self.srv2.stop()
        self.srv2.clean()
