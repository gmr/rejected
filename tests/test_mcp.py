"""Tests for the MCP"""
import mock
import multiprocessing

from helper import config
from rejected import mcp
from . import test_state

class TestMCP(test_state.TestState):

    CONFIG = {'poll_interval': 30.0, 'log_stats': True}

    @mock.patch.object(multiprocessing, 'Queue')
    def setUp(self, mock_queue_unused):

        self.cfg = config.Config()
        self.cfg.application.update(self.CONFIG)
        self._obj = mcp.MasterControlProgram(self.cfg)

    def test_mcp_init_consumers_dict(self):
        self.assertIsInstance(self._obj._consumers, dict)

    def test_mcp_init_consumers_dict_empty(self):
        self.assertTrue(not self._obj._consumers, dict)

    def test_mcp_init_queue_initialized(self):
        self.assertIsInstance(self._obj._stats_queue, mock.MagicMock)

