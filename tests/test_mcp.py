"""Tests for the MCP"""
import multiprocessing

try:
    from unittest import mock
except ImportError:
    import mock

from helper import config
from rejected import mcp

from . import test_state


class TestMCP(test_state.TestState):

    CONFIG = {'poll_interval': 30.0, 'log_stats': True, 'Consumers': {}}

    @mock.patch.object(multiprocessing, 'Queue')
    def setUp(self, _mock_queue_unused):
        self.cfg = config.Config()
        self.cfg.application.update(self.CONFIG)
        self._obj = mcp.MasterControlProgram(self.cfg)

    def test_mcp_init_consumers_dict(self):
        self.assertIsInstance(self._obj.consumers, dict)

    def test_mcp_init_consumers_dict_empty(self):
        self.assertTrue(not self._obj.consumers, dict)

    def test_mcp_init_queue_initialized(self):
        self.assertIsInstance(self._obj.stats_queue, mock.MagicMock)
