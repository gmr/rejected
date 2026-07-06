"""Tests for the MCP"""

import collections
import multiprocessing
import typing
import unittest
from unittest import mock

from rejected import config as config_module
from rejected import mcp

from . import test_state


class TestMCP(test_state.TestState):
    CONFIG: typing.ClassVar[dict] = {'poll_interval': 30.0, 'Consumers': {}}

    @mock.patch.object(multiprocessing, 'Queue')
    def setUp(self, _mock_queue_unused):
        self.cfg = config_module.Config.model_validate(self.CONFIG)
        self._obj = mcp.MasterControlProgram(self.cfg)

    def test_mcp_init_consumers_dict(self):
        self.assertIsInstance(self._obj.consumers, dict)

    def test_mcp_init_consumers_dict_empty(self):
        self.assertTrue(not self._obj.consumers, dict)

    def test_mcp_init_queue_initialized(self):
        self.assertIsInstance(self._obj.stats_queue, mock.MagicMock)


class MCPTestCase(unittest.TestCase):
    """Base for MCP behavior tests that need a constructed MCP instance."""

    CONFIG: typing.ClassVar[dict] = {'poll_interval': 30.0, 'Consumers': {}}

    @mock.patch.object(multiprocessing, 'Queue')
    def setUp(self, _mock_queue_unused):
        self.cfg = config_module.Config.model_validate(self.CONFIG)
        self._obj = mcp.MasterControlProgram(self.cfg)


class OnSigchldTests(MCPTestCase):
    """#76 — on_sigchld should only stop the daemon on abort/max_messages/
    shutdown, otherwise prune and let the next poll respawn."""

    def _no_active(self):
        return mock.patch.object(
            self._obj, 'active_processes', return_value=[]
        )

    @mock.patch('rejected.mcp.signal.setitimer')
    def test_respawn_path_does_not_stop(self, mock_setitimer):
        self._obj.state = self._obj.STATE_ACTIVE
        with self._no_active():
            self._obj.on_sigchld(0, None)
        self.assertEqual(self._obj.state, self._obj.STATE_ACTIVE)
        mock_setitimer.assert_not_called()

    @mock.patch('rejected.mcp.signal.setitimer')
    def test_child_abort_stops(self, mock_setitimer):
        self._obj.state = self._obj.STATE_ACTIVE
        self._obj.child_abort = True
        with self._no_active():
            self._obj.on_sigchld(0, None)
        self.assertEqual(self._obj.state, self._obj.STATE_STOPPED)
        mock_setitimer.assert_called_once()

    @mock.patch('rejected.mcp.signal.setitimer')
    def test_max_messages_stops(self, mock_setitimer):
        self._obj.state = self._obj.STATE_ACTIVE
        self._obj.max_messages = 10
        with self._no_active():
            self._obj.on_sigchld(0, None)
        self.assertEqual(self._obj.state, self._obj.STATE_STOPPED)
        mock_setitimer.assert_called_once()

    @mock.patch('rejected.mcp.signal.setitimer')
    def test_active_processes_no_stop(self, mock_setitimer):
        self._obj.state = self._obj.STATE_ACTIVE
        with mock.patch.object(
            self._obj, 'active_processes', return_value=[mock.Mock()]
        ):
            self._obj.on_sigchld(0, None)
        self.assertEqual(self._obj.state, self._obj.STATE_ACTIVE)
        mock_setitimer.assert_not_called()


class StartProcessAbortTests(MCPTestCase):
    """#76 — a failed spawn sets child_abort."""

    def test_start_process_sets_child_abort_on_oserror(self):
        self._obj.consumers['foo'] = mcp.Consumer(0, {}, 1, 'foo')
        with mock.patch.object(self._obj, 'new_process') as new_process:
            proc = mock.Mock()
            proc.start.side_effect = OSError('nope')
            new_process.return_value = ('foo-1', proc)
            self._obj.start_process('foo')
        self.assertTrue(self._obj.child_abort)


class ProcessCountGuardTests(MCPTestCase):
    """#91 — respawn paths must bail when not running."""

    def setUp(self):
        super().setUp()
        self._obj.consumers['foo'] = mcp.Consumer(0, {}, 1, 'foo')

    def test_check_process_counts_bails_when_not_running(self):
        self._obj.state = self._obj.STATE_SHUTTING_DOWN
        with mock.patch.object(self._obj, 'start_processes') as start:
            self._obj.check_process_counts()
        start.assert_not_called()

    def test_check_process_counts_runs_when_active(self):
        self._obj.state = self._obj.STATE_ACTIVE
        with mock.patch.object(self._obj, 'start_processes') as start:
            self._obj.check_process_counts()
        start.assert_called_once_with('foo', 1)

    def test_start_processes_bails_when_not_running(self):
        self._obj.state = self._obj.STATE_SHUTTING_DOWN
        with mock.patch.object(self._obj, 'start_process') as start_one:
            self._obj.start_processes('foo', 2)
        start_one.assert_not_called()


class CalculateStatsTests(MCPTestCase):
    """#92 — no input mutation, no aliasing, dead-process baselines."""

    def setUp(self):
        super().setUp()
        self._obj.consumers['foo'] = mcp.Consumer(0, {}, 1, 'foo')

    @staticmethod
    def _data():
        return {
            'timestamp': 123.0,
            'foo': {'foo-1': {'counts': {'processed': 5, 'acked': 5}}},
        }

    def test_does_not_mutate_input_timestamp(self):
        data = self._data()
        self._obj.calculate_stats(data)
        self.assertIn('timestamp', data)

    def test_process_data_not_aliased(self):
        data = self._data()
        stats = self._obj.calculate_stats(data)
        self.assertIsNot(stats['process_data'], data)
        self.assertNotIn('timestamp', stats['process_data'])

    def test_sums_live_process_counts(self):
        stats = self._obj.calculate_stats(self._data())
        self.assertEqual(stats['consumers']['foo']['processed'], 5)

    def test_folds_in_baseline(self):
        self._obj.consumer_baselines['foo'] = collections.Counter(
            {'processed': 10}
        )
        data = {
            'timestamp': 1.0,
            'foo': {'foo-2': {'counts': {'processed': 3}}},
        }
        stats = self._obj.calculate_stats(data)
        self.assertEqual(stats['consumers']['foo']['processed'], 13)


class RetirePollResultsTests(MCPTestCase):
    """#92 — retiring a process prunes its entry and folds its counts."""

    def test_folds_counts_and_prunes(self):
        self._obj.last_poll_results = {
            'foo': {'foo-1': {'counts': {'processed': 7}}}
        }
        self._obj.retire_poll_results('foo', 'foo-1')
        self.assertNotIn('foo-1', self._obj.last_poll_results['foo'])
        self.assertEqual(self._obj.consumer_baselines['foo']['processed'], 7)

    def test_accumulates_across_retirements(self):
        self._obj.last_poll_results = {
            'foo': {
                'foo-1': {'counts': {'processed': 7}},
                'foo-2': {'counts': {'processed': 4}},
            }
        }
        self._obj.retire_poll_results('foo', 'foo-1')
        self._obj.retire_poll_results('foo', 'foo-2')
        self.assertEqual(self._obj.consumer_baselines['foo']['processed'], 11)

    def test_missing_entry_is_noop(self):
        self._obj.last_poll_results = {'foo': {}}
        self._obj.retire_poll_results('foo', 'foo-1')
        self.assertNotIn('foo', self._obj.consumer_baselines)
