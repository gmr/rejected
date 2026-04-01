"""Tests for rejected.controller"""

import argparse
import signal
import sys
import unittest
from unittest import mock

import rejected.controller
from rejected import config as config_module


def _make_args(**kwargs):
    defaults = {
        'config': '/etc/rejected/config.yaml',
        'consumer': None,
        'profile': None,
        'prepend_path': None,
        'quantity': None,
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def _make_config(**kwargs):
    return config_module.Config.model_validate(kwargs)


class ControllerInitTests(unittest.TestCase):
    def test_defaults(self):
        ctrl = rejected.controller.Controller(_make_args(), _make_config())
        self.assertIsNone(ctrl._mcp)
        self.assertFalse(ctrl._reload_requested)
        self.assertFalse(ctrl._shutdown_requested)
        self.assertFalse(ctrl._sentry_client)

    def test_stores_args_and_config(self):
        args = _make_args()
        cfg = _make_config()
        ctrl = rejected.controller.Controller(args, cfg)
        self.assertIs(ctrl.args, args)
        self.assertIs(ctrl.config, cfg)


class ControllerSignalTests(unittest.TestCase):
    def setUp(self):
        self.ctrl = rejected.controller.Controller(
            _make_args(), _make_config()
        )
        self.ctrl._mcp = mock.Mock()

    def test_on_sigterm_sets_shutdown_requested(self):
        self.ctrl._on_sigterm(signal.SIGTERM, None)
        self.assertTrue(self.ctrl._shutdown_requested)

    def test_on_sigterm_calls_stop_processes(self):
        self.ctrl._on_sigterm(signal.SIGTERM, None)
        self.ctrl._mcp.stop_processes.assert_called_once()

    def test_on_sigterm_no_mcp(self):
        self.ctrl._mcp = None
        self.ctrl._on_sigterm(signal.SIGTERM, None)  # should not raise
        self.assertTrue(self.ctrl._shutdown_requested)

    def test_on_sighup_sets_reload_requested(self):
        self.ctrl._on_sighup(signal.SIGHUP, None)
        self.assertTrue(self.ctrl._reload_requested)

    def test_on_sighup_calls_stop_processes(self):
        self.ctrl._on_sighup(signal.SIGHUP, None)
        self.ctrl._mcp.stop_processes.assert_called_once()

    def test_on_sighup_no_mcp(self):
        self.ctrl._mcp = None
        self.ctrl._on_sighup(signal.SIGHUP, None)  # should not raise
        self.assertTrue(self.ctrl._reload_requested)

    def test_on_sighup_does_not_set_shutdown_requested(self):
        self.ctrl._on_sighup(signal.SIGHUP, None)
        self.assertFalse(self.ctrl._shutdown_requested)


class ControllerRunTests(unittest.TestCase):
    def setUp(self):
        self.ctrl = rejected.controller.Controller(
            _make_args(), _make_config()
        )

    def test_normal_run_starts_mcp_once(self):
        mock_mcp = mock.Mock()
        with mock.patch(
            'rejected.controller.mcp.MasterControlProgram',
            return_value=mock_mcp,
        ):
            with mock.patch.object(self.ctrl, '_setup_signals'):
                self.ctrl.run()
        mock_mcp.run.assert_called_once()

    def test_prepend_path_inserted(self):
        self.ctrl.args.prepend_path = '/some/path'
        mock_mcp = mock.Mock()
        original_path = sys.path[:]
        with mock.patch(
            'rejected.controller.mcp.MasterControlProgram',
            return_value=mock_mcp,
        ):
            with mock.patch.object(self.ctrl, '_setup_signals'):
                self.ctrl.run()
        self.assertEqual(sys.path[0], '/some/path')
        sys.path[:] = original_path  # restore

    def test_keyboard_interrupt_exits_cleanly(self):
        mock_mcp = mock.Mock()
        mock_mcp.run.side_effect = KeyboardInterrupt
        with mock.patch(
            'rejected.controller.mcp.MasterControlProgram',
            return_value=mock_mcp,
        ):
            with mock.patch.object(self.ctrl, '_setup_signals'):
                self.ctrl.run()  # should not raise

    def test_exception_propagates(self):
        mock_mcp = mock.Mock()
        mock_mcp.run.side_effect = RuntimeError('boom')
        with mock.patch(
            'rejected.controller.mcp.MasterControlProgram',
            return_value=mock_mcp,
        ):
            with mock.patch.object(self.ctrl, '_setup_signals'):
                with self.assertRaises(RuntimeError):
                    self.ctrl.run()

    def test_shutdown_requested_skips_loop(self):
        self.ctrl._shutdown_requested = True
        with mock.patch(
            'rejected.controller.mcp.MasterControlProgram'
        ) as mock_cls:
            with mock.patch.object(self.ctrl, '_setup_signals'):
                self.ctrl.run()
        mock_cls.assert_not_called()


class ControllerReloadTests(unittest.TestCase):
    def setUp(self):
        self.cfg = _make_config()
        self.ctrl = rejected.controller.Controller(_make_args(), self.cfg)

    def _run_with_reload(self, new_cfg=None, load_raises=None):
        """Run the controller, simulate a SIGHUP on the first MCP run."""
        call_count = 0
        new_cfg = new_cfg or _make_config(poll_interval=30.0)

        def fake_mcp_run():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                self.ctrl._reload_requested = True

        mock_mcp = mock.Mock()
        mock_mcp.run.side_effect = fake_mcp_run

        if load_raises is not None:
            load_target = mock.patch(
                'rejected.controller.config_module.load',
                side_effect=load_raises,
            )
        else:
            load_target = mock.patch(
                'rejected.controller.config_module.load',
                return_value=new_cfg,
            )

        with mock.patch(
            'rejected.controller.mcp.MasterControlProgram',
            return_value=mock_mcp,
        ):
            with mock.patch.object(self.ctrl, '_setup_signals'):
                with load_target:
                    self.ctrl.run()

        return mock_mcp, call_count

    def test_reload_starts_second_mcp(self):
        _, call_count = self._run_with_reload()
        self.assertEqual(call_count, 2)

    def test_reload_updates_config(self):
        new_cfg = _make_config(poll_interval=99.0)
        self._run_with_reload(new_cfg=new_cfg)
        self.assertEqual(self.ctrl.config.poll_interval, 99.0)

    def test_reload_file_not_found_keeps_previous_config(self):
        self._run_with_reload(load_raises=FileNotFoundError('gone'))
        self.assertIs(self.ctrl.config, self.cfg)

    def test_reload_invalid_config_keeps_previous_config(self):
        self._run_with_reload(load_raises=ValueError('bad yaml'))
        self.assertIs(self.ctrl.config, self.cfg)

    def test_reload_still_restarts_after_load_failure(self):
        _, call_count = self._run_with_reload(
            load_raises=FileNotFoundError('gone')
        )
        self.assertEqual(call_count, 2)

    def test_reload_flag_cleared_each_iteration(self):
        self._run_with_reload()
        self.assertFalse(self.ctrl._reload_requested)
