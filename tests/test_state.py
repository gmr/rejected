"""Tests for the State Class"""
import mock
import time
import unittest

from rejected import state


class TestState(unittest.TestCase):
    def setUp(self):
        self._obj = state.State()

    def test_set_state_invalid_value(self):
        self.assertRaises(ValueError, self._obj.set_state, 9999)

    def test_set_state_expected_assignment(self):
        self.state = self._obj.STATE_IDLE
        self._obj.set_state(self._obj.STATE_CONNECTING)
        self.assertEqual(self._obj.state, self._obj.STATE_CONNECTING)

    def test_set_state_state_start(self):
        self.state = self._obj.STATE_IDLE
        value = 86400
        with mock.patch('time.time', return_value=value):
            self._obj.set_state(self._obj.STATE_CONNECTING)
            self.assertEqual(self._obj.state_start, value)

    def test_state_initializing_desc(self):
        self._obj.state = self._obj.STATE_INITIALIZING
        self.assertEqual(self._obj.state_description,
                         self._obj.STATES[self._obj.STATE_INITIALIZING])

    def test_state_connecting_desc(self):
        self._obj.state = self._obj.STATE_CONNECTING
        self.assertEqual(self._obj.state_description,
                         self._obj.STATES[self._obj.STATE_CONNECTING])

    def test_state_idle_desc(self):
        self._obj.state = self._obj.STATE_IDLE
        self.assertEqual(self._obj.state_description,
                         self._obj.STATES[self._obj.STATE_IDLE])

    def test_state_active_desc(self):
        self._obj.state = self._obj.STATE_ACTIVE
        self.assertEqual(self._obj.state_description,
                         self._obj.STATES[self._obj.STATE_ACTIVE])

    def test_state_stop_requested_desc(self):
        self._obj.state = self._obj.STATE_STOP_REQUESTED
        self.assertEqual(self._obj.state_description,
                         self._obj.STATES[self._obj.STATE_STOP_REQUESTED])

    def test_state_shutting_down_desc(self):
        self._obj.state = self._obj.STATE_SHUTTING_DOWN
        self.assertEqual(self._obj.state_description,
                         self._obj.STATES[self._obj.STATE_SHUTTING_DOWN])

    def test_state_stopped_desc(self):
        self._obj.state = self._obj.STATE_STOPPED
        self.assertEqual(self._obj.state_description,
                         self._obj.STATES[self._obj.STATE_STOPPED])

    def test_is_active_state_initializing(self):
        self._obj.state = self._obj.STATE_INITIALIZING
        self.assertFalse(self._obj.is_active)

    def test_is_active(self):
        self._obj.state = self._obj.STATE_ACTIVE
        self.assertTrue(self._obj.is_active)

    def test_is_connecting_state_initializing(self):
        self._obj.state = self._obj.STATE_INITIALIZING
        self.assertFalse(self._obj.is_connecting)

    def test_is_connecting(self):
        self._obj.state = self._obj.STATE_CONNECTING
        self.assertTrue(self._obj.is_connecting)

    def test_is_idle_state_initializing(self):
        self._obj.state = self._obj.STATE_INITIALIZING
        self.assertFalse(self._obj.is_idle)

    def test_is_idle_state_connecting(self):
        self._obj.state = self._obj.STATE_CONNECTING
        self.assertFalse(self._obj.is_idle)

    def test_is_idle_state_idle(self):
        self._obj.state = self._obj.STATE_IDLE
        self.assertTrue(self._obj.is_idle)

    def test_is_idle_state_processing(self):
        self._obj.state = self._obj.STATE_ACTIVE
        self.assertFalse(self._obj.is_idle)

    def test_is_idle_state_stop_requested(self):
        self._obj.state = self._obj.STATE_STOP_REQUESTED
        self.assertFalse(self._obj.is_idle)

    def test_is_idle_state_shutting_down(self):
        self._obj.state = self._obj.STATE_SHUTTING_DOWN
        self.assertFalse(self._obj.is_idle)

    def test_is_idle_state_stopped(self):
        self._obj.state = self._obj.STATE_STOPPED
        self.assertFalse(self._obj.is_idle)

    def test_is_running_state_initializing(self):
        self._obj.state = self._obj.STATE_INITIALIZING
        self.assertFalse(self._obj.is_running)

    def test_is_running_state_connecting(self):
        self._obj.state = self._obj.STATE_CONNECTING
        self.assertFalse(self._obj.is_running)

    def test_is_running_state_idle(self):
        self._obj.state = self._obj.STATE_IDLE
        self.assertTrue(self._obj.is_running)

    def test_is_running_state_processing(self):
        self._obj.state = self._obj.STATE_ACTIVE
        self.assertTrue(self._obj.is_running)

    def test_is_running_state_stop_requested(self):
        self._obj.state = self._obj.STATE_STOP_REQUESTED
        self.assertFalse(self._obj.is_running)

    def test_is_running_state_shutting_down(self):
        self._obj.state = self._obj.STATE_SHUTTING_DOWN
        self.assertFalse(self._obj.is_running)

    def test_is_running_state_stopped(self):
        self._obj.state = self._obj.STATE_STOPPED
        self.assertFalse(self._obj.is_running)

    def test_is_shutting_down_state_initializing(self):
        self._obj.state = self._obj.STATE_INITIALIZING
        self.assertFalse(self._obj.is_shutting_down)

    def test_is_shutting_down_state_connecting(self):
        self._obj.state = self._obj.STATE_CONNECTING
        self.assertFalse(self._obj.is_shutting_down)

    def test_is_shutting_down_state_idle(self):
        self._obj.state = self._obj.STATE_IDLE
        self.assertFalse(self._obj.is_shutting_down)

    def test_is_shutting_down_state_processing(self):
        self._obj.state = self._obj.STATE_ACTIVE
        self.assertFalse(self._obj.is_shutting_down)

    def test_is_shutting_down_state_stop_requested(self):
        self._obj.state = self._obj.STATE_STOP_REQUESTED
        self.assertFalse(self._obj.is_shutting_down)

    def test_is_shutting_down_state_shutting_down(self):
        self._obj.state = self._obj.STATE_SHUTTING_DOWN
        self.assertTrue(self._obj.is_shutting_down)

    def test_is_shutting_down_state_stopped(self):
        self._obj.state = self._obj.STATE_STOPPED
        self.assertFalse(self._obj.is_shutting_down)

    def test_is_sleeping_state_initializing(self):
        self._obj.state = self._obj.STATE_INITIALIZING
        self.assertFalse(self._obj.is_sleeping)

    def test_is_sleeping_state_connecting(self):
        self._obj.state = self._obj.STATE_CONNECTING
        self.assertFalse(self._obj.is_sleeping)

    def test_is_sleeping_state_idle(self):
        self._obj.state = self._obj.STATE_IDLE
        self.assertFalse(self._obj.is_sleeping)

    def test_is_sleeping_state_processing(self):
        self._obj.state = self._obj.STATE_ACTIVE
        self.assertFalse(self._obj.is_sleeping)

    def test_is_sleeping_state_stop_requested(self):
        self._obj.state = self._obj.STATE_STOP_REQUESTED
        self.assertFalse(self._obj.is_sleeping)

    def test_is_sleeping_state_shutting_down(self):
        self._obj.state = self._obj.STATE_SHUTTING_DOWN
        self.assertFalse(self._obj.is_sleeping)

    def test_is_sleeping(self):
        self._obj.state = self._obj.STATE_SLEEPING
        self.assertTrue(self._obj.is_sleeping)

    def test_is_stopped_state_initializing(self):
        self._obj.state = self._obj.STATE_INITIALIZING
        self.assertFalse(self._obj.is_stopped)

    def test_is_stopped_state_connecting(self):
        self._obj.state = self._obj.STATE_CONNECTING
        self.assertFalse(self._obj.is_stopped)

    def test_is_stopped_state_idle(self):
        self._obj.state = self._obj.STATE_IDLE
        self.assertFalse(self._obj.is_stopped)

    def test_is_stopped_state_processing(self):
        self._obj.state = self._obj.STATE_ACTIVE
        self.assertFalse(self._obj.is_stopped)

    def test_is_stopped_state_stop_requested(self):
        self._obj.state = self._obj.STATE_STOP_REQUESTED
        self.assertFalse(self._obj.is_stopped)

    def test_is_stopped_state_shutting_down(self):
        self._obj.state = self._obj.STATE_SHUTTING_DOWN
        self.assertFalse(self._obj.is_stopped)

    def test_is_waiting_to_shutdown_state_shutting_down(self):
        self._obj.state = self._obj.STATE_SHUTTING_DOWN
        self.assertFalse(self._obj.is_waiting_to_shutdown)

    def test_is_waiting_to_shutdown(self):
        self._obj.state = self._obj.STATE_STOP_REQUESTED
        self.assertTrue(self._obj.is_waiting_to_shutdown)

    def test_time_in_state(self):
        start = time.time()
        self._obj.set_state(self._obj.STATE_ACTIVE)
        time.sleep(0.5)
        self.assertAlmostEqual(self._obj.time_in_state, time.time() - start, 2)
