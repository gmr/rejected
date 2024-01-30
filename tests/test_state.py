"""Tests for the State Class"""
import unittest
from unittest import mock

from rejected import state


class TestState(unittest.TestCase):

    def setUp(self):
        self.instance = state.State()

    def test_set_state_invalid_value(self):
        self.assertRaises(ValueError, self.instance.set_state, 9999)

    def test_set_state_expected_assignment(self):
        self.state = self.instance.STATE_IDLE
        self.instance.set_state(self.instance.STATE_CONNECTING)
        self.assertEqual(self.instance.state, self.instance.STATE_CONNECTING)

    def test_set_state_state_start(self):
        self.state = self.instance.STATE_IDLE
        value = 86400
        with mock.patch('time.time', return_value=value):
            self.instance.set_state(self.instance.STATE_CONNECTING)
            self.assertEqual(self.instance.state_start, value)

    def test_state_initializing(self):
        self.instance.state = self.instance.STATE_INITIALIZING
        self.assertEqual(
            self.instance.state_description,
            self.instance.STATES[self.instance.STATE_INITIALIZING])
        self.assertTrue(self.instance.is_initializing)

    def test_state_connecting(self):
        self.instance.state = self.instance.STATE_CONNECTING
        self.assertEqual(self.instance.state_description,
                         self.instance.STATES[self.instance.STATE_CONNECTING])
        self.assertTrue(self.instance.is_connecting)

    def test_state_idle(self):
        self.instance.state = self.instance.STATE_IDLE
        self.assertEqual(self.instance.state_description,
                         self.instance.STATES[self.instance.STATE_IDLE])
        self.assertTrue(self.instance.is_idle)

    def test_state_sleeping(self):
        self.instance.state = self.instance.STATE_SLEEPING
        self.assertEqual(self.instance.state_description,
                         self.instance.STATES[self.instance.STATE_SLEEPING])
        self.assertTrue(self.instance.is_sleeping)

    def test_state_active(self):
        self.instance.state = self.instance.STATE_ACTIVE
        self.assertEqual(self.instance.state_description,
                         self.instance.STATES[self.instance.STATE_ACTIVE])
        self.assertTrue(self.instance.is_active)

    def test_state_stop_requested(self):
        self.instance.state = self.instance.STATE_STOP_REQUESTED
        self.assertEqual(
            self.instance.state_description,
            self.instance.STATES[self.instance.STATE_STOP_REQUESTED])
        self.assertTrue(self.instance.is_waiting_to_stop)

    def test_state_stopping(self):
        self.instance.state = self.instance.STATE_STOPPING
        self.assertEqual(self.instance.state_description,
                         self.instance.STATES[self.instance.STATE_STOPPING])
        self.assertTrue(self.instance.is_stopping)

    def test_state_stopped(self):
        self.instance.state = self.instance.STATE_STOPPED
        self.assertEqual(self.instance.state_description,
                         self.instance.STATES[self.instance.STATE_STOPPED])
        self.assertTrue(self.instance.is_stopped)

    def test_is_idle_state_initializing(self):
        self.instance.state = self.instance.STATE_INITIALIZING
        self.assertFalse(self.instance.is_idle)

    def test_is_idle_state_connecting(self):
        self.instance.state = self.instance.STATE_CONNECTING
        self.assertFalse(self.instance.is_idle)

    def test_is_idle_state_idle(self):
        self.instance.state = self.instance.STATE_IDLE
        self.assertTrue(self.instance.is_idle)

    def test_is_idle_state_processing(self):
        self.instance.state = self.instance.STATE_ACTIVE
        self.assertFalse(self.instance.is_idle)

    def test_is_idle_state_stop_requested(self):
        self.instance.state = self.instance.STATE_STOP_REQUESTED
        self.assertFalse(self.instance.is_idle)

    def test_is_idle_state_stopping(self):
        self.instance.state = self.instance.STATE_STOPPING
        self.assertFalse(self.instance.is_idle)

    def test_is_idle_state_stopped(self):
        self.instance.state = self.instance.STATE_STOPPED
        self.assertFalse(self.instance.is_idle)

    def test_is_running_state_initializing(self):
        self.instance.state = self.instance.STATE_INITIALIZING
        self.assertFalse(self.instance.is_running)

    def test_is_running_state_connecting(self):
        self.instance.state = self.instance.STATE_CONNECTING
        self.assertFalse(self.instance.is_running)

    def test_is_running_state_idle(self):
        self.instance.state = self.instance.STATE_IDLE
        self.assertTrue(self.instance.is_running)

    def test_is_running_state_processing(self):
        self.instance.state = self.instance.STATE_ACTIVE
        self.assertTrue(self.instance.is_running)

    def test_is_running_state_stop_requested(self):
        self.instance.state = self.instance.STATE_STOP_REQUESTED
        self.assertFalse(self.instance.is_running)

    def test_is_running_state_stopping(self):
        self.instance.state = self.instance.STATE_STOPPING
        self.assertFalse(self.instance.is_running)

    def test_is_running_state_stopped(self):
        self.instance.state = self.instance.STATE_STOPPED
        self.assertFalse(self.instance.is_running)

    def test_is_stopping_state_initializing(self):
        self.instance.state = self.instance.STATE_INITIALIZING
        self.assertFalse(self.instance.is_stopping)

    def test_is_stopping_state_connecting(self):
        self.instance.state = self.instance.STATE_CONNECTING
        self.assertFalse(self.instance.is_stopping)

    def test_is_stopping_state_idle(self):
        self.instance.state = self.instance.STATE_IDLE
        self.assertFalse(self.instance.is_stopping)

    def test_is_stopping_state_processing(self):
        self.instance.state = self.instance.STATE_ACTIVE
        self.assertFalse(self.instance.is_stopping)

    def test_is_stopping_state_stop_requested(self):
        self.instance.state = self.instance.STATE_STOP_REQUESTED
        self.assertFalse(self.instance.is_stopping)

    def test_is_stopping_state_stopping(self):
        self.instance.state = self.instance.STATE_STOPPING
        self.assertTrue(self.instance.is_stopping)

    def test_is_stopping_state_stopped(self):
        self.instance.state = self.instance.STATE_STOPPED
        self.assertFalse(self.instance.is_stopping)

    def test_is_stopped_state_initializing(self):
        self.instance.state = self.instance.STATE_INITIALIZING
        self.assertFalse(self.instance.is_stopped)

    def test_is_stopped_state_connecting(self):
        self.instance.state = self.instance.STATE_CONNECTING
        self.assertFalse(self.instance.is_stopped)

    def test_is_stopped_state_idle(self):
        self.instance.state = self.instance.STATE_IDLE
        self.assertFalse(self.instance.is_stopped)

    def test_is_stopped_state_processing(self):
        self.instance.state = self.instance.STATE_ACTIVE
        self.assertFalse(self.instance.is_stopped)

    def test_is_stopped_state_stop_requested(self):
        self.instance.state = self.instance.STATE_STOP_REQUESTED
        self.assertFalse(self.instance.is_stopped)

    def test_is_stopped_state_stopping(self):
        self.instance.state = self.instance.STATE_STOPPING
        self.assertFalse(self.instance.is_stopped)

    def test_time_in_state(self):
        self.instance.set_state(self.instance.STATE_CONNECTING)
        self.assertLessEqual(self.instance.time_in_state, 1)
