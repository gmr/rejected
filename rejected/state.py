"""
Base State Tracking Class

"""
import logging
import time

LOGGER = logging.getLogger(__name__)


class State:
    """Base class uses for managing process state"""
    STATE_INITIALIZING = 0x01
    STATE_CONNECTING = 0x02
    STATE_IDLE = 0x03
    STATE_ACTIVE = 0x04
    STATE_SLEEPING = 0x05
    STATE_STOP_REQUESTED = 0x06
    STATE_STOPPING = 0x07
    STATE_STOPPED = 0x08
    STATES = {
        0x01: 'Initializing',
        0x02: 'Connecting',
        0x03: 'Idle',
        0x04: 'Active',
        0x05: 'Sleeping',
        0x06: 'Stop Requested',
        0x07: 'Shutting down',
        0x08: 'Stopped'
    }

    def __init__(self, *args, **kwargs):
        """Initialize the state of the object"""
        super().__init__(*args, **kwargs)
        self.state = self.STATE_INITIALIZING
        self.state_start = time.time()

    @property
    def is_active(self) -> bool:
        """Indicates the current runtime state is active"""
        return self.state == self.STATE_ACTIVE

    @property
    def is_connecting(self) -> bool:
        """Indicates the current runtime state is connecting"""
        return self.state == self.STATE_CONNECTING

    @property
    def is_idle(self) -> bool:
        """Indicates the current runtime state is idle"""
        return self.state == self.STATE_IDLE

    @property
    def is_initializing(self) -> bool:
        """Indicates the current runtime state is initializing"""
        return self.state == self.STATE_INITIALIZING

    @property
    def is_running(self) -> bool:
        """Indicates the current runtime state is running"""
        return self.state in [
            self.STATE_ACTIVE, self.STATE_IDLE, self.STATE_SLEEPING
        ]

    @property
    def is_sleeping(self) -> bool:
        """Indicates the current runtime state is sleeping"""
        return self.state == self.STATE_SLEEPING

    @property
    def is_stopped(self) -> bool:
        """Indicates the current runtime state is stopped"""
        return self.state == self.STATE_STOPPED

    @property
    def is_stopping(self) -> bool:
        """Indicates the current runtime state is stopping"""
        return self.state == self.STATE_STOPPING

    @property
    def is_waiting_to_stop(self) -> bool:
        """Indicates the current runtime state is waiting to stop"""
        return self.state == self.STATE_STOP_REQUESTED

    def set_state(self, new_state: int) -> None:
        """Assign the specified state to this consumer object.

        :param new_state: The new state of the object
        :raises: ValueError

        """
        if new_state not in self.STATES:
            raise ValueError('Invalid state value: %r' % new_state)
        LOGGER.debug('State changing from %s to %s', self.STATES[self.state],
                     self.STATES[new_state])
        self.state = new_state
        self.state_start = time.time()

    @property
    def state_description(self) -> str:
        """Return the string description of our running state"""
        return self.STATES[self.state]

    @property
    def time_in_state(self) -> float:
        """Return the time that has been spent in the current state."""
        return time.time() - self.state_start
