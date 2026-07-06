"""Base State Tracking Class"""

import logging
import time
import typing

LOGGER = logging.getLogger(__name__)


class State:
    """Class that is to be extended by MCP and process for maintaining the
    internal state of the application.

    """

    STATE_INITIALIZING: typing.ClassVar[int] = 0x01
    STATE_CONNECTING: typing.ClassVar[int] = 0x02
    STATE_IDLE: typing.ClassVar[int] = 0x03
    STATE_ACTIVE: typing.ClassVar[int] = 0x04
    STATE_SLEEPING: typing.ClassVar[int] = 0x05
    STATE_STOP_REQUESTED: typing.ClassVar[int] = 0x06
    STATE_SHUTTING_DOWN: typing.ClassVar[int] = 0x07
    STATE_STOPPED: typing.ClassVar[int] = 0x08

    STATES: typing.ClassVar[dict[int, str]] = {
        0x01: 'Initializing',
        0x02: 'Connecting',
        0x03: 'Idle',
        0x04: 'Active',
        0x05: 'Sleeping',
        0x06: 'Stop Requested',
        0x07: 'Shutting down',
        0x08: 'Stopped',
    }

    def __init__(self) -> None:
        self.state: int = self.STATE_INITIALIZING
        self.state_start: float = time.time()

    def set_state(self, new_state: int) -> None:
        """Assign the specified state to this consumer object.

        :param int new_state: The new state of the object
        :raises: ValueError

        """
        if new_state not in self.STATES:
            raise ValueError(f'Invalid state value: {new_state!r}')

        # A stopped object must not be resurrected into an active state
        if self.state == self.STATE_STOPPED and new_state == self.STATE_ACTIVE:
            raise ValueError('Cannot transition from Stopped to Active')

        LOGGER.debug(
            'State changing from %s to %s',
            self.STATES[self.state],
            self.STATES[new_state],
        )
        self.state = new_state
        self.state_start = time.time()

    @property
    def is_active(self) -> bool:
        return self.state == self.STATE_ACTIVE

    @property
    def is_connecting(self) -> bool:
        return self.state == self.STATE_CONNECTING

    @property
    def is_idle(self) -> bool:
        return self.state == self.STATE_IDLE

    @property
    def is_running(self) -> bool:
        return self.state in [
            self.STATE_IDLE,
            self.STATE_ACTIVE,
            self.STATE_SLEEPING,
        ]

    @property
    def is_shutting_down(self) -> bool:
        return self.state == self.STATE_SHUTTING_DOWN

    @property
    def is_sleeping(self) -> bool:
        return self.state == self.STATE_SLEEPING

    @property
    def is_stopped(self) -> bool:
        return self.state == self.STATE_STOPPED

    @property
    def is_waiting_to_shutdown(self) -> bool:
        return self.state == self.STATE_STOP_REQUESTED

    @property
    def state_description(self) -> str:
        return self.STATES[self.state]

    @property
    def time_in_state(self) -> float:
        return time.time() - self.state_start
