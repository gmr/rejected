import grp
import logging
import os
import pathlib
import pwd
import sys
import types

LOGGER = logging.getLogger(__name__)


class Daemon:
    """Context manager for daemonizing the application using double-forking"""

    def __init__(self,
                 user: str | None = None,
                 group: str | None = None,
                 pid_file: str | None = None) -> None:
        """Create an instance of the Daemon class

        If the `user` specified does not match the current user, an attempt
        will be made to set the current user.

        If the `group` specified does not match the current user, an attempt
        will be made to set the current group.

        """
        self.pid_file = self.get_pid_file(pid_file)
        self.gid = self.get_gid(group)
        self.uid = self.get_uid(user)

    def __enter__(self) -> 'Daemon':
        """Context manager method to return the handle to this object"""
        try:
            self.daemonize()
        except Exception as error:
            sys.stderr.write(f'Failed to daemonize application: {str(error)}')
            sys.exit(1)
        return self

    def __exit__(self, exc_type: type[BaseException] | None,
                 exc_val: BaseException | None,
                 exc_tb: types.TracebackType | None) -> bool | None:
        """Log an error on exit if it was not clean"""
        self.pid_file.unlink(True)
        return None

    def daemonize(self):
        """Daemonize as a background process"""
        LOGGER.info('Forking %s into the background', sys.argv[0])
        self.maybe_prep_pid_file()
        self.fork()
        self.maybe_set_uid()
        self.maybe_set_gid()
        self.reset_session()
        self.fork()
        self.redirect_standard_streams()
        with self.pid_file.open('w') as handle:
            handle.write(str(os.getpid()))

    @staticmethod
    def fork() -> int:
        """Fork the process, returning the pid"""
        try:
            pid = os.fork()
        except OSError as error:
            raise RuntimeError(f'Could not fork child: {str(error)}')
        if pid > 0:
            return sys.exit(0)
        return pid

    @staticmethod
    def get_gid(group: str | None) -> int:
        """Return group id of the specified group, or current group if None"""
        if group:
            try:
                return grp.getgrnam(group).gr_gid
            except KeyError:
                raise RuntimeError(f'Group "{group}" not found')
        return os.getgid()

    @staticmethod
    def get_pid_file(pid_file: str | None) -> pathlib.Path:
        """Return a pathlib.Path instance for the specified path, or
        find a valid place to write to.

        """
        app = sys.argv[0].split('/')[-1]
        for value in [
                pid_file, f'{os.getcwd()}/pids/{app}.pid',
                f'/var/run/{app}.pid', f'/var/run/{app}/{app}.pid',
                f'/var/tmp/{app}.pid', f'/tmp/{app}.pid', f'{app}.pid'
        ]:
            if value is None:
                continue
            path = pathlib.Path(value)
            if os.access(path.parent, os.W_OK):
                return path
        raise RuntimeError('Could not find location for pid_file')

    @staticmethod
    def get_uid(username: str | None) -> int:
        """Return group id of the specified group, or current group if None"""
        if username:
            try:
                return pwd.getpwnam(username).pw_uid
            except KeyError:
                raise RuntimeError(f'User "{username}" not found')
        return os.getuid()

    def maybe_prep_pid_file(self):
        """Ensure the pid file can be written to if changing users"""
        if os.getuid() != self.uid:
            with self.pid_file.open('w') as handle:
                os.fchmod(handle.fileno(), 0o644)
                os.fchown(handle.fileno(), self.uid, self.gid)

    def maybe_set_gid(self):
        """Set the GID of the process if it differs from the desired GID"""
        if os.getgid() != self.gid:
            try:
                os.setgid(self.gid)
            except OSError as error:
                LOGGER.error('Could not set group: %s', error)

    def maybe_set_uid(self):
        """Set the UID of the process if it differs from the desired UID"""
        if os.getuid() != self.uid:
            try:
                os.setuid(self.uid)
            except OSError as error:
                raise RuntimeError(f'Could not change user: {str(error)}')

    @staticmethod
    def redirect_standard_streams() -> None:
        """Ensure the child can read/write to standard streams appropriately"""
        sys.stdout.flush()
        sys.stderr.flush()
        si = open(os.devnull)
        so = open(os.devnull, 'a+')
        se = open(os.devnull, 'a+')
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

    @staticmethod
    def reset_session():
        """Reset the POSIX session, changing cwd to / and setting a umask"""
        os.chdir('/')
        os.setsid()
        os.umask(0o022)
