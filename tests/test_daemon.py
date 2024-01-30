import grp
import os
import pathlib
import pwd
import tempfile
import unittest
import uuid
from unittest import mock

from rejected import daemon


class TestCase(unittest.TestCase):

    def setUp(self) -> None:
        self.pid_file = \
            pathlib.Path(tempfile.gettempdir()) / f'{os.getpid()}.pid'

    def tearDown(self) -> None:
        self.pid_file.unlink(True)

    @mock.patch('rejected.daemon.Daemon.daemonize')
    def test_context_manager(self, daemonize) -> None:
        with daemon.Daemon():
            daemonize.assert_called_once()

    @mock.patch('rejected.daemon.Daemon.daemonize')
    @mock.patch('sys.exit')
    def test_context_manager_error(self, sys_exit, daemonize) -> None:
        daemonize.side_effect = OSError('Permission Denied')
        with daemon.Daemon():
            sys_exit.assert_called_once_with(1)

    @mock.patch('rejected.daemon.Daemon.fork')
    @mock.patch('rejected.daemon.Daemon.redirect_standard_streams')
    @mock.patch('rejected.daemon.Daemon.reset_session')
    def test_daemonize(self, reset_session, redirect_standard_streams,
                       fork) -> None:
        instance = daemon.Daemon(pid_file=str(self.pid_file))
        instance.daemonize()
        self.assertEqual(fork.call_count, 2)
        redirect_standard_streams.assert_called_once()
        reset_session.assert_called_once()
        with self.pid_file.open('r') as handle:
            pid = int(handle.read())
        self.assertEqual(os.getpid(), pid)

    @mock.patch('os.fork')
    @mock.patch('sys.exit')
    def test_fork(self, sys_exit, fork) -> None:
        fork.return_value = 0
        value = daemon.Daemon.fork()
        fork.assert_called_once()
        sys_exit.assert_not_called()
        self.assertEqual(value, 0)

    @mock.patch('os.fork')
    def test_fork_done(self, fork) -> None:
        fork.side_effect = OSError('mock error')
        with self.assertRaises(RuntimeError):
            daemon.Daemon.fork()

    @mock.patch('os.fork')
    @mock.patch('sys.exit')
    def test_fork_in_parent(self, sys_exit, fork) -> None:
        fork.return_value = os.getpid()
        daemon.Daemon.fork()
        fork.assert_called_once()
        sys_exit.assert_called_once()

    @mock.patch('os.fork')
    def test_fork_error(self, fork) -> None:
        fork.side_effect = OSError('mock error')
        with self.assertRaises(RuntimeError):
            daemon.Daemon.fork()

    def test_initialization_with_values(self) -> None:
        user = 'root'
        group = 'daemon'
        instance = daemon.Daemon(user, group, str(self.pid_file))
        self.assertEqual(instance.gid, grp.getgrnam(group).gr_gid)
        self.assertEqual(instance.uid, pwd.getpwnam(user).pw_uid)
        self.assertEqual(instance.pid_file, self.pid_file)

    def test_get_gid_root(self) -> None:
        self.assertGreaterEqual(daemon.Daemon.get_gid('tty'), 0)

    def test_get_gid_not_found(self) -> None:
        with self.assertRaises(RuntimeError):
            daemon.Daemon.get_gid(str(uuid.uuid4()))

    def test_get_gid_current_user(self) -> None:
        self.assertEqual(daemon.Daemon.get_gid(None), os.getgid())

    def test_get_uid_root(self) -> None:
        self.assertEqual(daemon.Daemon.get_uid('root'), 0)

    def test_get_uid_not_found(self) -> None:
        with self.assertRaises(RuntimeError):
            daemon.Daemon.get_uid(str(uuid.uuid4()))

    def test_get_uid_current_user(self) -> None:
        self.assertEqual(daemon.Daemon.get_uid(None), os.getuid())

    @mock.patch('os.access')
    def test_get_pid_file_no_access_raises(self, access):
        access.return_value = False
        with self.assertRaises(RuntimeError):
            daemon.Daemon.get_pid_file(None)

    @mock.patch('os.fchmod')
    @mock.patch('os.fchown')
    def test_maybe_prep_pid_file_current_user(self, fchmod, fchown) -> None:
        instance = daemon.Daemon()
        instance.maybe_prep_pid_file()
        fchmod.assert_not_called()
        fchown.assert_not_called()

    @mock.patch('os.fchmod')
    @mock.patch('os.fchown')
    def test_maybe_prep_pid_file_diff_user(self, fchown, fchmod) -> None:
        instance = daemon.Daemon()
        with mock.patch('os.getuid') as getuid:
            getuid.return_value = 0
            instance.maybe_prep_pid_file()
            if os.getuid() != 0:
                fchmod.assert_called_once()
                fchown.assert_called_once()

    @mock.patch('os.setgid')
    def test_maybe_set_gid_noop(self, setgid):
        instance = daemon.Daemon()
        instance.maybe_set_gid()
        setgid.assert_not_called()

    @mock.patch('os.setgid')
    def test_maybe_set_gid_invokes_setgid(self, setgid):
        instance = daemon.Daemon(group='daemon')
        instance.maybe_set_gid()
        setgid.assert_called_once_with(grp.getgrnam('daemon').gr_gid)

    @mock.patch('os.setgid')
    def test_maybe_set_gid_doesnt_raise_on_oserror(self, setgid):
        setgid.side_effect = OSError('Permission denied')
        instance = daemon.Daemon(group='daemon')
        instance.maybe_set_gid()
        setgid.assert_called_once_with(grp.getgrnam('daemon').gr_gid)

    @mock.patch('os.setuid')
    def test_maybe_set_uid_noop(self, setuid):
        instance = daemon.Daemon()
        instance.maybe_set_uid()
        setuid.assert_not_called()

    @mock.patch('os.setuid')
    def test_maybe_set_uid_invokes_setuid(self, setuid):
        instance = daemon.Daemon(user='root')
        instance.maybe_set_uid()
        if os.getuid() != 0:
            setuid.assert_called_once_with(pwd.getpwnam('root').pw_uid)

    @mock.patch('os.setuid')
    def test_maybe_set_uid_raises_on_oserror(self, setuid):
        setuid.side_effect = OSError('Permission denied')
        instance = daemon.Daemon(user='root')
        if os.getuid() != 0:
            with self.assertRaises(RuntimeError):
                instance.maybe_set_uid()
        else:
            instance.maybe_set_uid()

    @mock.patch('sys.stdout.flush')
    @mock.patch('sys.stderr.flush')
    @mock.patch('os.dup2')
    def test_redirect_stander_streams(self, dup2, flush1, flush2) -> None:
        with mock.patch('builtins.open') as mock_open:
            daemon.Daemon().redirect_standard_streams()
            self.assertEqual(dup2.call_count, 3)
            self.assertEqual(mock_open.call_count, 3)
            flush1.assert_called_once()
            flush2.assert_called_once()

    @mock.patch('os.chdir')
    @mock.patch('os.setsid')
    @mock.patch('os.umask')
    def test_reset_session(self, umask, setsid, chdir) -> None:
        daemon.Daemon().reset_session()
        chdir.assert_called_once_with('/')
        setsid.assert_called_once_with()
        umask.assert_called_once_with(0o022)
