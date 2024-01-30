import pathlib
import tempfile
import unittest

from rejected import models


class DaemonTestCase(unittest.TestCase):

    def test_empty_values(self):
        daemon = models.Daemon()
        self.assertIsNone(daemon.user)
        self.assertIsNone(daemon.group)
        self.assertIsNone(daemon.pidfile)

    def test_valid_values(self):
        user = 'root'
        group = 'daemon'
        pidfile = str(pathlib.Path(tempfile.gettempdir()) / 'rejected.pid')
        daemon = models.Daemon(user=user, group=group, pidfile=pidfile)
        self.assertEqual(daemon.user, user)
        self.assertEqual(daemon.group, group)
        self.assertEqual(daemon.pidfile, pidfile)
