import unittest

from rejected import utils


class TestImportNamspacedClass(unittest.TestCase):

    def test_import_consumer(self):
        import logging
        (result_class,
         result_version) = utils.import_consumer('logging.Logger')
        self.assertEqual(result_class, logging.Logger)

    def test_import_consumer_version(self):
        import logging
        (result_class,
         result_version) = utils.import_consumer('logging.Logger')
        self.assertEqual(result_version, logging.__version__)

    def test_import_consumer_no_version(self):
        (result_class,
         result_version) = utils.import_consumer('signal.ItimerError')
        self.assertIsNone(result_version)

    def test_import_consumer_failure(self):
        self.assertRaises(ImportError, utils.import_consumer,
                          'rejected.fake_module.Classname')
