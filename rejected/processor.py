"""
A consumer processing client base class

"""
__author__ = 'Gavin M. Roy'
__email__ = 'gmr@myyearbook.com'
__since__ = '2011-07-27'

import logging


class Processor(object):

    def __init__(self):

        # Setup the logger
        self._logger = logging.getLogger('rejected.processor.%s' %
                                         self.__class__.__name__)




    def _start_event(self, event_name):

        pass

    def _finish_event(self, event_name):

        pass

    def get_stats(self):
        pass
