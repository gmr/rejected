from setuptools import setup
from rejected import __version__
import sys

long_description = """\
Rejected is a RabbitMQ consumer framework and controller daemon that allows you
to focus on the development of the code that handles the messages and not the
code that facilitates the communication with RabbitMQ."""

classifiers = ['Development Status :: 5 - Production/Stable',
               'Intended Audience :: Developers',
               'Programming Language :: Python :: 2',
               'Programming Language :: Python :: 2.6',
               'Programming Language :: Python :: 2.7',
               'Programming Language :: Python :: Implementation :: CPython',
               'Programming Language :: Python :: Implementation :: PyPy',
               'License :: OSI Approved :: BSD License']

install_requires = ['helper',
                    'pika>=0.9.14',
                    'psutil',
                    'pyyaml',
                    'tornado']

extras_require = {'html': ['beautifulsoup4']}

if sys.version_info < (2, 7, 0):
    install_requires.append('importlib')

setup(name='rejected',
      version=__version__,
      description='Rejected is a Python RabbitMQ Consumer Framework and '
                  'Controller Daemon',
      long_description=long_description,
      classifiers=classifiers,
      keywords='amqp rabbitmq',
      author='Gavin M. Roy',
      author_email='gmr@meetme.com',
      url='http://github.com/gmr/rejected',
      license='BSD',
      packages=['rejected'],
      install_requires=install_requires,
      tests_require=['mock', 'nose', 'unittest2'],
      entry_points=dict(console_scripts=['rejected=rejected.controller:main']),
      zip_safe=True)
