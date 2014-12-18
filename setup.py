from setuptools import setup
from rejected import __version__
import sys

classifiers = ['Development Status :: 5 - Production/Stable',
               'Intended Audience :: Developers',
               'Programming Language :: Python :: 2',
               'Programming Language :: Python :: 2.6',
               'Programming Language :: Python :: 2.7',
               'Programming Language :: Python :: Implementation :: CPython',
               'Programming Language :: Python :: Implementation :: PyPy',
               'License :: OSI Approved :: BSD License']

install_requires = ['helper',
                    'pika',
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
      long_description=open('README.rst').read(),
      classifiers=classifiers,
      keywords='amqp rabbitmq',
      author='Gavin M. Roy',
      author_email='gavinmroy@gmail.com',
      url='https://github.com/gmr/rejected',
      license=open('LICENSE').read(),
      packages=['rejected'],
      package_data={'': ['LICENSE', 'README.rst']},
      include_package_data=True,
      install_requires=install_requires,
      tests_require=['mock', 'nose', 'unittest2'],
      entry_points=dict(console_scripts=['rejected=rejected.controller:main']),
      zip_safe=True)
