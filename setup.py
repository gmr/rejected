from setuptools import setup
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
                    'pika>=0.10.0',
                    'psutil',
                    'pyyaml',
                    'tornado>=4.2.0']

extras_require = {'html': ['beautifulsoup4'],
                  'msgpack': ['msgpack-python']}

if sys.version_info < (2, 7, 0):
    install_requires.append('backport_collections')
    install_requires.append('importlib')

setup(name='rejected',
      version='3.9.3',
      description='Rejected is a Python RabbitMQ Consumer Framework and '
                  'Controller Daemon',
      long_description=open('README.rst').read(),
      classifiers=classifiers,
      keywords='amqp rabbitmq',
      author='Gavin M. Roy',
      author_email='gavinmroy@gmail.com',
      url='https://github.com/gmr/rejected',
      license='BSD',
      packages=['rejected'],
      package_data={'': ['LICENSE', 'README.rst']},
      include_package_data=True,
      install_requires=install_requires,
      tests_require=['mock', 'nose', 'unittest2'],
      entry_points=dict(console_scripts=['rejected=rejected.controller:main']),
      zip_safe=True)
