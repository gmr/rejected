import sys
from setuptools import setup

version = "2.0.0"

long_description = """\
rejected is a consumer daemon and development framework for RabbitMQ designed
to let you focus on what to do with your messages.
"""
setup(name='rejected',
      version=version,
      description="rejected consumer daemon and development framework",
      long_description=long_description,
      classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
      ],
      keywords='rabbitmq consumer',
      author='Gavin M. Roy',
      author_email='gmr@myyearbook.com',
      url='http://github.com/gmr/rejected',
      license='BSD',
      packages=['rejected'],
      requires=['pika', 'pyyaml'],
      entry_points=dict(console_scripts=['rejected=rejected:main']),
      test_suite='nose.collector',
      tests_require=['nose'],
      zip_safe=False)
