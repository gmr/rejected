from setuptools import setup
from rejected import __version__

long_description = """\
Rejected is a RabbitMQ consumer daemon that allows you to focus on the
development of the code that handles the messages and not the code that
facilitates the communication with RabbitMQ.
"""

setup(name='rejected',
      version=__version__,
      description="RabbitMQ consumer daemon",
      long_description=long_description,
      classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
      ],
      keywords='amqp rabbitmq',
      author='Gavin M. Roy',
      author_email='gmr@myyearbook.com',
      url='http://github.com/gmr/rejected',
      license='BSD',
      packages=['rejected'],
      requires=['pika'],
      entry_points=dict(console_scripts=['rejected=cli:main']),
      zip_safe=True)
