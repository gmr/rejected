Exception Classes
=================
The following exceptions should be raised by your code in your
:class:`~rejected.consumer.Consumer` code. See the
:ref:`consumers <consumers>`. documentation for more information.

.. autoclass:: rejected.errors.ConsumerException
   :members:

.. autoclass:: rejected.errors.MessageException
   :members:

.. autoclass:: rejected.errors.ProcessingException
   :members:


Other Exceptions
----------------
These exceptions should not be raised directly by your code.

.. autoclass:: rejected.errors.RejectedException
   :members:

.. autoclass:: rejected.errors.RabbitMQException
   :members:

.. autoclass:: rejected.errors.ConfigurationException
   :members:

.. autoclass:: rejected.errors.ExecutionFinished
   :members:

.. autoclass:: rejected.errors.DropMessage
   :members:
