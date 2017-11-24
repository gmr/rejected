Exception Classes
=================
The following exceptions should be raised by your code in your
:class:`~rejected.consumer.Consumer` code. See the
:ref:`consumers <consumers>`. documentation for more information.

.. autoclass:: rejected.consumer.ConsumerException
   :members:

.. autoclass:: rejected.consumer.MessageException
   :members:

.. autoclass:: rejected.consumer.ProcessingException
   :members:


Other Exceptions
----------------
These exceptions should not be raised directly by your code.

.. autoclass:: rejected.consumer.RejectedException
   :members:


.. autoclass:: rejected.consumer.ConfigurationException
   :members:

