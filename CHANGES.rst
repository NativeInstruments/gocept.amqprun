CHANGES
=======

1.7.1 (2016-08-22)
------------------

- Fix bug, when a string is passed as a port number to
  ``gocept.amqprun.connection.Parameters``.


1.7 (2016-05-31)
----------------

- Fix possible unresponsiveness of message handlers which occurred after an
  exception in the worker. The exception caused a counter under-run which
  prevents switching channels. The counter is no longer bound to the
  transaction but to the message handling.

- Add a save guard which kills the whole process if the reference counter on
  the channel falls below zero.

- Acquire the channel before putting a task to the worker queue to prevent a
  possible race condition.

- Release the commit lock if ``tpc_abort()`` fails. So the next ``tpc_begin()``
  does not wait forever while trying to acquire the lock and thus blocking the
  whole process.

1.6 (2016-04-04)
----------------

- Allow arbitrary keywords in ``.testing.QueueTestCase.send_message()`` to be
  passed to generated message.

1.5 (2016-02-01)
----------------

- Add a ``testing`` extra which can be used to reuse the test infrastructure of
  this package to develop tests on top of it.

1.4 (2015-11-20)
----------------

- Acknowledge message in ``ErrorHandlingHandler`` on non recoverable error.
  This got lost in 0.17.0.
  Fixes: https://bitbucket.org/gocept/gocept.amqprun/issue/5


- The error message of ``ErrorHandlingHandler`` on a non recoverable error
  references the original message using its correlation_id again.
  This got lost in 0.17.0.
  Fixes: https://bitbucket.org/gocept/gocept.amqprun/issue/6


1.3 (2015-09-11)
----------------

- Update the test infrastructure to be able to run with newer RabbitMQ versions
  than 3.1.5.


1.2 (2015-09-11)
----------------

- Add `py.test` as test runner.

- Add `settings` attribute to `IHandler` to ease accessing `ISettings`.

1.1 (2015-09-01)
----------------

- ``.connection.Parameters`` no longer accepts arbitrary keyword parameters.


1.0 (2015-04-10)
----------------

- Notify ``IConfigFinished`` event after `gocept.amqprun` is fully configured
  but before the workers are started. This event can be used to open database
  connections for instance.

- Bump version to 1.0 as this package is used in production for years.


0.17.0 (2015-04-09)
-------------------

- Abort transaction in ``ErrorHandlingHandler`` on non recoverable error.
  Fixes: https://bitbucket.org/gocept/gocept.amqprun/issue/4


0.16.0 (2015-03-25)
-------------------

- Raise ``RuntimeError`` if connection is not alive after connect. (This can
  happen when the credentials are wrong.)


0.15.2 (2015-02-24)
-------------------

- Fix error reporting in ``.testing.QueueLayer.rabbitmqctl()``.


0.15.1 (2015-01-21)
-------------------

- Moved code to and bug tracker to Bitbucket_.

.. _Bitbucket : https://bitbucket.org/gocept/gocept.amqprun


0.15.0 (2014-10-08)
-------------------

- Pretend to support savepoints so we can work together with other systems that
  support (and require) them.


0.14.0 (2014-09-09)
-------------------

- Introduce ``gocept.amqprun.handler.ErrorHandlingHandler`` base class.

- Run integration tests (MainTestCase) in a separate ZCA, isolated from any
  registrations that might have been done by other tests.

- Create temporary virtualhost on the rabbitmq server per test run.

- Improved test-case method ``send_message`` to take headers as a parameter.


0.13.0 (2014-04-16)
-------------------

- Introduce ``IResponse`` for transaction-integrated exception handling.

- Fix timing bug: when the remote side closes the socket and at the same time a
  channel switch is triggered, this used to break down (since the socket is
  closed). We now ignore the channel switch attempt and simply wait for the
  reconnect, which opens a new channel anyway.


0.12.2 (2014-02-20)
-------------------

- Add ``xfilename`` variable to the ``<amqp:writefiles pattern="">`` setting.


0.12.1 (2014-02-20)
-------------------

- Add safeguard that two handlers cannot bind to the same queue.


0.12 (2014-02-13)
-----------------

- Include <amqp:writefiles> into the transaction handling, i.e. write to file
  only on commit. Previously, when an error occurred e.g. inside the
  MessageStored event, duplicate files would be written (each time the message
  was processed again on retry after the error).


0.11 (2014-01-21)
-----------------

- Crash the process on filesystem errors for <amqp:readfiles>. Previously only
  the file reading thread crashed, but the process kept running, unaware that
  it now was not doing its job anymore, since the thread had died.


0.10 (2013-05-28)
-----------------

- Support more than one Server in a single process.

- Introduce ``setup_handlers`` parameter for Server so clients can disable
  setting up handlers.

- Fix a bug in the <amqp:readfiles> transaction implementation that caused
  crash when aborting a transaction (#12437).

- Allow test server to take longer time for startup on slow computers.


0.9.5 (2013-04-16)
------------------

- Refactor Session / DataManager responsibilities (#9988).


0.9.4 (2012-09-07)
------------------

- Fix IDataManager implementation: abort() may be called multiple times,
  regardless of transaction outcome. Only release the channel refcount once.


0.9.3 (2012-09-07)
------------------

- Improve logging of IDataManager.


0.9.2 (2012-09-07)
------------------

- Improve logging of IChannelManager.acquire/release.


0.9.1 (2012-09-06)
------------------

- Fix IDataManager implementation: tpc_abort() may also be called without a
  prior tpc_begin() (happens for errors in savepoints, for example).
- Fix method signature of Connection.close().


0.9 (2012-08-31)
----------------

- Introduce optional integration with zope.security: handlers can declare a
  principal id with which an interaction will be created.
- Use a separate channel for sending messages that are not a response to a
  received message.
- Introduce SETTINGS_LAYER for tests relying on ISettings.


0.8 (2012-04-04)
----------------

- Fix race condition that caused messages to be acknowledged on a different
  channel than they were received on (#10635).

- Fix race condition that caused attempts at sending messages before the
  server was started properly (#10620).


0.7 (2012-03-22)
----------------

- Fix race condition between getting the current channel in the DataManager and
  switching the current channel in the Server (#10521).
- Make AMQP server configurable for tests (#9232).


0.6.1 (2012-02-23)
------------------

- Fixed bug in creating references header when parent message has no references
  (#10478).


0.6 (2012-02-22)
----------------

Features
~~~~~~~~

- Changed FileStoreReader from its own process to a thread that uses
  gocep.amqprun for sending (previously it used amqplib). Introduced
  ``amqp:readfiles`` ZCML directive. (#10177)

- Changed `filestore` extra to `readfiles` extra.

- Transmit filename as ``X-Filename`` header from ``amqp:readfiles``.

- Introduced ``ISender`` utility.

Bugs
~~~~

- Fixed bug with acknowledging messages that was introduced in 0.5 (#10030).

Internal
~~~~~~~~

- Changed API for MainTestCase from ``create_reader`` to ``start_server``.


0.5.1 (2012-01-09)
------------------

- Bugfix to support unicode arguments for queue declaration as pika
  only supports bytestrings here.
- Bugfix to make ``arguments`` parameter of ``amqp:writefiles`` work (#10115).


0.5 (2011-12-08)
----------------

General
~~~~~~~

- Added `writefiles` extra to make ZCML directive ``amqp:writefiles`` optional.

- Added `filestore` extra to make ``gocept.amqprun.filestore`` optional.

- Moved declaration of ``amqp:writefiles`` from ``configure.zcml`` to
  ``meta.zcml``.


Features
~~~~~~~~

- Renamed ``gocept.amqprun.server.MessageReader`` into
  ``gocept.amqprun.server.Server`` and added a ``send`` method so it can
  initiate sending of messages.

- Add support for arguments for queue_declare e.g to support x-ha-policy
  headers for RabbitMQ mirrored queue deployments (#10036).


Internal
~~~~~~~~

- Internal API change in ``server.AMQPDataManager.__init__``: the `message`
  parameter is now optional, so it was moved to the end of the list of
  arguments.

- Use plone.testing for layer infrastructure.


0.4.2 (2011-08-23)
------------------

- Add helper methods for dealing with header files to FileWriter (for #9443).


0.4.1 (2011-08-22)
------------------

- Log Message-ID.


0.4 (2011-07-25)
----------------

- The message id of outgoing messages is set.
- The correlation id of outgoing messages is set to the incoming message's
  message id (if set).
- A custom header ``references`` is set to the incoming message's reference
  header + the incomming message's message id (like `References` in RFC5322).
- Fixed broken tests.
- Allow upper case in settings keys.
- Extend AMQP server configuration for FileStoreReader to include credentials
  and virtual host.
- Allow specifying multiple routing keys (#9326).
- Allow specifying a filename/path pattern (#9327).
- The FileWriter stores the headers in addition to the body (#9328).
- FileWriter sends IMessageStored event (#9335).


0.3 (2011-02-05)
----------------

- Renamed decorator from ``handle`` to ``declare``.
- Added helper method ``wait_for_response`` to MainTestCase.
- Added an IProcessStarting event which is sent during startup.
- Added the <amqp:writefiles/> directive that sets up a handler that writes
  incoming messages into files.
- Added handling of <logger> directives


0.2 (2010-09-14)
----------------

- Added a decorator ``gocept.amqprun.handler.handle(queue_name, routing_key)``.


0.1 (2010-08-13)
----------------

- first release.
