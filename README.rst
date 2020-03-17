==============
gocept.amqprun
==============

``gocept.amqprun`` helps you writing and running AMQP consumers, and sending
AMQP messages. It currently only supports AMQP 0-9-1 and integrates with the
Zope Tool Kit (ZTK) so you can use adapters, utilities and all the buzz.

.. contents:: :depth: 1


Basic concepts and terms
========================

* A *message handler* is a function which is bound with a routing key to
  exactly one queue. It is called for each message on that queue, and may
  return a list of messages as a result.

* The result messages of one handled message are sent in one transaction
  together with the ACK of the handled message.

* When an exception is raised during message processing, the transaction is
  aborted. (The received message would be NACKed if RabbitMQ was supporting
  it.)

* A message handler handles exactly one message at a time. Multiple messages
  can be processed at the same time using multiple processes. (Each process is
  single-threaded.)


Things you don't need to take care of
=====================================

* Socket handling and locking for communicating with the AMQP broker

* Transaction handling

* Message ids

  * Each outgoing message gets an email-like message id.

  * The correlation id of each outgoing message is set to the message id of
    the incoming message.

  * Each outgoing message gets a custom references header which is set to the
    incoming message's reference header plus the incoming message's message
    id.


Getting started: receiving messages
===================================

To get started, define a function which does the work. In this example, we log
the message body and send a message. The ``declare`` decorator takes two
arguments, the queue name and the routing key (you can also pass in a list to
bind the function to multiple routing keys). The ``declare`` decorator also
supports an optional ``arguments`` argument that is a dictionary to be passed
to the AMQP queue_declare call to, e.g., support mirrored queues on RabbitMQ.
The optional argument ``principal`` specifies to wrap the handler call into a
zope.security interaction using the given principal id (you need the
``[security]`` setup.py extra to use this integration functionality).

::

    import logging
    import gocept.amqprun.handler
    import gocept.amqprun.message

    log = logging.getLogger(__name__)

    @gocept.amqprun.handler.declare('test.queue', 'test.routing')
    def log_message_body(message):
        log.info(message.body)
        msg = gocept.amqprun.message.Message(
            header=dict(content_type='text/plain'),
            body=u'Thank you for your message.',
            routing_key='test.thank.messages')
        return [msg]


The handler function needs to be registered as a named utility. With ZCML this
looks like this [#grok]_::

    <configure xmlns="http://namespaces.zope.org/zope">
      <utility component="your.package.log_message_body" name="basic" />
    </configure>

To set up a server, it's recommended to create a buildout. The following
buildout creates a config file for ``gocept.amqprun`` as well as a ZCML file
for the component configuration and uses ZDaemon to daemonize the process::

    [buildout]
    parts =
            config
            zcml
            app
            server

    [deployment]
    name = queue
    recipe = gocept.recipe.deploymentsandbox
    root = ${buildout:directory}

    [config]
    recipe = lovely.recipe:mkfile
    path = ${deployment:etc-directory}/queue.conf

    amqp-hostname = localhost
    amqp-username = guest
    amqp-password = guest
    amqp-virtualhost = /

    eventlog =
        <eventlog>
          level DEBUG
          <logfile>
            formatter zope.exceptions.log.Formatter
            path STDOUT
          </logfile>
        </eventlog>
    amqp-server =
        <amqp-server>
          hostname ${:amqp-hostname}
          username ${:amqp-username}
          password ${:amqp-password}
          virtual_host ${:amqp-virtualhost}
        </amqp-server>

    content =
        ${:eventlog}
        ${:amqp-server}
        <worker>
          component-configuration ${zcml:path}
        </worker>
        <settings>
          your.custom.settings here
        </settings>

    [zcml]
    recipe = lovely.recipe:mkfile
    path = ${deployment:etc-directory}/queue.zcml
    content =
        <configure xmlns="http://namespaces.zope.org/zope">
          <include package="your.package" />
        </configure>

    [app]
    recipe = zc.recipe.egg:script
    eggs =
       gocept.amqprun
       your.package
       zope.exceptions
    arguments = '${config:path}'
    scripts = server=app

    [server]
    recipe = zc.zdaemonrecipe
    deployment = deployment
    program = ${buildout:bin-directory}/app


.. [#grok] It's likely that there will be a special ZCML statement and/or grok
   support to make registering of handlers easier.


Sending messages
================

If all you want to do is send messages, you don't have to register any
handlers, but can use ``gocept.amqprun.server.Server.send()`` directly. While
the handlers usually run in their own process, started by the ``server``
entrypoint (as described above), if you're just sending messages, you can also
skip the extra process and run the ``gocept.amqprun.server.Server`` in your
original process. Here is some example code to do that::

    def start_server(**kw):
        server = gocept.amqprun.server.Server(kw, setup_handlers=False)
        server.connect()
        return server

(When you're using the ZCA, you'll probably want to register the ``Server`` as
a utility at that point, too, so clients can access it to send messages
easily.)


Settings
========

For application-specific settings gocept.amqprun makes the ``<settings>``
section from the configuration available via an ``ISettings`` utility::

    settings = zope.component.getUtility(
        gocept.amqprun.interfaces.ISettings)
    settings.get('your.settings.key')


Limitations
===========

* Currently all messages are sent and received through the `amq.topic`
  exchange. Other exchanges are not supported at the moment.


Interfacing with the file system
================================

Reading
-------

There is a ``send_files`` entrypoint in the setup.py. It can be configured with
three arguments: The path of the zconfig file, a watch path and a routing key.
It reads new files in the directory named ``new`` in the watch path and sends
a message with its content as the body and the filename as an X-Filename header
to the route. Sent files are moved to the directory called ``cur`` in the
watch path.

Development
===========

You can set the AMQP server parameters for running the tests via environment
variables:

:AMQP_HOSTNAME:
    default: localhost

:AMQP_USERNAME:
    default: guest

:AMQP_PASSWORD:
    default: guest

:AMQP_VIRTUALHOST:
    default: None, so a vhost with a temporary name is created and
    deleted automatically (using ``AMQP_RABBITMQCTL`` command)

:AMQP_RABBITMQCTL:
   default: 'sudo rabbitmqctl'

The source code is available in the mercurial repository at
https://github.com/gocept/gocept.amqprun

Please report any bugs you find at
https://github.com/gocept/gocept.amqprun/issues


bin/test_sender and bin/test_server
-----------------------------------

``test_sender`` and ``test_server`` are scripts that provide basic sender and
handler capabilities to smoke test the behaviour of our current implementation.
When started ``test_sender`` emits 10 messages routed to ``test.routing``.
``test.server`` declares a ``test.queue`` which all messages from
``test.routing`` are sent to and a handler logging every incoming message from
``test.queue``.

bin/test_send_files
-------------------
``test_send_files`` starts a server that watches the ./testdir/new directory
and sends files copied into it as an amqp message to ``test.routing``. Its
entrypoint is ``gocept.amqprun.readfiles:main``.
