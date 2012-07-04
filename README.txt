==============
gocept.amqprun
==============

gocept.amqprun helps you writing and running AMQP consumers, and sending AMQP
messages. It currently only supports AMQP 0-8 and integrates with the Zope Tool
Kit (ZTK) so you can use adapters, utilities and all the buzz.

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
  can be processed at the same time using threads. Those threads are called
  *workers*.


Things you don't need to take care of
=====================================

* Threading of multiple workers

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
``[security]`` extra to use this integration functionality).

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
      <include package="gocept.amqprun" />
      <utility component="path.to.package.log_message_body" name="basic" />
    </configure>

To set up a server, it's recommended to create a buildout. The following
buildout creates a config file for gocept.amqprun as well as a ZCML file for
the component configuration and uses ZDaemon to daemonize the process::

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
          amount 10
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
          <include package="gocept.amqprun" />
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
original process, in its own thread. Here is some example code to do that::

    def start_server(**kw):
        parameters = gocept.amqprun.connection.Parameters(**kw)
        server = gocept.amqprun.server.Server(parameters)
        server_thread = threading.Thread(target=server.start)
        server_thread.daemon = True
        server_thread.start()
        import time
        time.sleep(0.1)
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

Writing
-------

gocept.amqprun provides a quick way to set up a handler that writes incoming
messages as individual files to a given directory, using the
``<amqp:writefiles>`` ZCML directive. You need the `writefiles` extra to
enable this directive::

    <configure xmlns="http://namespaces.zope.org/zope"
               xmlns:amqp="http://namespaces.gocept.com/amqp">

      <include package="gocept.amqprun" file="meta.zcml" />

      <amqp:writefiles
        routing_key="test.data"
        queue_name="test.queue"
        directory="/path/to/output-directory"
        />
    </configure>

All messages with routing key 'test.data' would then be written to
'output-directory', two files per message, one containing the body and the
other containing the headers (in ``zope.xmlpickle`` format).
(Note that in the buildout example above, you would need to put the writefiles
directive into the ``[zcml]`` section, not the ``[config]`` section.)

You can specify multiple routing keys separated by spaces::

    <amqp:writefiles
      routing_key="test.foo test.bar"
      queue_name="test.queue"
      directory="/path/to/output-directory"
      />

You can configure the way files are named with the ``pattern`` parameter, for
example::

    <amqp:writefiles
      routing_key="test.data"
      queue_name="test.queue"
      directory="/path/to/output-directory"
      pattern="${routing_key}/${date}/${msgid}-${unique}.xml"
      />

``pattern`` performs a ``string.Template`` substitution. The following
variables are available:

  :date: The date the message arrived, formatted ``%Y-%m-%d``
  :msgid: The value of the message-id header
  :routing_key: The routing key of the message
  :unique: A token that guarantees the filename will be unique in its directory

The default value for ``pattern`` is ``${routing_key}-${unique}``.

To support zc.buildout, ``{variable}`` is accepted as an alternative syntax to
``${variable}``. (zc.buildout uses ``${}`` for its own substitutions, but
unfortunately does not support escaping them.)

If ``pattern`` contains slashes, intermediate directories will be created below
``directory``, so in the example, messages would be stored like this::

    /path/to/output-directory/example.route/2011-04-07/asdf998-1234098791.xml

Just like the ``declare`` decorator, the ``<amqp:writefiles>`` ZCML directive
also supports an optional ``arguments`` parameter that is passed to the AMQP
``queue_declare`` call to, e.g., support RabbitMQ mirrored queues::

    <amqp:writefiles
      routing_key="test.foo test.bar"
      queue_name="test.queue"
      directory="/path/to/output-directory"
      arguments="
      x-ha-policy = all
      "
      />

Reading
-------

You can also set up a thread to read files from a directory and publish them
onto the queue, using the ``<amqp:readfiles>`` ZCML directive (the filename
will be transmitted in the ``X-Filename`` header). You need the `readfiles`
extra to enable this directive::

    <configure xmlns="http://namespaces.zope.org/zope"
               xmlns:amqp="http://namespaces.gocept.com/amqp">

      <include package="gocept.amqprun" file="meta.zcml" />

      <amqp:readfiles
        directory="/path/to/input-directory"
        routing_key="test.data"
        />
    </configure>

The input-directory is expected to be a Maildir, i.e. files to be read should
appear in ``input-directory/new` which will be polled every second. After the
files have been published to the given routing key, they will be moved to
``input-directory/cur``.


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
    default: /

The source code is available in the mercurial repository at
https://code.gocept.com/hg/public/gocept.amqprun

Please report any bugs you find at
https://projects.gocept.com/projects/projects/gocept-amqprun/issues

.. vim: set ft=rst:
