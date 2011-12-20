==============
gocept.amqprun
==============

gocept.amqprun helps you writing and running AMQP consumers. It currently only
supports AMQP 0-8 and integrates with the Zope Tool Kit (ZTK) so you can use
adapters, utilities and all the buzz. It requires Python 2.7.


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

* Message Ids

  * Each outgoing message gets a email-like message id.

  * The corrlation id of outoing message is set to the message id of the
    incomming message.

  * Each outgoing message gets a custom references header which is set to the
    incoming message's reference header plus the eincoming message's message
    id.


Getting started
===============

To get started define a function which does the work. In this case, we log the
message body and send a message. The ``declare`` decorator takes two arguments,
the queue name and the routing key (you can also pass in a list if you want to
bind the function to multiple routing keys). The ``declare`` decorator also
supports an optional ``arguments`` argument as a dictionary that is passed to
the AMQP queue_declare call e.g to support mirrored queues on RabbitMQ::

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
looks like this[1]_::

    <configure xmlns="http://namespaces.zope.org/zope">
      <include package="gocept.amqprun" />
      <utility component="path.to.package.log_message_body" name="basic" />
    </configure>


.. [1]: It's likely that there will be a special ZCML statement and/or grok
        support to make registering of handlers easier.


To set up a server, it's recommended to create a buildout. The following
buildout creates a config for gocept.amqprun, a ZCML file for the component
configuration, and uses ZDaemon to daemonize the process::

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



Settings
========

For application specific settings gocept.amqprun makes the ``<settings>``
section from the configuration available via an ``ISettings`` utility::

    settings = zope.component.getUtility(
        gocept.amqprun.interfaces.ISettings)
    settings.get('your.settings.key')


Limitations
===========

* Currently all messages are send and received through the `amq.topic`
  exchange.  Other exchanges are not supported at the moment.


Interfacing with the filesystem
===============================

gocept.amqprun provides a quick way to set up a handler that writes incoming
messages as individual files to a given directory, using the
``<amqp:writefiles>`` ZCML directive::

    <configure xmlns="http://namespaces.zope.org/zope"
               xmlns:amqp="http://namespaces.gocept.com/amqp">

      <include package="gocept.amqprun" />

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

``pattern`` performs as ``string.Template`` substitution. The following
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

The ``<amqp:writefiles>`` ZCML directive also supports an optional
``arguments`` parameter just like the ``declare`` decorator that is passed
to the AMQP queue_declare call e.g to support RabbitMQ mirrored queues::

    <amqp:writefiles
      routing_key="test.foo test.bar"
      queue_name="test.queue"
      directory="/path/to/output-directory"
      arguments="
      x-ha-policy = all
      "
      />


Reporting bugs
==============

Please report any bugs you find at
https://intra.gocept.com/projects/projects/gocept-amqprun