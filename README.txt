==============
gocept.amqprun
==============

gocept.amqprun helps you writing and running AMQP consumers. It currently only
supports AMQP 0-8 and integrates with the Zope Tool Kit (ZTK) so you can use
adapters, utilities and all the buzz.


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


Getting started
===============

To get started define a function which does the work. In this case, we log the
message body and send a message. The ``handle`` decorator takes two arguments,
the queue name and the routing key::

    import logging
    import gocept.amqprun.handler
    import gocept.amqprun.message

    log = logging.getLogger(__name__)

    @gocept.amqprun.handler.handle('test.queue', 'test.routing')
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
