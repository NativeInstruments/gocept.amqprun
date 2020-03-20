# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import logging
import gocept.amqprun.handler
import gocept.amqprun.main
import gocept.amqprun.message
import gocept.amqprun.interfaces
import transaction
import zope.component


log = logging.getLogger(__name__)


@gocept.amqprun.handler.declare('test.queue', 'test.routing')
def basic_handler(message):
    log.info('basic_handler: %s', message.body)


@gocept.amqprun.handler.declare('test.queue', 'test.echo')
def basic_echo(message):
    sender = zope.component.getUtility(gocept.amqprun.interfaces.ISender)
    message.routing_key = 'test.echoed'
    sender.send(message)


def send_messages(config_file):  # pragma: no cover
    server = gocept.amqprun.main.create_configured_server(config_file)
    server.connect()
    for x in range(10):
        message = gocept.amqprun.message.Message(
            header={}, body='Nachricht %s' % x,
            routing_key='test.routing')
        server.send(message)

    transaction.commit()
