# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import gocept.amqprun.handler


messages_received = []

def handle_message(message):
    messages_received.append(message)


handler = gocept.amqprun.handler.HandlerDeclaration(
    'test.queue', 'test.routing', handle_message)
