# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import logging
import gocept.amqprun.handler


log = logging.getLogger(__name__)


def log_message(message):
    log.info(message.body)


basic_handler = gocept.amqprun.handler.HandlerDeclaration(
    'queue', 'routing', log_message)
