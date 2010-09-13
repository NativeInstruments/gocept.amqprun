# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import logging
import gocept.amqprun.handler


log = logging.getLogger(__name__)


@gocept.amqprun.handler.handle('test.queue', 'test.routing')
def basic_handler(message):
    log.info(message.body)
