# Copyright (c) 2010-2012 gocept gmbh & co. kg
# See also LICENSE.txt

import gocept.amqprun.interfaces
import logging
import time
import transaction.interfaces
import zope.interface


log = logging.getLogger(__name__)


class Session(object):

    zope.interface.implements(gocept.amqprun.interfaces.ISession)

    def __init__(self, channel, message_to_ack=None):
        self.messages = []
        self._needs_to_join = True
        self.channel = channel
        self.message_to_ack = message_to_ack

    def send(self, message):
        self.join_transaction()
        self.messages.append(message)

    def reset(self):
        self.messages[:] = []
        self.message_to_ack = None
        self._needs_to_join = True

    def join_transaction(self):
        if not self._needs_to_join:
            return
        dm = AMQPDataManager(self)
        transaction.get().join(dm)
        self._needs_to_join = False


class AMQPDataManager(object):

    zope.interface.implements(transaction.interfaces.IDataManager)

    transaction_manager = None

    def __init__(self, session):
        self.session = session
        self.connection_lock = session.channel.connection_lock
        self._channel = session.channel
        self._tpc_begin = False

    # XXX refactor structure of Session and AMQPDataManager, see #9988
    @property
    def message(self):
        return self.session.message_to_ack

    def abort(self, transaction):
        # Called on transaction.abort() *and* on errors in tpc_vote/tpc_finish
        # of any datamanger *if* self has *not* voted, yet.
        if self._tpc_begin:
            # If a TPC has begun already, do nothing. tpc_abort handles
            # everything we do as well.
            return
        self.session.reset()
        gocept.amqprun.interfaces.IChannelManager(self._channel).release()

    def tpc_begin(self, transaction):
        log.debug("Acquire commit lock: %s for %s", self, transaction)
        self._tpc_begin = True
        self.connection_lock.acquire()
        self._channel.tx_select()

    def commit(self, transaction):
        if self.message is not None:
            log.debug("Ack'ing message %s.", self.message.delivery_tag)
            self._channel.basic_ack(self.message.delivery_tag)
        for message in self.session.messages:
            log.debug("Publishing message (%s).", message.routing_key)
            self._set_references(message)
            self._channel.basic_publish(
                message.exchange, message.routing_key,
                message.body, message.header)
        self.session.reset()

    def _set_references(self, message):
        if self.message is None:
            # no reply
            return
        if not self.message.header.message_id:
            return
        if not message.header.correlation_id:
            message.header.correlation_id = self.message.header.message_id
        if message.header.headers is None:
            message.header.headers = {}
        if 'references' not in message.header.headers:
            if (self.message.header.headers and
                self.message.header.headers.get('references')):
                parent_references = (
                    self.message.header.headers['references'] + '\n')
            else:
                parent_references = ''
            message.header.headers['references'] = (
                parent_references +
                self.message.header.message_id)

    def tpc_abort(self, transaction):
        if self._tpc_begin:
            log.debug('tx_rollback')
            self._channel.tx_rollback()
            self.connection_lock.release()
        # The original idea was to reject the message here. Reject with requeue
        # immediately re-queues the message in the current rabbitmq
        # implementation (2.1.1). We let the message dangle until the channel
        # is closed. At this point the message is re-queued and re-submitted to
        # us.
        self.session.reset()
        gocept.amqprun.interfaces.IChannelManager(self._channel).release()

    def tpc_vote(self, transaction):
        log.debug("tx_commit")
        self._channel.tx_commit()

    def tpc_finish(self, transaction):
        log.debug("Release commit lock: %s for %s", self, transaction)
        self.connection_lock.release()
        gocept.amqprun.interfaces.IChannelManager(self._channel).release()

    def sortKey(self):
        # Try to sort last, so that we vote last.
        return "~gocept.amqprun:%f" % time.time()
