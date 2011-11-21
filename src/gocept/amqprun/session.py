# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import gocept.amqprun.interfaces
import logging
import time
import transaction.interfaces
import zope.interface


log = logging.getLogger(__name__)


class Session(object):

    zope.interface.implements(gocept.amqprun.interfaces.ISession)

    def __init__(self, context, message=None):
        self.messages = []
        self.context = context
        self.message = message
        self._needs_to_join = True

    def send(self, message):
        self._join_transaction()
        self.messages.append(message)

    def reset(self):
        self.messages[:] = []
        self._needs_to_join = True

    def _join_transaction(self):
        if not self._needs_to_join:
            return
        dm = AMQPDataManager(self.context, self, self.message)
        transaction.get().join(dm)
        self._needs_to_join = False


class AMQPDataManager(object):

    zope.interface.implements(transaction.interfaces.IDataManager)

    transaction_manager = None

    def __init__(self, reader, session, message=None):
        self.connection_lock = reader.connection.lock
        self._channel = reader.channel
        self.message = message
        self.session = session
        self._tpc_begin = False
        gocept.amqprun.interfaces.IChannelManager(self._channel).acquire()

    def abort(self, transaction):
        # Called on transaction.abort() *and* on errors in tpc_vote/tpc_finish
        # of any datamanger *if* self has *not* voted, yet.
        if self._tpc_begin:
            # If a TPC has begun already, do nothing. tpc_abort handles
            # everything we do as well.
            return
        with self.connection_lock:
            self.session.reset()
            gocept.amqprun.interfaces.IChannelManager(self._channel).release()

    def tpc_begin(self, transaction):
        log.debug("Acquire commit lock for %s", transaction)
        self.connection_lock.acquire()
        self._tpc_begin = True
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
                self.message.header.headers['references']):
                parent_references = (
                    self.message.header.headers['references'] + '\n')
            else:
                parent_references = ''
            message.header.headers['references'] = (
                parent_references +
                self.message.header.message_id)

    def tpc_abort(self, transaction):
        log.debug('tx_rollback')
        self._channel.tx_rollback()
        # The original idea was to reject the message here. Reject with requeue
        # immediately re-queues the message in the current rabbitmq
        # implementation (2.1.1). We let the message dangle until the channel
        # is closed. At this point the message is re-queued and re-submitted to
        # us.
        self.session.reset()
        gocept.amqprun.interfaces.IChannelManager(self._channel).release()
        self.connection_lock.release()

    def tpc_vote(self, transaction):
        log.debug("tx_commit")
        self._channel.tx_commit()

    def tpc_finish(self, transaction):
        log.debug("releasing commit lock")
        gocept.amqprun.interfaces.IChannelManager(self._channel).release()
        self.connection_lock.release()

    def sortKey(self):
        # Try to sort last, so that we vote last.
        return "~gocept.amqprun:%f" % time.time()
