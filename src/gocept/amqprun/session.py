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

    def __init__(self, channel, received_message=None):
        self.messages = []
        self._needs_to_join = True
        self.channel = channel
        self.received_message = received_message
        self.received_tag =getattr(self.received_message, 'delivery_tag', None)

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

    def flush(self):
        self.ack_received_message()
        self.publish_response_messages()
        self.reset()

    def ack_received_message(self):
        if self.received_message is None:
            return
        log.debug("Ack'ing message %s.", self.received_message.delivery_tag)
        self.channel.basic_ack(self.received_message.delivery_tag)

    def publish_response_messages(self):
        for message in self.messages:
            log.debug("Publishing message to %s in response to %s.",
                      message.routing_key, self.received_tag)
            self._set_references(message)
            self.channel.basic_publish(
                message.exchange, message.routing_key,
                message.body, message.header)

    def _set_references(self, message):
        if self.received_message is None:
            # no reply
            return
        received_header = self.received_message.header
        if not received_header.message_id:
            return
        if not message.header.correlation_id:
            message.header.correlation_id = received_header.message_id
        if message.header.headers is None:
            message.header.headers = {}
        if 'references' not in message.header.headers:
            if (received_header.headers and
                received_header.headers.get('references')):
                parent_references = (
                    received_header.headers['references'] + '\n')
            else:
                parent_references = ''
            message.header.headers['references'] = (
                parent_references +
                received_header.message_id)

    def __repr__(self):
        return '<gocept.amqprun.session.Session %s>' % self.received_tag


class AMQPDataManager(object):

    zope.interface.implements(transaction.interfaces.IDataManager)

    transaction_manager = None

    def __init__(self, session):
        self.session = session
        self.connection_lock = session.channel.connection_lock
        self._channel = session.channel
        self._channel_released = False
        self._tpc_begin = False

    def _release_channel(self):
        if not self._channel_released:
            self._channel_released = True
            gocept.amqprun.interfaces.IChannelManager(self._channel).release()

    def abort(self, transaction):
        # Called on
        # - transaction.abort()
        # - errors in savepoints
        # - errors after a tpc_begin in tpc_vote/tpc_finish, *if* self has
        #   *not* yet voted (tpc_abort will still be called afterwards).
        #   NOTE: if self *has* voted, tpc_abort will be called *instead*
        #   of abort.
        # - during afterCommitHooks, regardless of transaction outcome
        self.session.reset()
        self._release_channel()

    def tpc_begin(self, transaction):
        log.debug("Acquire commit lock by %s", self)
        self._tpc_begin = True
        self.connection_lock.acquire()
        self._channel.tx_select()

    def commit(self, transaction):
        self.session.flush()

    def tpc_abort(self, transaction):
        # The original idea was to reject the message here. Reject with requeue
        # immediately re-queues the message in the current rabbitmq
        # implementation (2.1.1). We let the message dangle until the channel
        # is closed. At this point the message is re-queued and re-submitted to
        # us.
        if self._tpc_begin:
            log.debug('tx_rollback')
            self._channel.tx_rollback()
            self.connection_lock.release()
        self.session.reset()
        self._release_channel()

    def tpc_vote(self, transaction):
        log.debug("tx_commit")
        self._channel.tx_commit()

    def tpc_finish(self, transaction):
        log.debug("Release commit lock by %s", self)
        self.connection_lock.release()
        self._release_channel()

    def sortKey(self):
        # Try to sort last, so that we vote last.
        return "~gocept.amqprun:%f" % time.time()

    def __repr__(self):
        return '<gocept.amqprun.session.DataManager for %s, %s>' % (
            transaction.get(), self.session)
