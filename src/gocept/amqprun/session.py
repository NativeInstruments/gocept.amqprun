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

    @property
    def received_message(self):
        return self._received_message

    @received_message.setter
    def received_message(self, value):
        self._received_message = value
        self.received_tag = getattr(value, 'delivery_tag', None)

    def send(self, message):
        self.join_transaction()
        self.messages.append(message)

    def reset(self):
        self.messages[:] = []
        self.received_message = None
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
        self.received_message.acknowledge()

    def publish_response_messages(self):
        for message in self.messages:
            log.debug("Publishing message to %s in response to %s.",
                      message.routing_key, self.received_tag)
            message.reference(self.received_message)
            self.channel.basic_publish(
                message.as_amqp_message(),
                exchange=message.exchange,
                routing_key=message.routing_key)

    def __repr__(self):
        return '<gocept.amqprun.session.Session %s>' % self.received_tag


class AMQPDataManager(object):

    zope.interface.implements(transaction.interfaces.ISavepointDataManager)

    transaction_manager = None

    def __init__(self, session):
        self.session = session
        self._channel = session.channel
        self._tpc_begin = False

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

    def tpc_begin(self, transaction):
        self._tpc_begin = True
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
            # On errors in savepoints tpc_abort is called without a prior
            # tpc_begin().
            log.debug('tx_rollback')
            # XXX The following should not fail. But if it raises an
            # exception nevertheless we release the lock as otherwise the
            # next `tpc_begin()` will block forever when acquiring the
            # lock. Maybe there is a better solution.
            self._channel.tx_rollback()
        self.session.reset()

    def tpc_vote(self, transaction):
        log.debug("tx_commit")
        self._channel.tx_commit()

    def tpc_finish(self, transaction):
        pass

    def sortKey(self):
        # Try to sort last, so that we vote last.
        return "~gocept.amqprun:%f" % time.time()

    def savepoint(self):
        return NoOpSavepoint()

    def __repr__(self):
        return '<gocept.amqprun.session.DataManager for %s, %s>' % (
            transaction.get(), self.session)


class NoOpSavepoint(object):

    zope.interface.implements(transaction.interfaces.IDataManagerSavepoint)

    def rollback(self):
        pass
