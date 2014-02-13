import gocept.amqprun.interfaces
import zope.component


@zope.component.adapter(gocept.amqprun.interfaces.IMessageStored)
def provoke_error(event):
    raise RuntimeError('provoked')
