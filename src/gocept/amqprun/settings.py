import gocept.amqprun.interfaces
import zope.interface


@zope.interface.implementer_only(gocept.amqprun.interfaces.ISettings)
class Settings(dict):
    """Storage for settings with a dict API."""
