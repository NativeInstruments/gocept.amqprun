# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import gocept.amqprun.interfaces
import zope.interface


class Settings(dict):

    zope.interface.implements(gocept.amqprun.interfaces.ISettings)
