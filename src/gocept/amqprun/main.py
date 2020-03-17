# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import ZConfig
import gocept.amqprun.server
import gocept.amqprun.settings
import gocept.amqprun.worker
import logging
import pkg_resources
import zope.component
import zope.configuration.xmlconfig
import zope.event


log = logging.getLogger(__name__)


def create_configured_server(config_file):
    schema = ZConfig.loadSchemaFile(pkg_resources.resource_stream(
        __name__, 'schema.xml'))
    conf, handler = ZConfig.loadConfigFile(schema, open(config_file))

    conf.eventlog.startup()
    for logger in conf.loggers:
        logger.startup()

    # Provide utility before xml config to allow components configured via ZCML
    # to use the utility.
    settings = gocept.amqprun.settings.Settings()
    zope.component.provideUtility(settings)
    if conf.settings:
        settings.update(
            {unicode(k): unicode(v, 'UTF-8')
             for k, v in conf.settings.items()})

    zope.configuration.xmlconfig.file(conf.worker.component_configuration)

    params = {
        key: getattr(conf.amqp_server, key)
        for key in conf.amqp_server.getSectionAttributes()
        if getattr(conf.amqp_server, key)
    }

    server = gocept.amqprun.server.Server(params)
    zope.component.provideUtility(server, gocept.amqprun.interfaces.ISender)

    zope.event.notify(gocept.amqprun.interfaces.ConfigFinished())

    return server


def main(config_file):
    """"Main loop - does not return."""
    server = create_configured_server(config_file)
    server.start()
