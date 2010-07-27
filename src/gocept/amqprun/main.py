# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import ZConfig
import gocept.amqprun.server
import gocept.amqprun.settings
import gocept.amqprun.worker
import pkg_resources
import zope.component
import zope.configuration.xmlconfig


# Holds a reference to the reader stared by main(). This is to make testing
# easier where main() is started in a thread.
main_reader = None


def main(config_file):
    global main_reader
    schema = ZConfig.loadSchemaFile(pkg_resources.resource_stream(
        __name__, 'schema.xml'))
    conf, handler = ZConfig.loadConfigFile(schema, open(config_file))
    conf.eventlog.startup()
    # Provide utility before xml config to allow components configured via ZCML
    # to use the utility.
    settings = gocept.amqprun.settings.Settings()
    zope.component.provideUtility(settings)
    if conf.settings:
        settings.update(conf.settings)

    zope.configuration.xmlconfig.file(conf.worker.component_configuration)

    reader = gocept.amqprun.server.MessageReader(
        lambda: gocept.amqprun.server.Connection(conf.amqp_server))
    main_reader = reader

    for i in range(conf.worker.amount):
        worker = gocept.amqprun.worker.Worker(
            reader.tasks, reader.create_session)
        worker.start()
    reader.start()  # this blocks until reader is stopped from outside.
    main_reader = None
