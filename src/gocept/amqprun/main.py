# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import ZConfig
import gocept.amqprun.server
import gocept.amqprun.settings
import gocept.amqprun.worker
import logging
import pkg_resources
import signal
import zope.component
import zope.configuration.xmlconfig


log = logging.getLogger(__name__)


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
        settings.update(
            {unicode(k): unicode(v) for k, v in conf.settings.items()})

    zope.configuration.xmlconfig.file(conf.worker.component_configuration)

    reader = gocept.amqprun.server.MessageReader(conf.amqp_server)
    main_reader = reader

    def stop_reader(signum, frame):
        log.info("Received signal %s, terminating." % signum)
        for worker in workers:
            worker.stop()
        reader.stop()

    signal.signal(signal.SIGINT, stop_reader)
    signal.signal(signal.SIGTERM, stop_reader)

    workers = []
    for i in range(conf.worker.amount):
        worker = gocept.amqprun.worker.Worker(
            reader.tasks, reader.create_session)
        workers.append(worker)
        worker.start()

    reader.start()  # this blocks until reader is stopped from outside.
    main_reader = None
