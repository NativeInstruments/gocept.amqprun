# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import ZConfig
import gocept.amqprun.server
import gocept.amqprun.settings
import gocept.amqprun.worker
import logging
import pkg_resources
import signal
import threading
import time
import zope.component
import zope.configuration.xmlconfig
import zope.event

log = logging.getLogger(__name__)


# Holds a reference to the reader started by main(). This is to make testing
# easier where main() is started in a thread.
main_reader = None


def main(config_file):
    global main_reader

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

    reader = gocept.amqprun.server.Server(conf.amqp_server)
    main_reader = reader

    def stop_reader(signum, frame):
        log.info("Received signal %s, terminating." % signum)
        for worker in workers:
            worker.stop()
        reader.stop()

    signal.signal(signal.SIGINT, stop_reader)
    signal.signal(signal.SIGTERM, stop_reader)

    zope.event.notify(gocept.amqprun.interfaces.ProcessStarting())

    workers = []
    for i in range(conf.worker.amount):
        worker = gocept.amqprun.worker.Worker(
            reader.tasks, reader.get_session)
        workers.append(worker)
        worker.start()

    # Start the reader in a separate thread. When we receive a signal
    # (INT/TERM) the main thread is busy handling the signal (stop_reader()
    # above). If the reader was running in the main thread it could not longer
    # do the communication with the amqp server. This leads to frozen workers
    # because they wait for a commit or abort which requires communication.
    # Using a separate thread for the reader allows the reader to run until it
    # is explicitly stopped by the signal handler.
    reader_thread = threading.Thread(target=reader.start)
    reader_thread.start()
    while reader_thread.is_alive():
        time.sleep(1)
    reader_thread.join()
    log.info('Exiting')
    main_reader = None
