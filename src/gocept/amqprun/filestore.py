# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import ZConfig
import amqplib.client_0_8 as amqp
import gocept.amqprun.handler
import gocept.amqprun.interfaces
import gocept.filestore
import logging
import os.path
import pkg_resources
import time
import zope.configuration.fields
import zope.interface
import zope.schema


log = logging.getLogger(__name__)


class FileStoreReader(object):

    zope.interface.implements(gocept.amqprun.interfaces.ILoop)

    def __init__(self, path, routing_key, server):
        self.running = False
        hostname = server.hostname
        if server.port:
            hostname = '%s:%s' % (hostname, server.port)
        self.connection = amqp.Connection(
            host=hostname,
            userid=server.username,
            password=server.password,
            virtual_host=server.virtual_host)
        self.channel = self.connection.channel()
        self.routing_key = routing_key
        self.filestore = gocept.filestore.FileStore(path)
        self.filestore.prepare()

    def start(self):
        self.running = True
        while self.running:
            self.scan()
            time.sleep(1)

    def stop(self):
        self.running = False
        self.channel.close()
        self.connection.close()

    def scan(self):
        for filename in self.filestore.list('new'):
            log.debug('reading %r' % filename)
            self.send(open(filename).read())
            self.filestore.move(filename, 'new', 'cur')

    def send(self, body):
        # XXX make content-type configurable?
        self.channel.basic_publish(
            amqp.Message(body, content_type='text/xml'),
            'amq.topic', routing_key=self.routing_key)


def main(config_file):
    schema = ZConfig.loadSchemaFile(pkg_resources.resource_stream(
        __name__, 'filestore.xml'))
    conf, handler = ZConfig.loadConfigFile(schema, open(config_file))
    conf.eventlog.startup()
    reader = FileStoreReader(conf.settings.path,
                             conf.settings.routing_key,
                             conf.amqp_server)
    reader.start()


class FileWriter(object):

    def __init__(self, directory):
        self.directory = directory

    def __call__(self, message):
        output = open(os.path.join(
            self.directory, self._unique_filename(message)), 'w')
        output.write(message.body)
        output.close()

    def _unique_filename(self, message):
        # since CPython doesn't use OS-level threads, there won't be actual
        # concurrency, so we can get away with using the current time to
        # uniquify the filename -- we have to take care about the precision,
        # though: '%s' loses digits, but '%f' doesn't.
        return '%s_%f' % (message.routing_key, time.time())


class IWriteFilesDirective(zope.interface.Interface):

    routing_key = zope.configuration.fields.Tokens(
        title=u"Routing key(s) to listen on",
        value_type=zope.schema.TextLine())

    queue_name = zope.schema.TextLine(title=u"Queue name")

    directory = zope.configuration.fields.Path(
        title=u"Path to the directory in which to write the files")


def writefiles_directive(_context, routing_key, queue_name, directory):
    writer = FileWriter(directory)
    handler = gocept.amqprun.handler.HandlerDeclaration(
        queue_name, routing_key, writer)
    zope.component.zcml.utility(
        _context,
        component=handler,
        name=unicode('gocept.amqprun.amqpwrite.%s' % queue_name))
