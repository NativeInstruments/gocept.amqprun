# Copyright (c) 2010-2011 gocept gmbh & co. kg
# See also LICENSE.txt

import gocept.amqprun.handler
import gocept.amqprun.interfaces
import logging
import os.path
import re
import time
import transaction
import zope.component.zcml
import zope.configuration.fields
import zope.event
import zope.interface
import zope.schema
import zope.xmlpickle


log = logging.getLogger(__name__)


class FileWriter(object):

    def __init__(self, directory, pattern):
        self.directory = directory
        if not pattern:
            pattern = '${routing_key}-${unique}'
        self.pattern = pattern

    def __call__(self, message):
        filename = message.generate_filename(self.pattern)
        dm = FileDataManager(message, self.directory, filename)
        transaction.get().join(dm)
        zope.event.notify(
            gocept.amqprun.interfaces.MessageStored(message, filename))

    @staticmethod
    def header_filename(filename):
        basename, extension = os.path.splitext(filename)
        return '%s.header%s' % (basename, extension)

    @staticmethod
    def is_header_file(filename):
        basename, extension = os.path.splitext(filename)
        return basename.endswith('.header')


class FileDataManager(object):

    def __init__(self, message, base_directory, path):
        self.message = message
        self.base_directory = base_directory
        self.path = path

    def abort(self, transaction):
        pass

    def tpc_begin(self, transaction):
        pass

    def commit(self, transaction):
        pass

    def tpc_abort(self, transaction):
        pass

    def tpc_vote(self, transaction):
        directory = os.path.join(
            self.base_directory, os.path.dirname(self.path))
        filename = os.path.basename(self.path)
        log.debug('Writing message to %s%s' % (directory, filename))
        self._ensure_directory(directory)
        self._write(self.message.body, directory, filename)
        self._write(zope.xmlpickle.dumps(self.message.header),
                    directory, FileWriter.header_filename(filename))

    def tpc_finish(self, transaction):
        pass

    def sortKey(self):
        # Try to sort last, so that we vote last.
        return "~gocept.amqprun.writefiles:%f" % time.time()

    def _write(self, content, *path):
        output = open(os.path.join(*path), 'w')
        output.write(content)
        output.close()

    def _ensure_directory(self, path):
        path = path.replace(self.base_directory, '')
        parts = path.split(os.sep)
        directory = self.base_directory
        for d in parts:
            directory = os.path.join(directory, d)
            if not os.path.exists(directory):
                os.mkdir(directory)


class IWriteFilesDirective(zope.interface.Interface):

    routing_key = zope.configuration.fields.Tokens(
        title=u"Routing key(s) to listen on",
        value_type=zope.schema.TextLine())

    queue_name = zope.schema.TextLine(title=u"Queue name")

    directory = zope.configuration.fields.Path(
        title=u"Path to the directory in which to write the files")

    pattern = zope.schema.TextLine(title=u"File name pattern")

    arguments = gocept.amqprun.interfaces.RepresentableDict(
        key_type=zope.schema.TextLine(),
        value_type=zope.schema.TextLine(),
        required=False)


def writefiles_directive(
        _context, routing_key, queue_name, directory, pattern, arguments=None):
    # buildout doesn't support escaping '${}' and thus thinks it should resolve
    # those substitions itself. So we support '{}' in addition to '${}'
    pattern = re.sub(r'(^|[^$]){', r'\1${', pattern)
    writer = FileWriter(directory, pattern)
    handler = gocept.amqprun.handler.Handler(
        queue_name, routing_key, writer, arguments)
    zope.component.zcml.utility(
        _context,
        component=handler,
        name=unicode('gocept.amqprun.amqpwrite.%s' % queue_name))
