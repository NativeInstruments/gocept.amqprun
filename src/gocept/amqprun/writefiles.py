# Copyright (c) 2010-2011 gocept gmbh & co. kg
# See also LICENSE.txt

import gocept.amqprun.handler
import gocept.amqprun.interfaces
import os.path
import re
import zope.component.zcml
import zope.configuration.fields
import zope.event
import zope.interface
import zope.schema
import zope.xmlpickle


class FileWriter(object):

    def __init__(self, directory, pattern):
        self.directory = directory
        if not pattern:
            pattern = '${routing_key}-${unique}'
        self.pattern = pattern

    def __call__(self, message):
        path = message.generate_filename(self.pattern)
        directory = os.path.join(self.directory, os.path.dirname(path))
        filename = os.path.basename(path)

        self.ensure_directory(directory)

        self.write(message.body,
                   directory, filename)
        self.write(zope.xmlpickle.dumps(message.header),
                   directory, self.header_filename(filename))

        zope.event.notify(gocept.amqprun.interfaces.MessageStored(
                message, path))

    def write(self, content, *path):
        output = open(os.path.join(*path), 'w')
        output.write(content)
        output.close()

    def ensure_directory(self, path):
        path = path.replace(self.directory, '')
        parts = path.split(os.sep)
        directory = self.directory
        for d in parts:
            directory = os.path.join(directory, d)
            if not os.path.exists(directory):
                os.mkdir(directory)

    @staticmethod
    def header_filename(filename):
        basename, extension = os.path.splitext(filename)
        return '%s.header%s' % (basename, extension)

    @staticmethod
    def is_header_file(filename):
        basename, extension = os.path.splitext(filename)
        return basename.endswith('.header')


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
    handler = gocept.amqprun.handler.HandlerDeclaration(
        queue_name, routing_key, writer, arguments)
    zope.component.zcml.utility(
        _context,
        component=handler,
        name=unicode('gocept.amqprun.amqpwrite.%s' % queue_name))
