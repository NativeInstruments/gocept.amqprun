# Copyright (c) 2010-2011 gocept gmbh & co. kg
# See also LICENSE.txt

import datetime
import gocept.amqprun.handler
import gocept.amqprun.interfaces
import os.path
import re
import string
import time
import zope.component.zcml
import zope.configuration.fields
import zope.event
import zope.interface
import zope.schema
import zope.xmlpickle


key_value_regex = re.compile(r'^(?P<key>[^:=\s[][^:=]*)'
                             r'(?P<sep>[:=]\s*)'
                             r'(?P<value>.*)$')


@zope.interface.implementer(zope.schema.interfaces.IDict)
class RepresentableDict(zope.schema.Dict):
    """A field representing a Dict, but representable in ZCML."""

    def fromUnicode(self, raw_value):
        retval = {}
        for line in raw_value.strip().splitlines():
            m = key_value_regex.match(line.strip())
            if m is None:
                continue

            key = m.group('key').rstrip()
            value = m.group('value')
            retval[key] = value

        self.validate(retval)
        return retval


class FileWriter(object):

    def __init__(self, directory, pattern):
        self.directory = directory
        if not pattern:
            pattern = '${routing_key}-${unique}'
        self.pattern = string.Template(pattern)

    def __call__(self, message):
        path = self.generate_filename(message)
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

    def generate_filename(self, message):
        if message.header.timestamp is not None:
            timestamp = datetime.datetime.fromtimestamp(
                message.header.timestamp)
        else:
            timestamp = datetime.datetime.now()
        variables = dict(
            date=timestamp.strftime('%Y-%m-%d'),
            msgid=message.header.message_id,
            routing_key=message.routing_key,
            # since CPython doesn't use OS-level threads, there won't be actual
            # concurrency, so we can get away with using the current time to
            # uniquify the filename -- we have to take care about the
            # precision, though: '%s' loses digits, but '%f' doesn't.
            unique='%f' % time.time(),
        )
        return self.pattern.substitute(variables)

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

    arguments = RepresentableDict(key_type=zope.schema.TextLine(),
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
