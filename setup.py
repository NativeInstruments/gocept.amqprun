# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

from setuptools import setup, find_packages


setup(
    name='gocept.amqprun',
    version='0.5.1',
    author='gocept <cz at gocept dot com>',
    author_email='cz@gocept.com',
    url='https://code.gocept.com/hg/public/gocept.amqprun',
    description="""\
gocept.amqprun helps you writing and running AMQP consumers, and sending AMQP
messages. It currently only supports AMQP 0-8 and integrates with the Zope Tool
Kit (ZTK) so you can use adapters, utilities and all the buzz.
""",
    long_description=(
        open('README.txt').read()
        + '\n\n'
        + open('CHANGES.txt').read()),
    packages=find_packages('src'),
    package_dir={'': 'src'},
    include_package_data=True,
    zip_safe=False,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Framework :: Zope3',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Zope Public License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development'
    ],
    license='ZPL',
    namespace_packages=['gocept'],
    install_requires=[
        'ZConfig',
        'pika < 0.9',
        'setuptools',
        'transaction',
        'zope.component[zcml]',
        'zope.configuration',
        'zope.event',
        'zope.interface',
        'zope.schema',
    ],
    extras_require=dict(
        test=[
            'amqplib',
            'mock>=0.7.0',
            'plone.testing',
            'tcpwatch',
            'zope.testing',
            ],
        writefiles=[
            'amqplib',
            'zope.xmlpickle',
            ],
        filestore=[
            'gocept.filestore',
            ]),
    entry_points=dict(console_scripts=[
        'server = gocept.amqprun.main:main',
        'filestore-reader = gocept.amqprun.filestore:main',
    ]),
)
