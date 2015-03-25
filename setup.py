# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

from setuptools import setup, find_packages


install_requires = [
    'ZConfig',
    'pika < 0.9',
    'setuptools',
    'transaction',
    'zope.component[zcml]',
    'zope.configuration',
    'zope.event',
    'zope.interface',
    'zope.schema',
]

writefiles_require = [
    'zope.xmlpickle',
]

readfiles_require = [
    'gocept.filestore',
]

security_require = [
    'zope.security>=4.0.0dev',
]

tests_require = writefiles_require + readfiles_require + security_require + [
    'amqplib',
    'gocept.testing',
    'mock>=0.8.0',
    'plone.testing',
    'tcpwatch',
    'zope.testing',
]


setup(
    name='gocept.amqprun',
    version='0.16.0',
    author='gocept <mail at gocept dot com>',
    author_email='mail@gocept.com',
    url='https://bitbucket.org/gocept/gocept.amqprun',
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
    install_requires=install_requires,
    extras_require=dict(
        test=tests_require,
        writefiles=writefiles_require,
        readfiles=readfiles_require,
        security=security_require,
    ),
    entry_points=dict(console_scripts=[
        'server = gocept.amqprun.main:main',
    ]),
)
