# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

from setuptools import setup, find_packages


setup(
    name='gocept.amqprun',
    version='0.1dev',
    author='gocept',
    author_email='mail@gocept.com',
    url='',
    description="""\
""",
    long_description= (
        open('README.txt').read()
        + '\n\n'
        + open('CHANGES.txt').read()),
    packages=find_packages('src'),
    package_dir = {'': 'src'},
    include_package_data = True,
    zip_safe=False,
    license='ZPL',
    namespace_packages = ['gocept'],
    install_requires=[
        'pika',
        'setuptools',
    ],
    extras_require=dict(test=[
        'amqplib',
        'zope.testing',
    ]),
)
