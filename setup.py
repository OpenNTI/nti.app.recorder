import codecs
from setuptools import setup, find_packages

entry_points = {
    "z3c.autoinclude.plugin": [
        'target = nti.app',
    ],
}

TESTS_REQUIRE = [
    'nti.app.testing',
    'nti.testing',
    'zope.dottedname',
    'zope.testrunner',
]


def _read(fname):
    with codecs.open(fname, encoding='utf-8') as f:
        return f.read()


setup(
    name='nti.app.recorder',
    version=_read('version.txt').strip(),
    author='Jason Madden',
    author_email='jason@nextthought.com',
    description="NTI Recorder Appp Layer",
    long_description=_read('README.rst'),
    license='Apache',
    keywords='recorder pyramid',
    classifiers=[
        'Framework :: Pyramid',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: Implementation :: CPython'
    ],
    url="https://github.com/NextThought/nti.app.recorder",
    zip_safe=True,
    packages=find_packages('src'),
    package_dir={'': 'src'},
    include_package_data=True,
    namespace_packages=['nti', 'nti.app'],
    tests_require=TESTS_REQUIRE,
    install_requires=[
        'setuptools',
        'nti.base',
        'nti.common',
        'nti.externalization',
        'nti.links',
        'nti.ntiids',
        'nti.recorder',
        'nti.site',
        'nti.traversal',
        'nti.zodb',
        'nti.zope_catalog',
        'pyramid',
        'requests',
        'six',
        'zc.catalog',
        'ZODB',
        'zope.annotation',
        'zope.cachedescriptors',
        'zope.component',
        'zope.dublincore',
        'zope.generations',
        'zope.interface',
        'zope.i18nmessageid',
        'zope.index',
        'zope.interface',
        'zope.intid',
        'zope.lifecycleevent',
        'zope.location',
        'zope.mimetype',
        'zope.security',
        'zope.traversing',
    ],
    extras_require={
        'test': TESTS_REQUIRE,
        'docs': [
            'Sphinx',
            'repoze.sphinx.autointerface',
            'sphinx_rtd_theme',
        ],
    },
    entry_points=entry_points,
)
