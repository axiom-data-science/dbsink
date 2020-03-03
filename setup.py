from setuptools import setup, find_packages


def version():
    with open('VERSION') as f:
        return f.read().strip()


def readme():
    with open('README.md', 'rb') as f:
        return f.read().decode('utf-8', errors='ignore')


reqs = [line.strip() for line in open('requirements.txt') if not line.startswith('#')]
# `msgpack-python` and `msgpack` have different names between pypa and conda
# Fix any libraries that end in '-python'
# import re
# reqs = [ re.sub(r'-python$', '', r) for r in reqs ]

setup(
    name                          = "dbsink",
    version                       = version(),
    description                   = "Sink kafka messages to a database table",
    long_description              = readme(),
    long_description_content_type = 'text/markdown',
    license                       = 'MIT',
    author                        = "Kyle Wilcox",
    author_email                  = "kyle@axds.co",
    url                           = "https://github.com/axiom-data-science/dbsink",
    packages                      = find_packages(),
    install_requires              = reqs,
    entry_points                  = {
        'console_scripts': [
            'dbsink = dbsink.listen:run'
        ],
        'dbsink.maps': [
            'JsonMap               = dbsink.maps:JsonMap',
            'StringMap             = dbsink.maps:StringMap',
            'GenericGeography      = dbsink.tables:GenericGeography',
            'GenericFloat          = dbsink.tables:GenericFloat',
            'AreteData             = dbsink.tables:AreteData',
            'NumurusData           = dbsink.tables:NumurusData',
            'NumurusStatus         = dbsink.tables:NumurusStatus',
            'NwicFloatReports      = dbsink.tables:NwicFloatReports',
            'GenericFieldStatistic = dbsink.tables:GenericFieldStatistic'
        ]
    },
    classifiers         = [
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python',
        'Topic :: Scientific/Engineering',
    ]
)
