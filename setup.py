from distutils.core import setup

setup(
    name = 'linqish',
    py_modules = ['linqish'],
    version = '0.5.2',
    description = 'Iterable manipulation',
    author = 'Henri Wiechers',
    author_email = 'hwiechers@gmail.com',
    url = 'http://linqish-py.googlecode.com',
    download_url = 'http://linqish-py.googlecode.com/files/linqish-0.5.2.zip',
    keywords = ['iterable', 'linq'],
    classifiers = [
    'Programming Language :: Python :: 2.7',
    'License :: OSI Approved :: Apache Software License',
    'Operating System :: OS Independent',
    'Development Status :: 4 - Beta',
    'Intended Audience :: Developers',
    'Topic :: Software Development :: Libraries :: Python Modules'
    ],
    long_description = """\
Python module for manipulating iterables.
An implementation of the .Net Framework's Linq to Objects for Python.
""")

