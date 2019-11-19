#!/usr/bin/env python
from setuptools import setup

import tanker

long_description = '''
Tanker is a Python database library targeting analytic operations but
it also fits most transactional processing.

As its core it's mainly a query builder that simplify greatly the join
operations. It also comes with an way to automatically create the
database tables based on your schema definition.

Currently Postgresql and Sqlite are supported and the API is made to
seamlessly integrate with pandas DataFrames.
'''

description = ('Tanker is a Python database library targeting analytic '
               'operations')

setup(name='Tanker',
      version=tanker.__version__,
      description=description,
      long_description=long_description,
      author='Bertrand Chenal',
      author_email='bertrand@adimian.com',
      url='https://github.com/bertrandchenal/tanker',
      license='MIT',
      py_modules=['tanker'],
      entry_points={
          'console_scripts': [
              'tk = tanker:cli',
          ],
      },
  )
