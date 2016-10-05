#!/usr/bin/env python
from distutils.core import setup
import os

import tanker

description = '''
Tanker goal is to allow easy batch operations without compromising
database modeling. For pandas users, it's like DataFrame.to_sql on
steroids.
'''

setup(name='Tanker',
      version=tanker.__version__,
      description=description,
      author='Bertrand Chenal',
      author_email='bertrand@adimian.com',
      url='https://bitbucket.org/bertrandchenal/tanker',
      license='MIT',
      py_modules=['tanker'],
  )
