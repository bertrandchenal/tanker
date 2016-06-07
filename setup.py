#!/usr/bin/env python
from distutils.core import setup
import os

import tanker


setup(name='Tanker',
      version=tanker.__version__,
      description='Tanker',
      author='Bertrand Chenal',
      author_email='bertrand@adimian.com',
      url='https://hg.adimian.com/tanker',
      license='MIT',
      py_modules=['tanker'],
  )
