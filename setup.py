# (c) Deductive 2012-2020, all rights reserved
# This code is licensed under MIT license (see license.txt for details)
import os
from setuptools import setup

with open('README.md') as f:
    long_description = f.read()

with open(os.path.join(os.path.split(__file__)[0], 'requirements.txt')) as f:
    reqs = f.read().splitlines()

print(reqs)

setup(name='ADL_connections',
      version=0.1,
      description='Provides wrapper for ADL database connection.',
      url='https://github.com/willcollierADL/ADL_connections',
      author='ADL Smartcare',
      license='MIT',
      zip_safe=False,
      packages=['database_connection'],
      setup_requires=[
          'setuptools>=41.0.1',
          'wheel>=0.33.4',
          'numpy>=1.13.3'],
      extras_require={
          "full": reqs,
      },
      python_requires='>=3.7',
      keywords='ADLSmartcare', )
