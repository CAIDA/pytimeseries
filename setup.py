#
# Copyright (C) 2017 The Regents of the University of California.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice,
#    this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
#

from setuptools import setup, Extension, find_packages

_pytimeseries_module = Extension("_pytimeseries",
                                 libraries=["timeseries"],
                                 sources=["src/_pytimeseries_module.c",
                                          "src/_pytimeseries_timeseries.c",
                                          "src/_pytimeseries_backend.c",
                                          "src/_pytimeseries_kp.c"])

setup(name="pytimeseries",
      description="A Python interface to libtimeseries",
      version="1.0.0",
      author="Alistair King",
      author_email="corsaro-info@caida.org",
      url="http://github.com/CAIDA/pytimeseries",
      license="BSD",
      classifiers=[
          'Development Status :: 5 - Production/Stable',
          'Environment :: Console',
          'Intended Audience :: Science/Research',
          'Intended Audience :: System Administrators',
          'Intended Audience :: Telecommunications Industry',
          'Intended Audience :: Information Technology',
          'Operating System :: POSIX',
      ],
      keywords='_pytimeseries pytimeseries timeseries graphite',
      ext_modules=[_pytimeseries_module, ],
      packages=find_packages(),
      entry_points={'console_scripts': [
          'pytsk-proxy=pytimeseries.tsk.proxy:main'
      ]},
      install_requires=['confluent-kafka']
      )
