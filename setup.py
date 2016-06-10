from setuptools import setup, Extension

_pytimeseries_module = Extension("_pytimeseries",
                                 libraries=["timeseries"],
                                 sources=["src/_pytimeseries_module.c",
                                          "src/_pytimeseries_timeseries.c",
                                          "src/_pytimeseries_backend.c"])

setup(name="pytimeseries",
      description="A Python interface to libtimeseries",
      version="0.2.0",
      author="Alistair King",
      author_email="corsaro-info@caida.org",
      url="http://github.com/CAIDA/libtimeseries",
      license="GPLv3",
      classifiers=[
          'Development Status :: 5 - Production/Stable',
          'Environment :: Console',
          'Intended Audience :: Science/Research',
          'Intended Audience :: System Administrators',
          'Intended Audience :: Telecommunications Industry',
          'Intended Audience :: Information Technology',
          'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
          'Operating System :: POSIX',
      ],
      keywords='_pytimeseries pytimeseries timeseries graphite',
      ext_modules=[_pytimeseries_module, ])
