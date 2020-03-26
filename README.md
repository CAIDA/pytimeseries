# PyTimeSeries

PyTimeSeries is a Python library that provides a high-performance
abstraction layer for efficiently writing to time series databases. It
is optimized for writing values for many time series simultaneously.

PyTimeSeries uses the
[libtimeseries](https://github.com/caida/libtimeseries) C library.

## Quick Start

To get started using PyTimeSeries, first
[install libtimeseries](https://github.com/caida/libtimeseries).

Then, you should be able to install PyTimeSeries.

### Installing PyTimeSeries from source

```
$ git clone https://github.com/caida/pytimeseries.git
$ cd pytimeseries
$ python setup.py build_ext
$ sudo python setup.py install
```

Use python setup.py install --user to install the library in your home directory.

## Usage

See [test/_pytimeseries_test.py](/test/_pytimeseries_test.py) for a
working example of how to use PyTimeSeries.

## Copyright and Open Source Software

Unless otherwise specified (below or in file headers) PyTimeseries is
Copyright The Regents of the University of California and released
under a BSD license. See the [LICENSE](LICENSE) file for details.

### External Dependencies

#### Required

 - [libtimeseries](https://github.com/caida/libtimeseries) is [released
   under BSD and compatible licenses](https://github.com/caida/libtimeseries#copyright-and-open-source-software).
