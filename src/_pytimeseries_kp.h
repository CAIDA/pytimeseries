/*
 * This file is part of pytimeseries
 *
 * CAIDA, UC San Diego
 * timeseries-info@caida.org
 *
 * Copyright (C) 2012 The Regents of the University of California.
 * Authors: Alistair King, Chiara Orsini
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License as published by the Free Software
 * Foundation; either version 2 of the License, or (at your option) any later
 * version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <Python.h>

#ifndef ___pytimeseries_kp_H
#define ___pytimeseries_kp_H

#include <timeseries.h>

#include "_pytimeseries_timeseries.h"

typedef struct {
  PyObject_HEAD

  /* Keypackage Instance Handle */
  timeseries_kp_t *kp;

  /* Parent Timeseries object (we have a reference to it) */
  PyObject *TS;

} KeyPackageObject;

/** Expose the KeypackageType structure */
PyTypeObject *_pytimeseries_kp_get_KeyPackageType(void);

/** Expose our new function as it is not exposed to Python */
PyObject *KeyPackage_new(PyObject *TS, timeseries_kp_t *kp);

#endif /* ___pytimeseries_kp_H */
