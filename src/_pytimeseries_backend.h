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

#ifndef ___pytimeseries_backend_H
#define ___pytimeseries_backend_H

#include <timeseries.h>

typedef struct {
  PyObject_HEAD

  /* Backend Instance Handle */
  timeseries_backend_t *be;
} BackendObject;

/** Expose the BackendType structure */
PyTypeObject *_pytimeseries_backend_get_BackendType(void);

/** Expose our new function as it is not exposed to Python */
PyObject *Backend_new(timeseries_backend_t *be);

#endif /* ___pytimeseries_backend_H */
