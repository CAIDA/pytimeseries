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
#include "pyutils.h"

#include <timeseries.h>

#include "_pytimeseries_backend.h"

#define BackendDocstring "Timeseries Backend object"

static void
Backend_dealloc(BackendObject *self)
{
  self->be = NULL;
  Py_TYPE(self)->tp_free((PyObject*)self);
}

static int
Backend_init(BackendObject *self,
	       PyObject *args, PyObject *kwds)
{
  return 0;
}

static PyMethodDef Backend_methods[] = {

  {NULL}  /* Sentinel */
};

static PyTypeObject BackendType = {
  PyVarObject_HEAD_INIT(NULL, 0)
  "_pytimeseries.Backend",             /* tp_name */
  sizeof(BackendObject), /* tp_basicsize */
  0,                                    /* tp_itemsize */
  (destructor)Backend_dealloc,        /* tp_dealloc */
  0,                                    /* tp_print */
  0,                                    /* tp_getattr */
  0,                                    /* tp_setattr */
  0,                                    /* tp_compare */
  0,                                    /* tp_repr */
  0,                                    /* tp_as_number */
  0,                                    /* tp_as_sequence */
  0,                                    /* tp_as_mapping */
  0,                                    /* tp_hash */
  0,                                    /* tp_call */
  0,                                    /* tp_str */
  0,                                    /* tp_getattro */
  0,                                    /* tp_setattro */
  0,                                    /* tp_as_buffer */
  Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /* tp_flags */
  BackendDocstring,      /* tp_doc */
  0,		               /* tp_traverse */
  0,		               /* tp_clear */
  0,		               /* tp_richcompare */
  0,		               /* tp_weaklistoffset */
  0,		               /* tp_iter */
  0,		               /* tp_iternext */
  Backend_methods,             /* tp_methods */
  0,             /* tp_members */
  0,                         /* tp_getset */
  0,                         /* tp_base */
  0,                         /* tp_dict */
  0,                         /* tp_descr_get */
  0,                         /* tp_descr_set */
  0,                         /* tp_dictoffset */
  (initproc)Backend_init,  /* tp_init */
  0,                         /* tp_alloc */
  0,             /* tp_new */
};

PyTypeObject *_pytimeseries_backend_get_BackendType()
{
  return &BackendType;
}

/* only available to c code */
PyObject *Backend_new(timeseries_backend_t *be)
{
  BackendObject *self;

  self = (BackendObject *)(BackendType.tp_alloc(&BackendType, 0));
  if(self == NULL) {
    return NULL;
  }

  self->be = be;

  return (PyObject *)self;
}