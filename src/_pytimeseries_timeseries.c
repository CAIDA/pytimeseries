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
#include "_pytimeseries_kp.h"

typedef struct {
  PyObject_HEAD

  /* BGP Stream Instance Handle */
  timeseries_t *ts;
} TimeseriesObject;

#define TimeseriesDocstring "Timeseries object"

#define TimeseriesTypeName "_pytimeseries.Timeseries"


static void
Timeseries_dealloc(TimeseriesObject *self)
{
  if (self->ts != NULL) {
      timeseries_free(&self->ts);
  }
  Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject *
Timeseries_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
  TimeseriesObject *self;

  self = (TimeseriesObject *)type->tp_alloc(type, 0);
  if(self == NULL) {
    return NULL;
  }

  if ((self->ts = timeseries_init()) == NULL) {
    Py_DECREF(self);
    return NULL;
  }

  return (PyObject *)self;
}

static int
Timeseries_init(TimeseriesObject *self,
	       PyObject *args, PyObject *kwds)
{
  return 0;
}

/** Enable the given Backend */
static PyObject *
Timeseries_enable_backend(TimeseriesObject *self, PyObject *args)
{
  BackendObject *pybe = NULL;
  const char *optstr = NULL;

  /* get the Backend argument */
  if (!PyArg_ParseTuple(args, "O!|s",
                        _pytimeseries_backend_get_BackendType(),
                        &pybe, &optstr)) {
    return NULL;
  }

  if (!pybe->be) {
    PyErr_SetString(PyExc_RuntimeError, "Invalid Timeseries Backend object");
    return NULL;
  }

  if (timeseries_enable_backend(pybe->be, optstr) == 0) {
    Py_RETURN_TRUE;
  }

  Py_RETURN_FALSE;
}

/** Get the backend with the given ID */
static PyObject *
Timeseries_get_backend_by_id(TimeseriesObject *self, PyObject *args)
{
  int id = -1;
  timeseries_backend_t *be;

  /* get the ID argument */
  if (!PyArg_ParseTuple(args, "i", &id)) {
    return NULL;
  }

  if ((be = timeseries_get_backend_by_id(self->ts, id)) == NULL) {
    Py_RETURN_NONE;
  }

  return Backend_new(be);
}

/** Get the backend with the given name */
static PyObject *
Timeseries_get_backend_by_name(TimeseriesObject *self, PyObject *args)
{
  const char *namestr;
  timeseries_backend_t *be;

  /* get the name argument */
  if (!PyArg_ParseTuple(args, "s", &namestr)) {
    return NULL;
  }

  if ((be = timeseries_get_backend_by_name(self->ts, namestr)) == NULL) {
    Py_RETURN_NONE;
  }

  return Backend_new(be);
}

/** Get the all available backends */
static PyObject *
Timeseries_get_all_backends(TimeseriesObject *self)
{
  timeseries_backend_t **bes;
  PyObject *list;
  int i;

  /* get the array from libtimeseries */
  if ((bes = timeseries_get_all_backends(self->ts)) == NULL) {
    return NULL;
  }

  /* create a list */
  if((list = PyList_New(0)) == NULL)
    return NULL;

  for(i=0; i<TIMESERIES_BACKEND_ID_LAST; i++) {
    if (bes[i] != NULL) {
      /* add backend to list */
      if(PyList_Append(list, Backend_new(bes[i])) == -1) {
        return NULL;
      }
    }
  }

  return list;
}

/* Set a single data point */
static PyObject *
Timeseries_set_single(TimeseriesObject *self, PyObject *args)
{
  const char *key;
  unsigned long long value;
  unsigned long time;

  if (!PyArg_ParseTuple(args, "sKk", &key, &value, &time)) {
    return NULL;
  }

  if (timeseries_set_single(self->ts, key, value, time) != 0) {
    PyErr_SetString(PyExc_RuntimeError, "Failed to set single key");
    return NULL;
  }

  Py_RETURN_NONE;
}

/* Create a new key package */
static PyObject *
Timeseries_new_keypackage(TimeseriesObject *self,
                          PyObject *args, PyObject *keywds)
{
  static char *kwlist[] = {
    "reset", //
    "disable", //
    NULL //
  };
  int reset = 0;
  int disable = 0;
  timeseries_kp_t *kp;

  if (!PyArg_ParseTupleAndKeywords(args, keywds, "|ii", kwlist,
                                   &reset, &disable)) {
    return NULL;
  }

  int flags = 0;
  if (reset) {
    flags |= TIMESERIES_KP_RESET;
  }
  if (disable) {
    flags |= TIMESERIES_KP_DISABLE;
  }

  if ((kp = timeseries_kp_init(self->ts, flags)) == NULL) {
    return NULL;
  }

  return KeyPackage_new((PyObject*)self, kp);
}

static PyMethodDef Timeseries_methods[] = {

  {
    "enable_backend",
    (PyCFunction)Timeseries_enable_backend,
    METH_VARARGS,
    "Enable the given timeseries Backend"
  },

  {
    "get_backend_by_id",
    (PyCFunction)Timeseries_get_backend_by_id,
    METH_VARARGS,
    "Get the backend with the given ID"
  },

  {
    "get_backend_by_name",
    (PyCFunction)Timeseries_get_backend_by_name,
    METH_VARARGS,
    "Get the backend with the given name"
  },

  {
    "get_all_backends",
    (PyCFunction)Timeseries_get_all_backends,
    METH_NOARGS,
    "Get a list of all available backends"
  },

  {
    "set_single",
    (PyCFunction)Timeseries_set_single,
    METH_VARARGS,
    "Set a value for a single timeseries key"
  },

  {
    "new_keypackage",
    (PyCFunction)Timeseries_new_keypackage,
    METH_VARARGS | METH_KEYWORDS,
    "Create a new Key Package"
  },


  {NULL}  /* Sentinel */
};

static PyTypeObject TimeseriesType = {
  PyVarObject_HEAD_INIT(NULL, 0)
  TimeseriesTypeName,             /* tp_name */
  sizeof(TimeseriesObject), /* tp_basicsize */
  0,                                    /* tp_itemsize */
  (destructor)Timeseries_dealloc,        /* tp_dealloc */
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
  TimeseriesDocstring,      /* tp_doc */
  0,		               /* tp_traverse */
  0,		               /* tp_clear */
  0,		               /* tp_richcompare */
  0,		               /* tp_weaklistoffset */
  0,		               /* tp_iter */
  0,		               /* tp_iternext */
  Timeseries_methods,             /* tp_methods */
  0,             /* tp_members */
  0,                         /* tp_getset */
  0,                         /* tp_base */
  0,                         /* tp_dict */
  0,                         /* tp_descr_get */
  0,                         /* tp_descr_set */
  0,                         /* tp_dictoffset */
  (initproc)Timeseries_init,  /* tp_init */
  0,                         /* tp_alloc */
  Timeseries_new,             /* tp_new */
};

PyTypeObject *_pytimeseries_timeseries_get_TimeseriesType()
{
  return &TimeseriesType;
}
