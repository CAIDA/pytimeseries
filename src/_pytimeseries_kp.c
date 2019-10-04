/*
 * Copyright (C) 2016 The Regents of the University of California.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
#include <Python.h>
#include "pyutils.h"

#include <timeseries.h>

#include "_pytimeseries_kp.h"

#define KeyPackageDocstring "Timeseries KeyPackage object"

#define KeyPackageTypeName "_pytimeseries.KeyPackage"

static void
KeyPackage_dealloc(KeyPackageObject *self)
{
  if (self->kp != NULL) {
    timeseries_kp_free(&self->kp);
  }

  if (self->TS != NULL) {
    Py_DECREF(self->TS);
    self->TS = NULL;
  }

  Py_TYPE(self)->tp_free((PyObject*)self);
}

static int
KeyPackage_init(KeyPackageObject *self,
	       PyObject *args, PyObject *kwds)
{
  return 0;
}

static PyObject *
KeyPackage_add_key(KeyPackageObject *self, PyObject *args)
{
  const char *key;
  int idx;

  if (!PyArg_ParseTuple(args, PT_BYTESTR, &key)) {
    return NULL;
  }

  if ((idx = timeseries_kp_add_key(self->kp, key)) < 0) {
    return NULL;
  }

  return Py_BuildValue("i", idx);
}

static PyObject *
KeyPackage_get_key(KeyPackageObject *self, PyObject *args)
{
  const char *key;
  int idx;

  if (!PyArg_ParseTuple(args, PT_BYTESTR, &key)) {
    return NULL;
  }

  if ((idx = timeseries_kp_get_key(self->kp, key)) < 0) {
    /* not a fatal error, just means the key does not exist */
    Py_RETURN_NONE;
  }

  return Py_BuildValue("i", idx);
}

static PyObject *
KeyPackage_disable_key(KeyPackageObject *self, PyObject *args)
{
  int idx;

  if (!PyArg_ParseTuple(args, "i", &idx)) {
    return NULL;
  }

  timeseries_kp_disable_key(self->kp, idx);

  Py_RETURN_NONE;
}

static PyObject *
KeyPackage_enable_key(KeyPackageObject *self, PyObject *args)
{
  int idx;

  if (!PyArg_ParseTuple(args, "i", &idx)) {
    return NULL;
  }

  timeseries_kp_enable_key(self->kp, idx);

  Py_RETURN_NONE;
}

static PyObject *
KeyPackage_get(KeyPackageObject *self, PyObject *args)
{
  int idx;
  unsigned long long val;

  if (!PyArg_ParseTuple(args, "i", &idx)) {
    return NULL;
  }

  val = timeseries_kp_get(self->kp, idx);

  return Py_BuildValue("K", val);
}

static PyObject *
KeyPackage_set(KeyPackageObject *self, PyObject *args)
{
  int idx;
  unsigned long long val;

  if (!PyArg_ParseTuple(args, "iK", &idx, &val)) {
    return NULL;
  }

  timeseries_kp_set(self->kp, idx, val);

  Py_RETURN_NONE;
}

static PyObject *
KeyPackage_resolve(KeyPackageObject *self)
{
  if (timeseries_kp_resolve(self->kp) < 0) {
    PyErr_SetString(PyExc_RuntimeError, "Failed to resolve keys");
    return NULL;
  }

  Py_RETURN_NONE;
}

static PyObject *
KeyPackage_flush(KeyPackageObject *self, PyObject *args)
{
  unsigned int time;

  if (!PyArg_ParseTuple(args, "I", &time)) {
    return NULL;
  }

  if (timeseries_kp_flush(self->kp, time) < 0) {
    PyErr_SetString(PyExc_RuntimeError, "Failed to flush keys");
    return NULL;
  }

  Py_RETURN_NONE;
}

static PyMethodDef KeyPackage_methods[] = {

  {
    "add_key",
    (PyCFunction)KeyPackage_add_key,
    METH_VARARGS,
    "Add a metric key"
  },

  {
    "get_key",
    (PyCFunction)KeyPackage_get_key,
    METH_VARARGS,
    "Get index of the given key"
  },

  {
    "disable_key",
    (PyCFunction)KeyPackage_disable_key,
    METH_VARARGS,
    "Disable the given key"
  },

  {
    "enable_key",
    (PyCFunction)KeyPackage_enable_key,
    METH_VARARGS,
    "Enable the given key"
  },

  {
    "get",
    (PyCFunction)KeyPackage_get,
    METH_VARARGS,
    "Get the current value of the given key"
  },

  {
    "set",
    (PyCFunction)KeyPackage_set,
    METH_VARARGS,
    "Set the current value of the given key"
  },

  {
    "resolve",
    (PyCFunction)KeyPackage_resolve,
    METH_NOARGS,
    "Force backends to resolve all keys in the key package"
  },

  {
    "flush",
    (PyCFunction)KeyPackage_flush,
    METH_VARARGS,
    "Flush the current values to all enabled backends"
  },

  {NULL}  /* Sentinel */
};

/* size */
static PyObject *
KeyPackage_get_size(KeyPackageObject *self, void *closure)
{
  return Py_BuildValue("i", timeseries_kp_size(self->kp));
}

/* enabled size */
static PyObject *
KeyPackage_get_enabled_size(KeyPackageObject *self, void *closure)
{
  return Py_BuildValue("i", timeseries_kp_enabled_size(self->kp));
}

static PyGetSetDef KeyPackage_getsetters[] = {

  {
    "size",
    (getter)KeyPackage_get_size, NULL,
    "Number of keys",
    NULL
  },

  {
    "enabled_size",
    (getter)KeyPackage_get_enabled_size, NULL,
    "Number of enabled keys",
    NULL
  },

  {NULL} /* Sentinel */
};

static PyTypeObject KeyPackageType = {
  PyVarObject_HEAD_INIT(NULL, 0)
  KeyPackageTypeName,             /* tp_name */
  sizeof(KeyPackageObject), /* tp_basicsize */
  0,                                    /* tp_itemsize */
  (destructor)KeyPackage_dealloc,        /* tp_dealloc */
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
  KeyPackageDocstring,      /* tp_doc */
  0,		               /* tp_traverse */
  0,		               /* tp_clear */
  0,		               /* tp_richcompare */
  0,		               /* tp_weaklistoffset */
  0,		               /* tp_iter */
  0,		               /* tp_iternext */
  KeyPackage_methods,             /* tp_methods */
  0,             /* tp_members */
  KeyPackage_getsetters,                         /* tp_getset */
  0,                         /* tp_base */
  0,                         /* tp_dict */
  0,                         /* tp_descr_get */
  0,                         /* tp_descr_set */
  0,                         /* tp_dictoffset */
  (initproc)KeyPackage_init,  /* tp_init */
  0,                         /* tp_alloc */
  0,             /* tp_new */
};

PyTypeObject *_pytimeseries_kp_get_KeyPackageType()
{
  return &KeyPackageType;
}

/* only available to c code */
PyObject *KeyPackage_new(PyObject *TS, timeseries_kp_t *kp)
{
  KeyPackageObject *self;

  self = (KeyPackageObject *)(KeyPackageType.tp_alloc(&KeyPackageType, 0));
  if(self == NULL) {
    return NULL;
  }

  self->kp = kp;

  self->TS = TS;
  Py_INCREF(self->TS);

  return (PyObject *)self;
}
