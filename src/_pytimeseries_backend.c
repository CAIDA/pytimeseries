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

#include "_pytimeseries_backend.h"

#define BackendDocstring "Timeseries Backend object"

#define BackendTypeName "_pytimeseries.Backend"

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

/* enabled? */
static PyObject *
Backend_get_enabled(BackendObject *self, void *closure)
{
  if (timeseries_backend_is_enabled(self->be) != 0) {
    Py_RETURN_TRUE;
  }

  Py_RETURN_FALSE;
}

/* id */
static PyObject *
Backend_get_id(BackendObject *self, void *closure)
{
  return Py_BuildValue("i", timeseries_backend_get_id(self->be));
}

/* name */
static PyObject *
Backend_get_name(BackendObject *self, void *closure)
{
  return PYSTR_FROMSTR(timeseries_backend_get_name(self->be));
}

static PyObject *
Backend_repr(PyObject *pyself)
{
  BackendObject *self = (BackendObject *)pyself;
  PyObject *arg_tuple = Py_BuildValue("OOO",
                                      Backend_get_id(self, NULL),
                                      Backend_get_name(self, NULL),
                                      Backend_get_enabled(self, NULL));

  PyObject *pystr =
    PYSTR_FROMSTR("<"BackendTypeName" (id: %i, name: %s, enabled: %s)>");

  return
    PyString_Format(pystr, arg_tuple);
}

static PyMethodDef Backend_methods[] = {

  {NULL}  /* Sentinel */
};

static PyGetSetDef Backend_getsetters[] = {

  /* enabled */
  {
    "enabled",
    (getter)Backend_get_enabled, NULL,
    "Enabled?",
    NULL
  },

  /* id */
  {
    "id",
    (getter)Backend_get_id, NULL,
    "ID",
    NULL
  },

  /* Name */
  {
    "name",
    (getter)Backend_get_name, NULL,
    "Name",
    NULL
  },

  {NULL} /* Sentinel */
};

static PyTypeObject BackendType = {
  PyVarObject_HEAD_INIT(NULL, 0)
  BackendTypeName,             /* tp_name */
  sizeof(BackendObject), /* tp_basicsize */
  0,                                    /* tp_itemsize */
  (destructor)Backend_dealloc,        /* tp_dealloc */
  0,                                    /* tp_print */
  0,                                    /* tp_getattr */
  0,                                    /* tp_setattr */
  0,                                    /* tp_compare */
  Backend_repr,                                    /* tp_repr */
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
  Backend_getsetters,                         /* tp_getset */
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
