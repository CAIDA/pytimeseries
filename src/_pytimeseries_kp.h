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

#ifndef ___pytimeseries_kp_H
#define ___pytimeseries_kp_H

#include <timeseries.h>

#include "_pytimeseries_timeseries.h"

#if PY_MAJOR_VERSION >= 3
 #define PT_BYTESTR "y"
#else
 #define PT_BYTESTR "s"
#endif

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
