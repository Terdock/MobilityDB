/*****************************************************************************
 *
 * This MobilityDB code is provided under The PostgreSQL License.
 *
 * Copyright (c) 2016-2021, Université libre de Bruxelles and MobilityDB
 * contributors
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written
 * agreement is hereby granted, provided that the above copyright notice and
 * this paragraph and the following two paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL UNIVERSITE LIBRE DE BRUXELLES BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
 * EVEN IF UNIVERSITE LIBRE DE BRUXELLES HAS BEEN ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 * UNIVERSITE LIBRE DE BRUXELLES SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON
 * AN "AS IS" BASIS, AND UNIVERSITE LIBRE DE BRUXELLES HAS NO OBLIGATIONS TO
 * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS. 
 *
 *****************************************************************************/

/**
 * @file stbox.c
 * Functions for spatiotemporal bounding boxes.
 */

#include "stbox.h"

#include <assert.h>
#include <funcapi.h>
#include <utils/builtins.h>
#include <utils/timestamp.h>

#include "period.h"
#include "timestampset.h"
#include "periodset.h"
#include "temporal_util.h"
#include "tnumber_mathfuncs.h"
#include "tpoint.h"
#include "tpoint_parser.h"
#include "tpoint_spatialfuncs.h"

/* Buffer size for input and output of STBOX */
#define MAXSTBOXLEN    256

/*****************************************************************************
 * Miscellaneous functions
 *****************************************************************************/

/**
 * Constructs a newly allocated spatiotemporal box
 */
STBOX *
stbox_make(bool hasx, bool hasz, bool hast, bool geodetic, int32 srid,
  double xmin, double xmax, double ymin, double ymax, double zmin,
  double zmax, TimestampTz tmin, TimestampTz tmax)
{
  STBOX *result = palloc0(sizeof(STBOX));
  MOBDB_FLAGS_SET_X(result->flags, hasx);
  MOBDB_FLAGS_SET_Z(result->flags, hasz);
  MOBDB_FLAGS_SET_T(result->flags, hast);
  MOBDB_FLAGS_SET_GEODETIC(result->flags, geodetic);
  result->srid = srid;

  /* Process X min/max */
  if (hasx)
  {
    double tmp;
    if (xmin > xmax)
    {
      tmp = xmin;
      xmin = xmax;
      xmax = tmp;
    }
    result->xmin = xmin;
    result->xmax = xmax;

    /* Process Y min/max */
    if (ymin > ymax)
    {
      tmp = ymin;
      ymin = ymax;
      ymax = tmp;
    }
    result->ymin = ymin;
    result->ymax = ymax;

    if (hasz || geodetic)
    {
      /* Process Z min/max */
      if (zmin > zmax)
      {
        tmp = zmin;
        zmin = zmax;
        zmax = tmp;
      }
      result->zmin = zmin;
      result->zmax = zmax;
    }
  }

  if (hast)
  {
    TimestampTz ttmp;
    /* Process T min/max */
    if (tmin > tmax)
    {
      ttmp = tmin;
      tmin = tmax;
      tmax = ttmp;
    }
    result->tmin = tmin;
    result->tmax = tmax;
  }
  return result;
}

/**
 * Returns a copy of the spatiotemporal box
 */
STBOX *
stbox_copy(const STBOX *box)
{
  STBOX *result = palloc0(sizeof(STBOX));
  memcpy(result, box, sizeof(STBOX));
  return result;
}

/**
 * Expand the first spatiotemporal box with the second one
 *
 * @pre No tests are made concerning the srid, dimensionality, etc.
 * This should be ensured by the calling function.
 */
void
stbox_expand(STBOX *box1, const STBOX *box2)
{
  if (MOBDB_FLAGS_GET_X(box1->flags))
  {
    box1->xmin = Min(box1->xmin, box2->xmin);
    box1->xmax = Max(box1->xmax, box2->xmax);
    box1->ymin = Min(box1->ymin, box2->ymin);
    box1->ymax = Max(box1->ymax, box2->ymax);
    if (MOBDB_FLAGS_GET_Z(box1->flags) ||
      MOBDB_FLAGS_GET_GEODETIC(box1->flags))
    {
      box1->zmin = Min(box1->zmin, box2->zmin);
      box1->zmax = Max(box1->zmax, box2->zmax);
    }
  }
  if (MOBDB_FLAGS_GET_T(box1->flags))
  {
    box1->tmin = Min(box1->tmin, box2->tmin);
    box1->tmax = Max(box1->tmax, box2->tmax);
  }
}

/**
 * Shift and/or scale the time span of the spatiotemporal box by the interval
 */
void
stbox_shift_tscale(STBOX *box, const Interval *start, const Interval *duration)
{
  assert(start != NULL || duration != NULL);
  if (start != NULL)
    box->tmin = DatumGetTimestampTz(DirectFunctionCall2(
      timestamptz_pl_interval, TimestampTzGetDatum(box->tmin),
      PointerGetDatum(start)));
  box->tmax = (duration == NULL) ?
    DatumGetTimestampTz(DirectFunctionCall2(timestamptz_pl_interval,
      TimestampTzGetDatum(box->tmax), PointerGetDatum(start))) :
    DatumGetTimestampTz(DirectFunctionCall2(timestamptz_pl_interval,
       TimestampTzGetDatum(box->tmin), PointerGetDatum(duration)));
  return;
}

/**
 * Constructs a newly allocated GBOX
 */
GBOX *
gbox_make(bool hasz, bool hasm, bool geodetic, double xmin, double xmax,
  double ymin, double ymax, double zmin, double zmax)
{
  GBOX *result = palloc0(sizeof(GBOX));
  result->xmin = xmin;
  result->xmax = xmax;
  result->ymin = ymin;
  result->ymax = ymax;
  result->zmin = zmin;
  result->zmax = zmax;
  FLAGS_SET_Z(result->flags, hasz);
  FLAGS_SET_M(result->flags, hasm);
  FLAGS_SET_GEODETIC(result->flags, geodetic);
  return result;
}

/*****************************************************************************
 * Parameter tests
 *****************************************************************************/

/**
 * Ensure that the temporal value has XY dimension
 */
void
ensure_has_X_stbox(const STBOX *box)
{
  if (! MOBDB_FLAGS_GET_X(box->flags))
    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
      errmsg("The box must have XY dimension")));
  return;
}

/**
 * Ensure that the temporal value has T dimension
 */
void
ensure_has_T_stbox(const STBOX *box)
{
  if (! MOBDB_FLAGS_GET_T(box->flags))
    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
      errmsg("The box must have time dimension")));
  return;
}

/**
 * Ensure that the temporal value has XY dimension
 */
void
ensure_not_geodetic_stbox(const STBOX *box)
{
  if (MOBDB_FLAGS_GET_GEODETIC(box->flags))
    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
      errmsg("The box cannot be geodetic")));
  return;
}

/*****************************************************************************
 * Input/Ouput functions
 *****************************************************************************/

PG_FUNCTION_INFO_V1(stbox_in);
/**
 * Input function for spatiotemporal boxes.
 *
 * Examples of input:
 * @code
 * STBOX((1.0, 2.0), (3.0, 4.0)) -> only spatial
 * STBOX Z((1.0, 2.0, 3.0), (4.0, 5.0, 6.0)) -> only spatial
 * STBOX T((1.0, 2.0, 2001-01-01), (3.0, 4.0, 2001-01-02)) -> spatiotemporal
 * STBOX ZT((1.0, 2.0, 3.0, 2001-01-01), (4.0, 5.0, 6.0, 2001-01-02)) -> spatiotemporal
 * STBOX T(( , , 2001-01-01), ( , , 2001-01-02)) -> only temporal
 * SRID=xxxx;STBOX... (any of the above)
 * GEODSTBOX((1.0, 2.0, 3.0), (4.0, 5.0, 6.0)) -> only spatial
 * GEODSTBOX T((1.0, 2.0, 3.0, 2001-01-01), (4.0, 5.0, 6.0, 2001-01-02)) -> spatiotemporal
 * GEODSTBOX T(( , , 2001-01-01), ( , , 2001-01-02)) -> only temporal
 * SRID=xxxx;GEODSTBOX... (any of the above)
 * @endcode
 * where the commas are optional and the SRID is optional. If the SRID is not
 * stated it is by default 0 for non geodetic boxes and 4326 for geodetic boxes
 */
PGDLLEXPORT Datum
stbox_in(PG_FUNCTION_ARGS)
{
  char *input = PG_GETARG_CSTRING(0);
  STBOX *result = stbox_parse(&input);
  PG_RETURN_POINTER(result);
}

/**
 * Returns the string representation of the spatiotemporal box
 */
static char *
stbox_to_string(const STBOX *box)
{
  static size_t size = MAXSTBOXLEN + 1;
  char *str, *xmin = NULL, *xmax = NULL, *ymin = NULL, *ymax = NULL,
    *zmin = NULL, *zmax = NULL, *tmin = NULL, *tmax = NULL;
  bool hasx = MOBDB_FLAGS_GET_X(box->flags);
  bool hasz = MOBDB_FLAGS_GET_Z(box->flags);
  bool hast = MOBDB_FLAGS_GET_T(box->flags);
  bool geodetic = MOBDB_FLAGS_GET_GEODETIC(box->flags);

  str = (char *) palloc(size);
  char srid[20];
  if (hasx && box->srid > 0)
    sprintf(srid, "SRID=%d;", box->srid);
  else
    srid[0] = '\0';
  char *boxtype = geodetic ? "GEODSTBOX" : "STBOX";
  assert(hasx || hast);
  if (hasx)
  {
    xmin = call_output(FLOAT8OID, Float8GetDatum(box->xmin));
    xmax = call_output(FLOAT8OID, Float8GetDatum(box->xmax));
    ymin = call_output(FLOAT8OID, Float8GetDatum(box->ymin));
    ymax = call_output(FLOAT8OID, Float8GetDatum(box->ymax));
    if (geodetic || hasz)
    {
      zmin = call_output(FLOAT8OID, Float8GetDatum(box->zmin));
      zmax = call_output(FLOAT8OID, Float8GetDatum(box->zmax));
    }
  }
  if (hast)
  {
    tmin = call_output(TIMESTAMPTZOID, TimestampTzGetDatum(box->tmin));
    tmax = call_output(TIMESTAMPTZOID, TimestampTzGetDatum(box->tmax));
  }
  if (hasx)
  {
    if (geodetic)
    {
      char *Z;
      if (hast)
      {
        Z = hasz ? "Z" : "";
        snprintf(str, size, "%s%s %sT((%s,%s,%s,%s),(%s,%s,%s,%s))",
          srid, boxtype, Z, xmin, ymin, zmin, tmin, xmax, ymax, zmax, tmax);
      }
      else
      {
        Z = hasz ? " Z" : "";
        snprintf(str, size, "%s%s%s((%s,%s,%s),(%s,%s,%s))",
          srid, boxtype, Z, xmin, ymin, zmin, xmax, ymax, zmax);
      }
    }
    else if (hasz && hast)
      snprintf(str, size, "%s%s ZT((%s,%s,%s,%s),(%s,%s,%s,%s))",
        srid, boxtype, xmin, ymin, zmin, tmin, xmax, ymax, zmax, tmax);
    else if (hasz)
      snprintf(str, size, "%s%s Z((%s,%s,%s),(%s,%s,%s))",
        srid, boxtype, xmin, ymin, zmin, xmax, ymax, zmax);
    else if (hast)
      snprintf(str, size, "%s%s T((%s,%s,%s),(%s,%s,%s))",
        srid, boxtype, xmin, ymin, tmin, xmax, ymax, tmax);
    else
      snprintf(str, size, "%s%s((%s,%s),(%s,%s))",
        srid, boxtype, xmin, ymin, xmax, ymax);
  }
  else
    /* Missing spatial dimension */
    snprintf(str, size, "%s%s T((,,%s),(,,%s))", srid, boxtype, tmin, tmax);
  if (hasx)
  {
    pfree(xmin); pfree(xmax);
    pfree(ymin); pfree(ymax);
    if (hasz)
    {
      pfree(zmin); pfree(zmax);
    }
  }
  if (hast)
  {
    pfree(tmin); pfree(tmax);
  }
  return str;
}

PG_FUNCTION_INFO_V1(stbox_out);
/**
 * Output function for spatiotemporal boxes.
 */
PGDLLEXPORT Datum
stbox_out(PG_FUNCTION_ARGS)
{
  STBOX *box = PG_GETARG_STBOX_P(0);
  char *result = stbox_to_string(box);
  PG_RETURN_CSTRING(result);
}

/*****************************************************************************
 * Constructor functions
 *****************************************************************************/

/**
 * Construct a spatiotemporal box from the arguments
 */
static Datum
stbox_constructor1(FunctionCallInfo fcinfo, bool hasx, bool hasz, bool hast,
  bool geodetic)
{
  double xmin = 0, xmax = 0, ymin = 0, ymax = 0, zmin = 0, zmax = 0;
  TimestampTz tmin = 0, tmax = 0;
  int srid = 0; /* make Codacy quiet */

  if (!hasx && hast)
  {
    tmin = PG_GETARG_TIMESTAMPTZ(0);
    tmax = PG_GETARG_TIMESTAMPTZ(1);
    srid = PG_GETARG_INT32(2);
  }
  else if (hasx && !hasz && !geodetic && !hast)
  {
    xmin = PG_GETARG_FLOAT8(0);
    ymin = PG_GETARG_FLOAT8(1);
    xmax = PG_GETARG_FLOAT8(2);
    ymax = PG_GETARG_FLOAT8(3);
    srid = PG_GETARG_INT32(4);
  }
  else if (hasx && (hasz || geodetic) && !hast)
  {
    xmin = PG_GETARG_FLOAT8(0);
    ymin = PG_GETARG_FLOAT8(1);
    zmin = PG_GETARG_FLOAT8(2);
    xmax = PG_GETARG_FLOAT8(3);
    ymax = PG_GETARG_FLOAT8(4);
    zmax = PG_GETARG_FLOAT8(5);
    srid = PG_GETARG_INT32(6);
  }
  else if (hasx && !hasz && !geodetic && hast)
  {
    xmin = PG_GETARG_FLOAT8(0);
    ymin = PG_GETARG_FLOAT8(1);
    tmin = PG_GETARG_TIMESTAMPTZ(2);
    xmax = PG_GETARG_FLOAT8(3);
    ymax = PG_GETARG_FLOAT8(4);
    tmax = PG_GETARG_TIMESTAMPTZ(5);
    srid = PG_GETARG_INT32(6);
  }
  else /* hasx && (hasz || geodetic) && hast) */
  {
    xmin = PG_GETARG_FLOAT8(0);
    ymin = PG_GETARG_FLOAT8(1);
    zmin = PG_GETARG_FLOAT8(2);
    tmin = PG_GETARG_TIMESTAMPTZ(3);
    xmax = PG_GETARG_FLOAT8(4);
    ymax = PG_GETARG_FLOAT8(5);
    zmax = PG_GETARG_FLOAT8(6);
    tmax = PG_GETARG_TIMESTAMPTZ(7);
    srid = PG_GETARG_INT32(8);
  }

  /* Construct the box */
  STBOX *result = stbox_make(hasx, hasz, hast, geodetic, srid,
    xmin, xmax, ymin, ymax, zmin, zmax, tmin, tmax);
  PG_RETURN_POINTER(result);
}

/*****************************************************************************/

PG_FUNCTION_INFO_V1(stbox_constructor_t);
/**
 * Construct a spatiotemporal box from the arguments
 */
PGDLLEXPORT Datum
stbox_constructor_t(PG_FUNCTION_ARGS)
{
  if (PG_NARGS() > 3)
    return stbox_constructor1(fcinfo, true, false, true, false);
  return stbox_constructor1(fcinfo, false, false, true, false);
}

PG_FUNCTION_INFO_V1(stbox_constructor);
/**
 * Construct a spatiotemporal box from the arguments
 */
PGDLLEXPORT Datum
stbox_constructor(PG_FUNCTION_ARGS)
{
  return stbox_constructor1(fcinfo, true, false, false, false);
}

PG_FUNCTION_INFO_V1(stbox_constructor_z);
/**
 * Construct a spatiotemporal box from the arguments
 */
PGDLLEXPORT Datum
stbox_constructor_z(PG_FUNCTION_ARGS)
{
  return stbox_constructor1(fcinfo, true, true, false, false);
}

PG_FUNCTION_INFO_V1(stbox_constructor_zt);
/**
 * Construct a spatiotemporal box from the arguments
 */
PGDLLEXPORT Datum
stbox_constructor_zt(PG_FUNCTION_ARGS)
{
  return stbox_constructor1(fcinfo, true, true, true, false);
}

/* The names of the SQL and C functions are different, otherwise there is
 * ambiguity and explicit casting of the arguments to ::timestamptz is needed */

PG_FUNCTION_INFO_V1(geodstbox_constructor_t);
/**
 * Construct a spatiotemporal box from the arguments
 */
PGDLLEXPORT Datum
geodstbox_constructor_t(PG_FUNCTION_ARGS)
{
  if (PG_NARGS() > 3)
    return stbox_constructor1(fcinfo, true, false, true, true);
  return stbox_constructor1(fcinfo, false, false, true, true);
}

PG_FUNCTION_INFO_V1(geodstbox_constructor);
/**
 * Construct a spatiotemporal box from the arguments
 */
PGDLLEXPORT Datum
geodstbox_constructor(PG_FUNCTION_ARGS)
{
  return stbox_constructor1(fcinfo, true, false, false, true);
}

PG_FUNCTION_INFO_V1(geodstbox_constructor_z);
/**
 * Construct a spatiotemporal box from the arguments
 */
PGDLLEXPORT Datum
geodstbox_constructor_z(PG_FUNCTION_ARGS)
{
  return stbox_constructor1(fcinfo, true, true, false, true);
}

PG_FUNCTION_INFO_V1(geodstbox_constructor_zt);
/**
 * Construct a spatiotemporal box from the arguments
 */
PGDLLEXPORT Datum
geodstbox_constructor_zt(PG_FUNCTION_ARGS)
{
  return stbox_constructor1(fcinfo, true, true, true, true);
}

/*****************************************************************************
 * Casting
 *****************************************************************************/

/**
 * Cast the spatiotemporal box as a GBOX value for PostGIS
 */
GBOX *
stbox_to_gbox(const STBOX *box)
{
  assert(MOBDB_FLAGS_GET_X(box->flags));
  return gbox_make(MOBDB_FLAGS_GET_Z(box->flags),
    FLAGS_GET_GEODETIC(box->flags), false, box->xmin, box->xmax,
    box->ymin, box->ymax, box->zmin, box->zmax);
}

PG_FUNCTION_INFO_V1(stbox_to_period);
/**
 * Cast the spatiotemporal box as a period
 */
PGDLLEXPORT Datum
stbox_to_period(PG_FUNCTION_ARGS)
{
  STBOX *box = PG_GETARG_STBOX_P(0);
  if (!MOBDB_FLAGS_GET_T(box->flags))
    elog(ERROR, "The box does not have time dimension");

  Period *result = period_make(box->tmin, box->tmax, true, true);
  PG_RETURN_POINTER(result);
}

/* Cast an STBOX as a box2d */

PG_FUNCTION_INFO_V1(stbox_to_box2d);
/**
 * Cast the spatiotemporal box as a GBOX value for PostGIS
 */
PGDLLEXPORT Datum
stbox_to_box2d(PG_FUNCTION_ARGS)
{
  STBOX *box = PG_GETARG_STBOX_P(0);
  if (!MOBDB_FLAGS_GET_X(box->flags))
    elog(ERROR, "The box does not have XY(Z) dimensions");
  GBOX *result = stbox_to_gbox(box);
  PG_RETURN_POINTER(result);
}

BOX3D *
stbox_to_box3d_internal(const STBOX *box)
{
  if (!MOBDB_FLAGS_GET_X(box->flags))
    elog(ERROR, "The box does not have XY(Z) dimensions");

  /* Initialize existing dimensions */
  BOX3D *result = palloc0(sizeof(BOX3D));
  result->xmin = box->xmin;
  result->xmax = box->xmax;
  result->ymin = box->ymin;
  result->ymax = box->ymax;
  if (MOBDB_FLAGS_GET_Z(box->flags))
  {
    result->zmin = box->zmin;
    result->zmax = box->zmax;
  }
  result->srid = box->srid;
  return result;
}

PG_FUNCTION_INFO_V1(stbox_to_box3d);
/**
 * Cast the spatiotemporal box as a BOX3D value for PostGIS
 */
PGDLLEXPORT Datum
stbox_to_box3d(PG_FUNCTION_ARGS)
{
  STBOX *box = PG_GETARG_STBOX_P(0);
  BOX3D *result = stbox_to_box3d_internal(box);
  PG_RETURN_POINTER(result);
}

/*****************************************************************************
 * Transform a <Type> to a STBOX
 * The functions assume that the argument box is set to 0 before with palloc0
 *****************************************************************************/

PG_FUNCTION_INFO_V1(box2d_to_stbox);
/**
 * Transform a box2d to a spatiotemporal box
 */
PGDLLEXPORT Datum
box2d_to_stbox(PG_FUNCTION_ARGS)
{
  GBOX *box = (GBOX *)PG_GETARG_POINTER(0);
  STBOX *result = stbox_make(true, false, false, false, 0,
    box->xmin, box->xmax, box->ymin, box->ymax, 0, 0, 0, 0);
  PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(box3d_to_stbox);
/**
 * Transform a box3d to a spatiotemporal box
 */
PGDLLEXPORT Datum
box3d_to_stbox(PG_FUNCTION_ARGS)
{
  BOX3D *box = (BOX3D *)PG_GETARG_POINTER(0);
  STBOX *result = stbox_make(true, true, false, false, box->srid, box->xmin,
    box->xmax, box->ymin, box->ymax, box->zmin, box->zmax, 0, 0);
  PG_RETURN_POINTER(result);
}

/**
 * Transform a geometry/geography to a spatiotemporal box
 * (internal function)
 */
bool
geo_to_stbox_internal(STBOX *box, const GSERIALIZED *gs)
{
  GBOX gbox;
  if (gserialized_get_gbox_p(gs, &gbox) == LW_FAILURE)
  {
    /* Spatial dimensions are set as missing for the SP-GiST index */
    MOBDB_FLAGS_SET_X(box->flags, false);
    MOBDB_FLAGS_SET_Z(box->flags, false);
    MOBDB_FLAGS_SET_T(box->flags, false);
    return false;
  }
  box->xmin = gbox.xmin;
  box->xmax = gbox.xmax;
  box->ymin = gbox.ymin;
  box->ymax = gbox.ymax;
  if (FLAGS_GET_Z(gs->flags) || FLAGS_GET_GEODETIC(gs->flags))
  {
    box->zmin = gbox.zmin;
    box->zmax = gbox.zmax;
  }
  box->srid = gserialized_get_srid(gs);
  MOBDB_FLAGS_SET_X(box->flags, true);
  MOBDB_FLAGS_SET_Z(box->flags, FLAGS_GET_Z(gs->flags));
  MOBDB_FLAGS_SET_T(box->flags, false);
  MOBDB_FLAGS_SET_GEODETIC(box->flags, FLAGS_GET_GEODETIC(gs->flags));
  return true;
}

PG_FUNCTION_INFO_V1(geo_to_stbox);
/**
 * Transform a geometry/geography to a spatiotemporal box
 */
PGDLLEXPORT Datum
geo_to_stbox(PG_FUNCTION_ARGS)
{
  GSERIALIZED *gs = PG_GETARG_GSERIALIZED_P(0);
  if (gserialized_is_empty(gs))
    PG_RETURN_NULL();
  STBOX *result = palloc0(sizeof(STBOX));
  geo_to_stbox_internal(result, gs);
  PG_FREE_IF_COPY(gs, 0);
  PG_RETURN_POINTER(result);
}

/**
 * Transform a timestampt to a spatiotemporal box
 * (internal function)
 */
void
timestamp_to_stbox_internal(STBOX *box, TimestampTz t)
{
  box->tmin = box->tmax = t;
  MOBDB_FLAGS_SET_X(box->flags, false);
  MOBDB_FLAGS_SET_Z(box->flags, false);
  MOBDB_FLAGS_SET_T(box->flags, true);
}

PG_FUNCTION_INFO_V1(timestamp_to_stbox);
/**
 * Transform a timestampt to a spatiotemporal box
 */
PGDLLEXPORT Datum
timestamp_to_stbox(PG_FUNCTION_ARGS)
{
  TimestampTz t = PG_GETARG_TIMESTAMPTZ(0);
  STBOX *result = palloc0(sizeof(STBOX));
  timestamp_to_stbox_internal(result, t);
  PG_RETURN_POINTER(result);
}

/**
 * Transform a timestamp set to a spatiotemporal box
 * (internal function)
 */
void
timestampset_to_stbox_internal(STBOX *box, const TimestampSet *ts)
{
  const Period *p = timestampset_bbox_ptr(ts);
  box->tmin = p->lower;
  box->tmax = p->upper;
  MOBDB_FLAGS_SET_T(box->flags, true);
}

PG_FUNCTION_INFO_V1(timestampset_to_stbox);
/**
 * Transform a timestamp set to a spatiotemporal box
 */
PGDLLEXPORT Datum
timestampset_to_stbox(PG_FUNCTION_ARGS)
{
  TimestampSet *ts = PG_GETARG_TIMESTAMPSET(0);
  STBOX *result = palloc0(sizeof(STBOX));
  timestampset_to_stbox_internal(result, ts);
  PG_FREE_IF_COPY(ts, 0);
  PG_RETURN_POINTER(result);
}

/**
 * Transform a period to a spatiotemporal box
 * (internal function)
 */
void
period_to_stbox_internal(STBOX *box, const Period *p)
{
  box->tmin = p->lower;
  box->tmax = p->upper;
  MOBDB_FLAGS_SET_T(box->flags, true);
}

PG_FUNCTION_INFO_V1(period_to_stbox);
/**
 * Transform a period to a spatiotemporal box
 */
PGDLLEXPORT Datum
period_to_stbox(PG_FUNCTION_ARGS)
{
  Period *p = PG_GETARG_PERIOD(0);
  STBOX *result = palloc0(sizeof(STBOX));
  period_to_stbox_internal(result, p);
  PG_RETURN_POINTER(result);
}

/**
 * Transform a period set to a spatiotemporal box
 * (internal function)
 */
void
periodset_to_stbox_internal(STBOX *box, const PeriodSet *ps)
{
  const Period *p = periodset_bbox_ptr(ps);
  box->tmin = p->lower;
  box->tmax = p->upper;
  MOBDB_FLAGS_SET_T(box->flags, true);
}

PG_FUNCTION_INFO_V1(periodset_to_stbox);
/**
 * Transform a period set to a spatiotemporal box
 */
PGDLLEXPORT Datum
periodset_to_stbox(PG_FUNCTION_ARGS)
{
  PeriodSet *ps = PG_GETARG_PERIODSET(0);
  STBOX *result = palloc0(sizeof(STBOX));
  periodset_to_stbox_internal(result, ps);
  PG_FREE_IF_COPY(ps, 0);
  PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(geo_timestamp_to_stbox);
/**
 * Transform a geometry/geography and a timestamp to a spatiotemporal box
 */
PGDLLEXPORT Datum
geo_timestamp_to_stbox(PG_FUNCTION_ARGS)
{
  GSERIALIZED *gs = PG_GETARG_GSERIALIZED_P(0);
  if (gserialized_is_empty(gs))
    PG_RETURN_NULL();
  TimestampTz t = PG_GETARG_TIMESTAMPTZ(1);
  STBOX *result = palloc0(sizeof(STBOX));
  geo_to_stbox_internal(result, gs);
  result->tmin = result->tmax = t;
  MOBDB_FLAGS_SET_T(result->flags, true);
  PG_FREE_IF_COPY(gs, 0);
  PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(geo_period_to_stbox);
/**
 * Transform a geometry/geography and a period to a spatiotemporal box
 */
PGDLLEXPORT Datum
geo_period_to_stbox(PG_FUNCTION_ARGS)
{
  GSERIALIZED *gs = PG_GETARG_GSERIALIZED_P(0);
  if (gserialized_is_empty(gs))
    PG_RETURN_NULL();
  Period *p = PG_GETARG_PERIOD(1);
  STBOX *result = palloc0(sizeof(STBOX));
  geo_to_stbox_internal(result, gs);
  result->tmin = p->lower;
  result->tmax = p->upper;
  MOBDB_FLAGS_SET_T(result->flags, true);
  PG_FREE_IF_COPY(gs, 0);
  PG_RETURN_POINTER(result);
}

/*****************************************************************************
 * Accessor functions
 *****************************************************************************/

PG_FUNCTION_INFO_V1(stbox_hasx);
/**
 * Returns true if the spatiotemporal box has X dimension
 */
PGDLLEXPORT Datum
stbox_hasx(PG_FUNCTION_ARGS)
{
  STBOX *box = PG_GETARG_STBOX_P(0);
  bool result = MOBDB_FLAGS_GET_X(box->flags);
  PG_RETURN_BOOL(result);
}

PG_FUNCTION_INFO_V1(stbox_hasz);
/**
 * Returns true if the spatiotemporal box has Z dimension
 */
PGDLLEXPORT Datum
stbox_hasz(PG_FUNCTION_ARGS)
{
  STBOX *box = PG_GETARG_STBOX_P(0);
  bool result = MOBDB_FLAGS_GET_Z(box->flags);
  PG_RETURN_BOOL(result);
}

PG_FUNCTION_INFO_V1(stbox_hast);
/**
 * Returns true if the spatiotemporal box has T dimension
 */
PGDLLEXPORT Datum
stbox_hast(PG_FUNCTION_ARGS)
{
  STBOX *box = PG_GETARG_STBOX_P(0);
  bool result = MOBDB_FLAGS_GET_T(box->flags);
  PG_RETURN_BOOL(result);
}

PG_FUNCTION_INFO_V1(stbox_isgeodetic);
/**
 * Returns true if the spatiotemporal box is geodetic
 */
PGDLLEXPORT Datum
stbox_isgeodetic(PG_FUNCTION_ARGS)
{
  STBOX *box = PG_GETARG_STBOX_P(0);
  bool result = MOBDB_FLAGS_GET_GEODETIC(box->flags);
  PG_RETURN_BOOL(result);
}

PG_FUNCTION_INFO_V1(stbox_xmin);
/**
 * Returns the minimum X value of the spatiotemporal box
 */
PGDLLEXPORT Datum
stbox_xmin(PG_FUNCTION_ARGS)
{
  STBOX *box = PG_GETARG_STBOX_P(0);
  if (!MOBDB_FLAGS_GET_X(box->flags))
    PG_RETURN_NULL();
  PG_RETURN_FLOAT8(box->xmin);
}

PG_FUNCTION_INFO_V1(stbox_xmax);
/**
 * Returns the maximum X value of the spatiotemporal box
 */
PGDLLEXPORT Datum
stbox_xmax(PG_FUNCTION_ARGS)
{
  STBOX *box = PG_GETARG_STBOX_P(0);
  if (!MOBDB_FLAGS_GET_X(box->flags))
    PG_RETURN_NULL();
  PG_RETURN_FLOAT8(box->xmax);
}

PG_FUNCTION_INFO_V1(stbox_ymin);
/**
 * Returns the minimum Y value of the spatiotemporal box
 */
PGDLLEXPORT Datum
stbox_ymin(PG_FUNCTION_ARGS)
{
  STBOX *box = PG_GETARG_STBOX_P(0);
  if (!MOBDB_FLAGS_GET_X(box->flags))
    PG_RETURN_NULL();
  PG_RETURN_FLOAT8(box->ymin);
}

PG_FUNCTION_INFO_V1(stbox_ymax);
/**
 * Returns the maximum Y value of the spatiotemporal box
 */
PGDLLEXPORT Datum
stbox_ymax(PG_FUNCTION_ARGS)
{
  STBOX *box = PG_GETARG_STBOX_P(0);
  if (!MOBDB_FLAGS_GET_X(box->flags))
    PG_RETURN_NULL();
  PG_RETURN_FLOAT8(box->ymax);
}

PG_FUNCTION_INFO_V1(stbox_zmin);
/**
 * Returns the minimum Z value of the spatiotemporal box
 */
PGDLLEXPORT Datum
stbox_zmin(PG_FUNCTION_ARGS)
{
  STBOX *box = PG_GETARG_STBOX_P(0);
  if (!MOBDB_FLAGS_GET_Z(box->flags))
    PG_RETURN_NULL();
  PG_RETURN_FLOAT8(box->zmin);
}

PG_FUNCTION_INFO_V1(stbox_zmax);
/**
 * Returns the maximum Z value of the spatiotemporal box
 */
PGDLLEXPORT Datum
stbox_zmax(PG_FUNCTION_ARGS)
{
  STBOX *box = PG_GETARG_STBOX_P(0);
  if (!MOBDB_FLAGS_GET_Z(box->flags))
    PG_RETURN_NULL();
  PG_RETURN_FLOAT8(box->zmax);
}

PG_FUNCTION_INFO_V1(stbox_tmin);
/**
 * Returns the minimum timestamp value of the spatiotemporal box
 */
PGDLLEXPORT Datum
stbox_tmin(PG_FUNCTION_ARGS)
{
  STBOX *box = PG_GETARG_STBOX_P(0);
  if (!MOBDB_FLAGS_GET_T(box->flags))
    PG_RETURN_NULL();
  PG_RETURN_TIMESTAMPTZ(box->tmin);
}

PG_FUNCTION_INFO_V1(stbox_tmax);
/**
 * Returns the maximum timestamp value of the spatiotemporal box
 */
PGDLLEXPORT Datum
stbox_tmax(PG_FUNCTION_ARGS)
{
  STBOX *box = PG_GETARG_STBOX_P(0);
  if (!MOBDB_FLAGS_GET_T(box->flags))
    PG_RETURN_NULL();
  PG_RETURN_TIMESTAMPTZ(box->tmax);
}

/*****************************************************************************
 * Transformation functions
 *****************************************************************************/

/**
 * Expand the spatial dimension of the spatiotemporal box with the double value
 * (internal function)
 */
STBOX *
stbox_expand_spatial_internal(STBOX *box, double d)
{
  ensure_has_X_stbox(box);
  STBOX *result = stbox_copy(box);
  result->xmin = box->xmin - d;
  result->xmax = box->xmax + d;
  result->ymin = box->ymin - d;
  result->ymax = box->ymax + d;
  if (MOBDB_FLAGS_GET_Z(box->flags) || MOBDB_FLAGS_GET_GEODETIC(box->flags))
  {
    result->zmin = box->zmin - d;
    result->zmax = box->zmax + d;
  }
  return result;
}

PG_FUNCTION_INFO_V1(stbox_expand_spatial);
/**
 * Expand the spatial dimension of the spatiotemporal box with the double value
 */
PGDLLEXPORT Datum
stbox_expand_spatial(PG_FUNCTION_ARGS)
{
  STBOX *box = PG_GETARG_STBOX_P(0);
  double d = PG_GETARG_FLOAT8(1);
  PG_RETURN_POINTER(stbox_expand_spatial_internal(box, d));
}

/**
 * Expand the temporal dimension of the spatiotemporal box with the interval value
 * (internal function)
 */
STBOX *
stbox_expand_temporal_internal(STBOX *box, Datum interval)
{
  ensure_has_T_stbox(box);
  STBOX *result = stbox_copy(box);
  result->tmin = DatumGetTimestampTz(call_function2(timestamp_mi_interval,
    TimestampTzGetDatum(box->tmin), interval));
  result->tmax = DatumGetTimestampTz(call_function2(timestamp_pl_interval,
    TimestampTzGetDatum(box->tmax), interval));
  return result;
}

PG_FUNCTION_INFO_V1(stbox_expand_temporal);
/**
 * Expand the temporal dimension of the spatiotemporal box with the interval value
 */
PGDLLEXPORT Datum
stbox_expand_temporal(PG_FUNCTION_ARGS)
{
  STBOX *box = PG_GETARG_STBOX_P(0);
  Datum interval = PG_GETARG_DATUM(1);
  PG_RETURN_POINTER(stbox_expand_temporal_internal(box, interval));
}

PG_FUNCTION_INFO_V1(stbox_set_precision);
/**
 * Sets the precision of the coordinates of the spatiotemporal box
 */
PGDLLEXPORT Datum
stbox_set_precision(PG_FUNCTION_ARGS)
{
  STBOX *box = PG_GETARG_STBOX_P(0);
  Datum size = PG_GETARG_DATUM(1);
  ensure_has_X_stbox(box);
  STBOX *result = stbox_copy(box);
  result->xmin = DatumGetFloat8(datum_round(Float8GetDatum(box->xmin), size));
  result->xmax = DatumGetFloat8(datum_round(Float8GetDatum(box->xmax), size));
  result->ymin = DatumGetFloat8(datum_round(Float8GetDatum(box->ymin), size));
  result->ymax = DatumGetFloat8(datum_round(Float8GetDatum(box->ymax), size));
  if (MOBDB_FLAGS_GET_Z(box->flags) || MOBDB_FLAGS_GET_GEODETIC(box->flags))
  {
    result->zmin = DatumGetFloat8(datum_round(Float8GetDatum(box->zmin), size));
    result->zmax = DatumGetFloat8(datum_round(Float8GetDatum(box->zmax), size));
  }
  PG_RETURN_POINTER(result);
}

/*****************************************************************************
 * Topological operators
 *****************************************************************************/

/**
 * Set the ouput variables with the values of the flags of the boxes.
 *
 * @param[in] box1,box2 Input boxes
 * @param[out] hasx,hasz,hast,geodetic Boolean variables
 */
static void
stbox_stbox_flags(const STBOX *box1, const STBOX *box2, bool *hasx,
  bool *hasz, bool *hast, bool *geodetic)
{
  *hasx = MOBDB_FLAGS_GET_X(box1->flags) && MOBDB_FLAGS_GET_X(box2->flags);
  *hasz = MOBDB_FLAGS_GET_Z(box1->flags) && MOBDB_FLAGS_GET_Z(box2->flags);
  *hast = MOBDB_FLAGS_GET_T(box1->flags) && MOBDB_FLAGS_GET_T(box2->flags);
  *geodetic = MOBDB_FLAGS_GET_GEODETIC(box1->flags) &&
    MOBDB_FLAGS_GET_GEODETIC(box2->flags);
  return;
}

/**
 * Verify the conditions and set the ouput variables with the values of the
 * flags of the boxes.
 *
 * Mixing 2D/3D is enabled to compute, for example, 2.5D operations
 * @param[in] box1,box2 Input boxes
 * @param[out] hasx,hasz,hast,geodetic Boolean variables
 */
static void
topo_stbox_stbox_init(const STBOX *box1, const STBOX *box2, bool *hasx,
  bool *hasz, bool *hast, bool *geodetic)
{
  ensure_common_dimension(box1->flags, box2->flags);
  if (MOBDB_FLAGS_GET_X(box1->flags) && MOBDB_FLAGS_GET_X(box2->flags))
  {
    ensure_same_geodetic(box1->flags, box2->flags);
    ensure_same_srid_stbox(box1, box2);
  }
  stbox_stbox_flags(box1, box2, hasx, hasz, hast, geodetic);
  return;
}

/**
 * Returns true if the first spatiotemporal box contains the second one
 * (internal function)
 */
bool
contains_stbox_stbox_internal(const STBOX *box1, const STBOX *box2)
{
  bool hasx, hasz, hast, geodetic;
  topo_stbox_stbox_init(box1, box2, &hasx, &hasz, &hast, &geodetic);
  if (hasx && (box2->xmin < box1->xmin || box2->xmax > box1->xmax ||
    box2->ymin < box1->ymin || box2->ymax > box1->ymax))
      return false;
  if ((hasz || geodetic) && (box2->zmin < box1->zmin || box2->zmax > box1->zmax))
      return false;
  if (hast && (box2->tmin < box1->tmin || box2->tmax > box1->tmax))
      return false;
  return true;
}

PG_FUNCTION_INFO_V1(contains_stbox_stbox);
/**
 * Returns true if the first spatiotemporal box contains the second one
 */
PGDLLEXPORT Datum
contains_stbox_stbox(PG_FUNCTION_ARGS)
{
  STBOX *box1 = PG_GETARG_STBOX_P(0);
  STBOX *box2 = PG_GETARG_STBOX_P(1);
  PG_RETURN_BOOL(contains_stbox_stbox_internal(box1, box2));
}

/**
 * Returns true if the first spatiotemporal box is contained by the second one
 * (internal function)
 */
bool
contained_stbox_stbox_internal(const STBOX *box1, const STBOX *box2)
{
  return contains_stbox_stbox_internal(box2, box1);
}

PG_FUNCTION_INFO_V1(contained_stbox_stbox);
/**
 * Returns true if the first spatiotemporal box is contained by the second one
 */
PGDLLEXPORT Datum
contained_stbox_stbox(PG_FUNCTION_ARGS)
{
  STBOX *box1 = PG_GETARG_STBOX_P(0);
  STBOX *box2 = PG_GETARG_STBOX_P(1);
  PG_RETURN_BOOL(contained_stbox_stbox_internal(box1, box2));
}

/**
 * Returns true if the spatiotemporal boxes overlap
 * (internal function)
 */
bool
overlaps_stbox_stbox_internal(const STBOX *box1, const STBOX *box2)
{
  bool hasx, hasz, hast, geodetic;
  topo_stbox_stbox_init(box1, box2, &hasx, &hasz, &hast, &geodetic);
  if (hasx && (box1->xmax < box2->xmin || box1->xmin > box2->xmax ||
    box1->ymax < box2->ymin || box1->ymin > box2->ymax))
    return false;
  if ((hasz || geodetic) && (box1->zmax < box2->zmin || box1->zmin > box2->zmax))
    return false;
  if (hast && (box1->tmax < box2->tmin || box1->tmin > box2->tmax))
    return false;
  return true;
}

PG_FUNCTION_INFO_V1(overlaps_stbox_stbox);
/**
 * Returns true if the spatiotemporal boxes overlap
 */
PGDLLEXPORT Datum
overlaps_stbox_stbox(PG_FUNCTION_ARGS)
{
  STBOX *box1 = PG_GETARG_STBOX_P(0);
  STBOX *box2 = PG_GETARG_STBOX_P(1);
  PG_RETURN_BOOL(overlaps_stbox_stbox_internal(box1, box2));
}

/**
 * Returns true if the spatiotemporal boxes are equal on the common dimensions
 * (internal function)
 */
bool
same_stbox_stbox_internal(const STBOX *box1, const STBOX *box2)
{
  bool hasx, hasz, hast, geodetic;
  topo_stbox_stbox_init(box1, box2, &hasx, &hasz, &hast, &geodetic);
  if (hasx && (box1->xmin != box2->xmin || box1->xmax != box2->xmax ||
    box1->ymin != box2->ymin || box1->ymax != box2->ymax))
    return false;
  if ((hasz || geodetic) && (box1->zmin != box2->zmin || box1->zmax != box2->zmax))
    return false;
  if (hast && (box1->tmin != box2->tmin || box1->tmax != box2->tmax))
    return false;
  return true;
}

PG_FUNCTION_INFO_V1(same_stbox_stbox);
/**
 * Returns true if the spatiotemporal boxes are equal on the common dimensions
 */
PGDLLEXPORT Datum
same_stbox_stbox(PG_FUNCTION_ARGS)
{
  STBOX *box1 = PG_GETARG_STBOX_P(0);
  STBOX *box2 = PG_GETARG_STBOX_P(1);
  PG_RETURN_BOOL(same_stbox_stbox_internal(box1, box2));
}

/**
 * Returns true if the spatiotemporal boxes are adjacent
 * (internal function)
 */
bool
adjacent_stbox_stbox_internal(const STBOX *box1, const STBOX *box2)
{
  bool hasx, hasz, hast, geodetic;
  topo_stbox_stbox_init(box1, box2, &hasx, &hasz, &hast, &geodetic);
  STBOX *inter = stbox_intersection_internal(box1, box2);
  if (inter == NULL)
    return false;
  /* Boxes are adjacent if they share n dimensions and their intersection is
   * at most of n-1 dimensions */
  if (!hasx && hast)
    return inter->tmin == inter->tmax;
  else if (hasx && !hast)
  {
    if (hasz || geodetic)
      return inter->xmin == inter->xmax || inter->ymin == inter->ymax ||
           inter->zmin == inter->zmax;
    else
      return inter->xmin == inter->xmax || inter->ymin == inter->ymax;
  }
  else
  {
    if (hasz || geodetic)
      return inter->xmin == inter->xmax || inter->ymin == inter->ymax ||
           inter->zmin == inter->zmax || inter->tmin == inter->tmax;
    else
      return inter->xmin == inter->xmax || inter->ymin == inter->ymax ||
           inter->tmin == inter->tmax;
  }
}

PG_FUNCTION_INFO_V1(adjacent_stbox_stbox);
/**
 * Returns true if the spatiotemporal boxes are adjacent
 */
PGDLLEXPORT Datum
adjacent_stbox_stbox(PG_FUNCTION_ARGS)
{
  STBOX *box1 = PG_GETARG_STBOX_P(0);
  STBOX *box2 = PG_GETARG_STBOX_P(1);
  PG_RETURN_BOOL(adjacent_stbox_stbox_internal(box1, box2));
}

/*****************************************************************************
 * Position operators
 *****************************************************************************/

/**
 * Verify the conditions for a position operator
 *
 * @param[in] box1,box2 Input boxes
 */
static void
pos_stbox_stbox_test(const STBOX *box1, const STBOX *box2)
{
  ensure_same_geodetic(box1->flags, box2->flags);
  ensure_same_srid_stbox(box1, box2);
  return;
}

/**
 * Returns true if the first spatiotemporal box is strictly to the left of the second one
 * (internal function)
 */
bool
left_stbox_stbox_internal(const STBOX *box1, const STBOX *box2)
{
  ensure_has_X_stbox(box1);
  ensure_has_X_stbox(box2);
  pos_stbox_stbox_test(box1, box2);
  return (box1->xmax < box2->xmin);
}

PG_FUNCTION_INFO_V1(left_stbox_stbox);
/**
 * Returns true if the first spatiotemporal box is strictly to the left of the second one
 */
PGDLLEXPORT Datum
left_stbox_stbox(PG_FUNCTION_ARGS)
{
  STBOX *box1 = PG_GETARG_STBOX_P(0);
  STBOX *box2 = PG_GETARG_STBOX_P(1);
  PG_RETURN_BOOL(left_stbox_stbox_internal(box1, box2));
}

/**
 * Returns true if the first spatiotemporal box does not extend to the right of the second one
 * (internal function)
 */
bool
overleft_stbox_stbox_internal(const STBOX *box1, const STBOX *box2)
{
  ensure_has_X_stbox(box1);
  ensure_has_X_stbox(box2);
  pos_stbox_stbox_test(box1, box2);
  return (box1->xmax <= box2->xmax);
}

PG_FUNCTION_INFO_V1(overleft_stbox_stbox);
/**
 * Returns true if the first spatiotemporal box does not extend to the right of the second one
 */
PGDLLEXPORT Datum
overleft_stbox_stbox(PG_FUNCTION_ARGS)
{
  STBOX *box1 = PG_GETARG_STBOX_P(0);
  STBOX *box2 = PG_GETARG_STBOX_P(1);
  PG_RETURN_BOOL(overleft_stbox_stbox_internal(box1, box2));
}

/**
 * Returns true if the first spatiotemporal box is strictly to the right of the second one
 * (internal function)
 */
bool
right_stbox_stbox_internal(const STBOX *box1, const STBOX *box2)
{
  ensure_has_X_stbox(box1);
  ensure_has_X_stbox(box2);
  pos_stbox_stbox_test(box1, box2);
  return (box1->xmin > box2->xmax);
}

PG_FUNCTION_INFO_V1(right_stbox_stbox);
/**
 * Returns true if the first spatiotemporal box is strictly to the right of the second one
 */
PGDLLEXPORT Datum
right_stbox_stbox(PG_FUNCTION_ARGS)
{
  STBOX *box1 = PG_GETARG_STBOX_P(0);
  STBOX *box2 = PG_GETARG_STBOX_P(1);
  PG_RETURN_BOOL(right_stbox_stbox_internal(box1, box2));
}

/**
 * Returns true if the first spatio temporal box does not extend to the left of the second one
 * (internal function)
 */
bool
overright_stbox_stbox_internal(const STBOX *box1, const STBOX *box2)
{
  ensure_has_X_stbox(box1);
  ensure_has_X_stbox(box2);
  pos_stbox_stbox_test(box1, box2);
  return (box1->xmin >= box2->xmin);
}

PG_FUNCTION_INFO_V1(overright_stbox_stbox);
/**
 * Returns true if the first spatio temporal box does not extend to the left of the second one
 */
PGDLLEXPORT Datum
overright_stbox_stbox(PG_FUNCTION_ARGS)
{
  STBOX *box1 = PG_GETARG_STBOX_P(0);
  STBOX *box2 = PG_GETARG_STBOX_P(1);
  PG_RETURN_BOOL(overright_stbox_stbox_internal(box1, box2));
}

/**
 * Returns true if the first spatiotemporal box is strictly below of the second one
 * (internal function)
 */
bool
below_stbox_stbox_internal(const STBOX *box1, const STBOX *box2)
{
  ensure_has_X_stbox(box1);
  ensure_has_X_stbox(box2);
  pos_stbox_stbox_test(box1, box2);
  return (box1->ymax < box2->ymin);
}

PG_FUNCTION_INFO_V1(below_stbox_stbox);
/**
 * Returns true if the first spatiotemporal box is strictly below of the second one
 */
PGDLLEXPORT Datum
below_stbox_stbox(PG_FUNCTION_ARGS)
{
  STBOX *box1 = PG_GETARG_STBOX_P(0);
  STBOX *box2 = PG_GETARG_STBOX_P(1);
  PG_RETURN_BOOL(below_stbox_stbox_internal(box1, box2));
}

/**
 * Returns true if the first spatiotemporal box does not extend above of the second one
 * (internal function)
 */
bool
overbelow_stbox_stbox_internal(const STBOX *box1, const STBOX *box2)
{
  ensure_has_X_stbox(box1);
  ensure_has_X_stbox(box2);
  pos_stbox_stbox_test(box1, box2);
  return (box1->ymax <= box2->ymax);
}

PG_FUNCTION_INFO_V1(overbelow_stbox_stbox);
/**
 * Returns true if the first spatiotemporal box does not extend above of the second one
 */
PGDLLEXPORT Datum
overbelow_stbox_stbox(PG_FUNCTION_ARGS)
{
  STBOX *box1 = PG_GETARG_STBOX_P(0);
  STBOX *box2 = PG_GETARG_STBOX_P(1);
  PG_RETURN_BOOL(overbelow_stbox_stbox_internal(box1, box2));
}

/**
 * Returns true if the first spatiotemporal box is strictly above of the second one
 * (internal function)
 */
bool
above_stbox_stbox_internal(const STBOX *box1, const STBOX *box2)
{
  ensure_has_X_stbox(box1);
  ensure_has_X_stbox(box2);
  pos_stbox_stbox_test(box1, box2);
  return (box1->ymin > box2->ymax);
}

PG_FUNCTION_INFO_V1(above_stbox_stbox);
/**
 * Returns true if the first spatiotemporal box is strictly above of the second one
 */
PGDLLEXPORT Datum
above_stbox_stbox(PG_FUNCTION_ARGS)
{
  STBOX *box1 = PG_GETARG_STBOX_P(0);
  STBOX *box2 = PG_GETARG_STBOX_P(1);
  PG_RETURN_BOOL(above_stbox_stbox_internal(box1, box2));
}

/**
 * Returns true if the first spatiotemporal box does not extend below of the second one
 * (internal function)
 */
bool
overabove_stbox_stbox_internal(const STBOX *box1, const STBOX *box2)
{
  ensure_has_X_stbox(box1);
  ensure_has_X_stbox(box2);
  pos_stbox_stbox_test(box1, box2);
  return (box1->ymin >= box2->ymin);
}

PG_FUNCTION_INFO_V1(overabove_stbox_stbox);
/**
 * Returns true if the first spatiotemporal box does not extend below of the second one
 */
PGDLLEXPORT Datum
overabove_stbox_stbox(PG_FUNCTION_ARGS)
{
  STBOX *box1 = PG_GETARG_STBOX_P(0);
  STBOX *box2 = PG_GETARG_STBOX_P(1);
  PG_RETURN_BOOL(overabove_stbox_stbox_internal(box1, box2));
}

/**
 * Returns true if the first spatiotemporal box is strictly in front of the second one
 * (internal function)
 */
bool
front_stbox_stbox_internal(const STBOX *box1, const STBOX *box2)
{
  ensure_has_Z(box1->flags);
  ensure_has_Z(box2->flags);
  pos_stbox_stbox_test(box1, box2);
  return (box1->zmax < box2->zmin);
}

PG_FUNCTION_INFO_V1(front_stbox_stbox);
/**
 * Returns true if the first spatiotemporal box is strictly in front of the second one
 */
PGDLLEXPORT Datum
front_stbox_stbox(PG_FUNCTION_ARGS)
{
  STBOX *box1 = PG_GETARG_STBOX_P(0);
  STBOX *box2 = PG_GETARG_STBOX_P(1);
  PG_RETURN_BOOL(front_stbox_stbox_internal(box1, box2));
}

/**
 * Returns true if the first spatiotemporal box does not extend to the back of the second one
 * (internal function)
 */
bool
overfront_stbox_stbox_internal(const STBOX *box1, const STBOX *box2)
{
  ensure_has_Z(box1->flags);
  ensure_has_Z(box2->flags);
  pos_stbox_stbox_test(box1, box2);
  return (box1->zmax <= box2->zmax);
}

PG_FUNCTION_INFO_V1(overfront_stbox_stbox);
/**
 * Returns true if the first spatiotemporal box does not extend to the back of the second one
 */
PGDLLEXPORT Datum
overfront_stbox_stbox(PG_FUNCTION_ARGS)
{
  STBOX *box1 = PG_GETARG_STBOX_P(0);
  STBOX *box2 = PG_GETARG_STBOX_P(1);
  PG_RETURN_BOOL(overfront_stbox_stbox_internal(box1, box2));
}

/**
 * Returns true if the first spatiotemporal box is strictly back of the second one
 * (internal function)
 */
bool
back_stbox_stbox_internal(const STBOX *box1, const STBOX *box2)
{
  ensure_has_Z(box1->flags);
  ensure_has_Z(box2->flags);
  pos_stbox_stbox_test(box1, box2);
  return (box1->zmin > box2->zmax);
}

PG_FUNCTION_INFO_V1(back_stbox_stbox);
/**
 * Returns true if the first spatiotemporal box is strictly back of the second one
 */
PGDLLEXPORT Datum
back_stbox_stbox(PG_FUNCTION_ARGS)
{
  STBOX *box1 = PG_GETARG_STBOX_P(0);
  STBOX *box2 = PG_GETARG_STBOX_P(1);
  PG_RETURN_BOOL(back_stbox_stbox_internal(box1, box2));
}

/**
 * Returns true if the first spatiotemporal box does not extend to the front of the second one
 * (internal function)
 */
bool
overback_stbox_stbox_internal(const STBOX *box1, const STBOX *box2)
{
  ensure_has_Z(box1->flags);
  ensure_has_Z(box2->flags);
  pos_stbox_stbox_test(box1, box2);
  return (box1->zmin >= box2->zmin);
}

PG_FUNCTION_INFO_V1(overback_stbox_stbox);
/**
 * Returns true if the first spatiotemporal box does not extend to the front of the second one
 */
PGDLLEXPORT Datum
overback_stbox_stbox(PG_FUNCTION_ARGS)
{
  STBOX *box1 = PG_GETARG_STBOX_P(0);
  STBOX *box2 = PG_GETARG_STBOX_P(1);
  PG_RETURN_BOOL(overback_stbox_stbox_internal(box1, box2));
}

/**
 * Returns true if the first spatiotemporal box is strictly before the second one
 * (internal function)
 */
bool
before_stbox_stbox_internal(const STBOX *box1, const STBOX *box2)
{
  ensure_has_T_stbox(box1);
  ensure_has_T_stbox(box2);
  return (box1->tmax < box2->tmin);
}

PG_FUNCTION_INFO_V1(before_stbox_stbox);
/**
 * Returns true if the first spatiotemporal box is strictly before the second one
 */
PGDLLEXPORT Datum
before_stbox_stbox(PG_FUNCTION_ARGS)
{
  STBOX *box1 = PG_GETARG_STBOX_P(0);
  STBOX *box2 = PG_GETARG_STBOX_P(1);
  PG_RETURN_BOOL(before_stbox_stbox_internal(box1, box2));
}

/**
 * Returns true if the first temporal box does not extend after the second one
 * (internal function)
 */
bool
overbefore_stbox_stbox_internal(const STBOX *box1, const STBOX *box2)
{
  ensure_has_T_stbox(box1);
  ensure_has_T_stbox(box2);
  return (box1->tmax <= box2->tmax);
}

PG_FUNCTION_INFO_V1(overbefore_stbox_stbox);
/**
 * Returns true if the first temporal box does not extend after the second one
 */
PGDLLEXPORT Datum
overbefore_stbox_stbox(PG_FUNCTION_ARGS)
{
  STBOX *box1 = PG_GETARG_STBOX_P(0);
  STBOX *box2 = PG_GETARG_STBOX_P(1);
  PG_RETURN_BOOL(overbefore_stbox_stbox_internal(box1, box2));
}

/**
 * Returns true if the first spatiotemporal box is strictly after the second one
 * (internal function)
 */
bool
after_stbox_stbox_internal(const STBOX *box1, const STBOX *box2)
{
  ensure_has_T_stbox(box1);
  ensure_has_T_stbox(box2);
  return (box1->tmin > box2->tmax);
}

PG_FUNCTION_INFO_V1(after_stbox_stbox);
/**
 * Returns true if the first spatiotemporal box is strictly after the second one
 */
PGDLLEXPORT Datum
after_stbox_stbox(PG_FUNCTION_ARGS)
{
  STBOX *box1 = PG_GETARG_STBOX_P(0);
  STBOX *box2 = PG_GETARG_STBOX_P(1);
  PG_RETURN_BOOL(after_stbox_stbox_internal(box1, box2));
}

/**
 * Returns true if the first temporal box does not extend before the second one
 * (internal function)
 */
bool
overafter_stbox_stbox_internal(const STBOX *box1, const STBOX *box2)
{
  ensure_has_T_stbox(box1);
  ensure_has_T_stbox(box2);
  return (box1->tmin >= box2->tmin);
}

PG_FUNCTION_INFO_V1(overafter_stbox_stbox);
/**
 * Returns true if the first temporal box does not extend before the second one
 */
PGDLLEXPORT Datum
overafter_stbox_stbox(PG_FUNCTION_ARGS)
{
  STBOX *box1 = PG_GETARG_STBOX_P(0);
  STBOX *box2 = PG_GETARG_STBOX_P(1);
  PG_RETURN_BOOL(overafter_stbox_stbox_internal(box1, box2));
}

/*****************************************************************************
 * Set operators
 *****************************************************************************/

/**
 * Returns the union of the spatiotemporal boxes
 * (internal function)
 */
STBOX *
stbox_union_internal(const STBOX *box1, const STBOX *box2, bool strict)
{
  ensure_same_geodetic(box1->flags, box2->flags);
  ensure_same_dimensionality(box1->flags, box2->flags);
  ensure_same_srid_stbox(box1, box2);
  /* If the strict parameter is true, we need to ensure that the boxes
   * intersect, otherwise their union cannot be represented by a box */
  if (strict && ! overlaps_stbox_stbox_internal(box1, box2))
    elog(ERROR, "Result of box union would not be contiguous");

  STBOX *result = stbox_copy(box1);
  stbox_expand(result, box2);
  return(result);
}

PG_FUNCTION_INFO_V1(stbox_union);
/**
 * Returns the union of the spatiotemporal boxes
 */
PGDLLEXPORT Datum
stbox_union(PG_FUNCTION_ARGS)
{
  STBOX *box1 = PG_GETARG_STBOX_P(0);
  STBOX *box2 = PG_GETARG_STBOX_P(1);
  STBOX *result = stbox_union_internal(box1, box2, true);
  PG_RETURN_POINTER(result);
}

/**
 * Returns the intersection of the spatiotemporal boxes
 * (internal function)
 */
STBOX *
stbox_intersection_internal(const STBOX *box1, const STBOX *box2)
{
  ensure_same_geodetic(box1->flags, box2->flags);
  ensure_same_srid_stbox(box1, box2);

  bool hasx = MOBDB_FLAGS_GET_X(box1->flags) && MOBDB_FLAGS_GET_X(box2->flags);
  bool hasz = MOBDB_FLAGS_GET_Z(box1->flags) && MOBDB_FLAGS_GET_Z(box2->flags);
  bool hast = MOBDB_FLAGS_GET_T(box1->flags) && MOBDB_FLAGS_GET_T(box2->flags);
  bool geodetic = MOBDB_FLAGS_GET_GEODETIC(box1->flags) && MOBDB_FLAGS_GET_GEODETIC(box2->flags);
  /* If there is no common dimension */
  if ((! hasx && ! hast) ||
    /* If they do no intersect in one common dimension */
    (hasx && (box1->xmin > box2->xmax || box2->xmin > box1->xmax ||
      box1->ymin > box2->ymax || box2->ymin > box1->ymax)) ||
    ((hasz || geodetic) && (box1->zmin > box2->zmax || box2->zmin > box1->zmax)) ||
    (hast && (box1->tmin > box2->tmax || box2->tmin > box1->tmax)))
    return(NULL);

  double xmin = 0, xmax = 0, ymin = 0, ymax = 0, zmin = 0, zmax = 0;
  TimestampTz tmin = 0, tmax = 0;
  if (hasx)
  {
    xmin = Max(box1->xmin, box2->xmin);
    xmax = Min(box1->xmax, box2->xmax);
    ymin = Max(box1->ymin, box2->ymin);
    ymax = Min(box1->ymax, box2->ymax);
    if (hasz || geodetic)
      {
      zmin = Max(box1->zmin, box2->zmin);
      zmax = Min(box1->zmax, box2->zmax);
      }
  }
  if (hast)
  {
    tmin = Max(box1->tmin, box2->tmin);
    tmax = Min(box1->tmax, box2->tmax);
  }
  return stbox_make(hasx, hasz, hast, geodetic, box1->srid,
    xmin, xmax, ymin, ymax, zmin, zmax, tmin, tmax);
}

PG_FUNCTION_INFO_V1(stbox_intersection);
/**
 * Returns the intersection of the spatiotemporal boxes
 */
PGDLLEXPORT Datum
stbox_intersection(PG_FUNCTION_ARGS)
{
  STBOX *box1 = PG_GETARG_STBOX_P(0);
  STBOX *box2 = PG_GETARG_STBOX_P(1);
  STBOX *result = stbox_intersection_internal(box1, box2);
  if (result == NULL)
    PG_RETURN_NULL();
  PG_RETURN_POINTER(result);
}

/*****************************************************************************
 * Comparison functions
 *****************************************************************************/

/**
 * Returns -1, 0, or 1 depending on whether the first spatiotemporal box
 * is less than, equal, or greater than the second one
 * (internal function)
 *
 * @note Function used for B-tree comparison
 */
int
stbox_cmp_internal(const STBOX *box1, const STBOX *box2)
{
  /* Compare the SRID */
  if (box1->srid < box2->srid)
    return -1;
  if (box1->srid > box2->srid)
    return 1;

  bool hasx, hasz, hast, geodetic;
  stbox_stbox_flags(box1, box2, &hasx, &hasz, &hast, &geodetic);
  if (hast)
  {
    /* Compare the box minima */
    if (box1->tmin < box2->tmin)
      return -1;
    if (box1->tmin > box2->tmin)
      return 1;
    /* Compare the box maxima */
    if (box1->tmax < box2->tmax)
      return -1;
    if (box1->tmax > box2->tmax)
      return 1;
  }
  if (hasx)
  {
    /* Compare the box minima */
    if (box1->xmin < box2->xmin)
      return -1;
    if (box1->xmin > box2->xmin)
      return 1;
    if (box1->ymin < box2->ymin)
      return -1;
    if (box1->ymin > box2->ymin)
      return 1;
    if (hasz)
    {
      if (box1->zmin < box2->zmin)
        return -1;
      if (box1->zmin > box2->zmin)
        return 1;
    }
    /* Compare the box maxima */
    if (box1->xmax < box2->xmax)
      return -1;
    if (box1->xmax > box2->xmax)
      return 1;
    if (box1->ymax < box2->ymax)
      return -1;
    if (box1->ymax > box2->ymax)
      return 1;
    if (hasz)
    {
      if (box1->zmax < box2->zmax)
        return -1;
      if (box1->zmax > box2->zmax)
        return 1;
    }
  }
  /* Finally compare the flags */
  if (box1->flags < box2->flags)
    return -1;
  if (box1->flags > box2->flags)
    return 1;
  /* The two boxes are equal */
  return 0;
}

PG_FUNCTION_INFO_V1(stbox_cmp);
/**
 * Returns -1, 0, or 1 depending on whether the first spatiotemporal box
 * is less than, equal, or greater than the second one
 *
 * @note Function used for B-tree comparison
 */
PGDLLEXPORT Datum
stbox_cmp(PG_FUNCTION_ARGS)
{
  STBOX *box1 = PG_GETARG_STBOX_P(0);
  STBOX *box2 = PG_GETARG_STBOX_P(1);
  int  cmp = stbox_cmp_internal(box1, box2);
  PG_RETURN_INT32(cmp);
}

PG_FUNCTION_INFO_V1(stbox_lt);
/**
 * Returns true if the first spatiotemporal box is less than the second one
 */
PGDLLEXPORT Datum
stbox_lt(PG_FUNCTION_ARGS)
{
  STBOX *box1 = PG_GETARG_STBOX_P(0);
  STBOX *box2 = PG_GETARG_STBOX_P(1);
  int  cmp = stbox_cmp_internal(box1, box2);
  PG_RETURN_BOOL(cmp < 0);
}

PG_FUNCTION_INFO_V1(stbox_le);
/**
 * Returns true if the first spatiotemporal box is less than or equal to
 * the second one
 */
PGDLLEXPORT Datum
stbox_le(PG_FUNCTION_ARGS)
{
  STBOX *box1 = PG_GETARG_STBOX_P(0);
  STBOX *box2 = PG_GETARG_STBOX_P(1);
  int  cmp = stbox_cmp_internal(box1, box2);
  PG_RETURN_BOOL(cmp <= 0);
}

PG_FUNCTION_INFO_V1(stbox_ge);
/**
 * Returns true if the first spatiotemporal box is greater than or equal to
 * the second one
 */
PGDLLEXPORT Datum
stbox_ge(PG_FUNCTION_ARGS)
{
  STBOX *box1 = PG_GETARG_STBOX_P(0);
  STBOX *box2 = PG_GETARG_STBOX_P(1);
  int  cmp = stbox_cmp_internal(box1, box2);
  PG_RETURN_BOOL(cmp >= 0);
}

PG_FUNCTION_INFO_V1(stbox_gt);
/**
 * Returns true if the first spatiotemporal box is greater than the second one
 */
PGDLLEXPORT Datum
stbox_gt(PG_FUNCTION_ARGS)
{
  STBOX *box1 = PG_GETARG_STBOX_P(0);
  STBOX *box2 = PG_GETARG_STBOX_P(1);
  int  cmp = stbox_cmp_internal(box1, box2);
  PG_RETURN_BOOL(cmp > 0);
}

/**
 * Returns true if the two spatiotemporal boxes are equal
 * (internal function)
 *
 * @note The internal B-tree comparator is not used to increase efficiency
 */
bool
stbox_eq_internal(const STBOX *box1, const STBOX *box2)
{
  if (box1->xmin != box2->xmin || box1->ymin != box2->ymin ||
    box1->zmin != box2->zmin || box1->tmin != box2->tmin ||
    box1->xmax != box2->xmax || box1->ymax != box2->ymax ||
    box1->zmax != box2->zmax || box1->tmax != box2->tmax ||
    box1->flags != box2->flags || box1->srid != box2->srid)
    return false;
  /* The two boxes are equal */
  return true;
}

PG_FUNCTION_INFO_V1(stbox_eq);
/**
 * Returns true if the two spatiotemporal boxes are equal
 */
PGDLLEXPORT Datum
stbox_eq(PG_FUNCTION_ARGS)
{
  STBOX *box1 = PG_GETARG_STBOX_P(0);
  STBOX *box2 = PG_GETARG_STBOX_P(1);
  PG_RETURN_BOOL(stbox_eq_internal(box1, box2));
}

PG_FUNCTION_INFO_V1(stbox_ne);
/**
 * Returns true if the two spatiotemporal boxes are different
 */
PGDLLEXPORT Datum
stbox_ne(PG_FUNCTION_ARGS)
{
  STBOX *box1 = PG_GETARG_STBOX_P(0);
  STBOX *box2 = PG_GETARG_STBOX_P(1);
  PG_RETURN_BOOL(! stbox_eq_internal(box1, box2));
}

/*****************************************************************************
 * Tiling functions
 *****************************************************************************/

/**
 * Generate a tile from the current state of the multidimensional grid
 */
static STBOX *
stbox_tile(bool hasz, bool hast, int32 srid, POINT3DZ sorigin,
  TimestampTz torigin, double xsize, int64 tsize, int *coords)
{
  double xmin = sorigin.x + (xsize * coords[0]);
  double xmax = sorigin.x + (xsize * (coords[0] + 1));
  double ymin = sorigin.y + (xsize * coords[1]);
  double ymax = sorigin.y + (xsize * (coords[1] + 1));
  double zmin = 0, zmax = 0;
  TimestampTz tmin = 0, tmax = 0;
  if (hasz)
  {
    zmin = sorigin.z + (xsize * coords[2]);
    zmax = sorigin.z + (xsize * (coords[2] + 1));
  }
  if (hast)
  {
    tmin = torigin + (TimestampTz) (tsize * coords[3]);
    tmax = torigin + (TimestampTz) (tsize * (coords[3] + 1));
  }
  return (STBOX *) stbox_make(true, hasz, hast, false, srid,
    xmin, xmax, ymin, ymax, zmin, zmax, tmin, tmax);
}

/**
 * Struct for storing the state that persists across multiple calls generating
 * the multidimensional grid
 */
#define MAXDIMS 4
typedef struct STboxGridState
{
  bool done;
  bool hasz;
  bool hast;
  int32 srid;
  double size;
  int64 tsize;
  POINT3DZ sorigin;
  int64 torigin;
  int min[MAXDIMS];
  int max[MAXDIMS];
  int coords[MAXDIMS];
} STboxGridState;

/**
 * Create the initial state that persists across the multiple calls generating
 * the multidimensional grid.
 * @pre The xsize argument must be greater to 0.
 * @note The tsize argument may be equal to 0 if it was not provided by the
 * user. In that case only the spatial dimension is tiled.
 */
static STboxGridState *
stbox_tile_state_new(STBOX *box, double size, int64 tsize, POINT3DZ sorigin,
  TimestampTz torigin, int32 srid)
{
  assert(size > 0);
  /* palloc0 to initialize the missing dimensions to 0 */
  STboxGridState *state = palloc0(sizeof(STboxGridState));

  /* fill in state */
  state->done = false;
  state->hasz = MOBDB_FLAGS_GET_Z(box->flags);
  state->hast = MOBDB_FLAGS_GET_T(box->flags) && tsize > 0;
  state->srid = box->srid;
  state->size = size;
  state->tsize = tsize;
  state->srid = srid;
  state->sorigin = sorigin;
  state->torigin = torigin;
  state->min[0] = floor(box->xmin / size);
  state->max[0] = floor(box->xmax / size);
  state->min[1] = floor(box->ymin / size);
  state->max[1] = floor(box->ymax / size);
  int ndims = 2;
  if (state->hasz)
  {
    state->min[ndims] = floor(box->zmin / size);
    state->max[ndims++] = floor(box->zmax / size);
  }
  else if (state->hast)
  {
    state->min[ndims] = floor(box->tmin / tsize);
    state->max[ndims++] = floor(box->tmax / tsize);
  }
  for (int i = 0; i < ndims; i++)
    state->coords[i] = state->min[i];
  return state;
}

/**
 * Increment the current state to the next tile of the multidimensional grid
 */
static void
stbox_tile_state_next(STboxGridState *state)
{
  if (!state || state->done)
      return;
  /* Move to the next cell. We need to to take into account whether
   * hasz and/or hast and thus there are 4 possible cases */
  state->coords[0]++;
  if (state->coords[0] > state->max[0])
  {
    state->coords[0] = state->min[0];
    state->coords[1]++;
    if (state->coords[1] > state->max[1])
    {
      if (state->hasz)
      {
        /* has Z */
        state->coords[0] = state->min[0];
        state->coords[1] = state->min[1];
        state->coords[2]++;
        if (state->coords[2] > state->max[2])
        {
          if (state->hast)
          {
            /* has Z and has T */
            state->coords[0] = state->min[0];
            state->coords[1] = state->min[1];
            state->coords[2] = state->min[2];
            state->coords[3]++;
            if (state->coords[3] > state->max[3])
            {
              state->done = true;
              return;
            }
          }
          else
          {
            /* has Z and does not have T */
            state->done = true;
            return;
          }
        }
      }
      else
      {
        /* does not have Z */
        if (state->hast)
        {
          /* does not have Z and has T */
          if (state->coords[2] > state->max[2])
          {
            state->coords[0] = state->min[0];
            state->coords[1] = state->min[1];
            state->coords[2] = state->min[2];
            state->coords[3]++;
            if (state->coords[3] > state->max[3])
            {
              state->done = true;
              return;
            }
          }
        }
        else
        {
          /* does not have Z and does have T */
          state->done = true;
          return;
        }
      }
    }
  }
  return;
}

PG_FUNCTION_INFO_V1(stbox_multidim_grid);
/**
 * Generate a multidimensional grid for temporal points.
 *
 * Signatures
 * @code
 * stbox_multidim_grid(bounds STBOX, size float8)
 * stbox_multidim_grid(bounds STBOX, size float8, interval Interval)
 * @endcode
 */
Datum stbox_multidim_grid(PG_FUNCTION_ARGS)
{
  FuncCallContext *funcctx;
  STboxGridState *state;
  bool isnull[2] = {0,0}; /* needed to say no value is null */
  Datum tuple_arr[2]; /* used to construct the composite return value */
  HeapTuple tuple;
  Datum result; /* the actual composite return value */

  if (SRF_IS_FIRSTCALL())
  {
    /* Get input parameters */
    STBOX *bounds = PG_GETARG_STBOX_P(0);
    ensure_not_geodetic_stbox(bounds);
    ensure_has_X_stbox(bounds);
    int32 srid = bounds->srid;
    double size = PG_GETARG_FLOAT8(1);
    ensure_positive_double(size);
    GSERIALIZED *sorigin;
    int64 tsize = 0;
    TimestampTz torigin = 0;
    if (PG_NARGS() == 3)
      sorigin = PG_GETARG_GSERIALIZED_P(2);
    else /* PG_NARGS() == 5 */
    {
      /* If time arguments are given */
      ensure_has_T_stbox(bounds);
      Interval *duration = PG_GETARG_INTERVAL_P(2);
      ensure_valid_duration(duration);
      tsize = get_interval_units(duration);
      sorigin = PG_GETARG_GSERIALIZED_P(3);
      torigin = PG_GETARG_TIMESTAMPTZ(4);
    }
    ensure_non_empty(sorigin);
    ensure_point_type(sorigin);
    int32 gs_srid = gserialized_get_srid(sorigin);
    if (gs_srid != 0)
      error_if_srid_mismatch(srid, gs_srid);
    POINT3DZ p;
    if (FLAGS_GET_Z(sorigin->flags))
      p = datum_get_point3dz(PointerGetDatum(sorigin));
    else
    {
      /* Initialize to 0 the Z dimension if it is missing */
      memset(&p, 0, sizeof(POINT3DZ));
      const POINT2D *p2d = gs_get_point2d_p(sorigin);
      p.x = p2d->x;
      p.y = p2d->y;
    }

    /* Initialize the FuncCallContext */
    funcctx = SRF_FIRSTCALL_INIT();
    /* Switch to memory context appropriate for multiple function calls */
    MemoryContext oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
    /* Create function state */
    funcctx->user_fctx = stbox_tile_state_new(bounds, size, tsize, p, torigin, srid);
    /* Build a tuple description for a multidim_grid tuple */
    get_call_result_type(fcinfo, 0, &funcctx->tuple_desc);
    BlessTupleDesc(funcctx->tuple_desc);
    MemoryContextSwitchTo(oldcontext);
  }

  /* stuff done on every call of the function */
  funcctx = SRF_PERCALL_SETUP();
  /* get state */
  state = funcctx->user_fctx;
  /* Stop when we've used up all the grid squares */
  if (state->done)
  {
    SRF_RETURN_DONE(funcctx);
  }

  /* Store tile coordinates */
  Datum coords[4];
  coords[0] = Int32GetDatum(state->coords[0]);
  coords[1] = Int32GetDatum(state->coords[1]);
  int ndims = 2;
  if (state->hasz)
    coords[ndims++] = Int32GetDatum(state->coords[2]);
  if (state->hast && state->tsize > 0)
    coords[ndims++] = Int32GetDatum(state->coords[3]);
  ArrayType *coordarr = intarr_to_array(coords, ndims);
  tuple_arr[0] = PointerGetDatum(coordarr);

  /* Generate box */
  STBOX *box = stbox_tile(state->hasz, state->hast, state->srid, 
    state->sorigin, state->torigin, state->size, state->tsize,
    state->coords);
  stbox_tile_state_next(state);
  tuple_arr[1] = PointerGetDatum(box);

  /* Form tuple and return */
  tuple = heap_form_tuple(funcctx->tuple_desc, tuple_arr, isnull);
  result = HeapTupleGetDatum(tuple);
  SRF_RETURN_NEXT(funcctx, result);
}

PG_FUNCTION_INFO_V1(stbox_multidim_tile);
/**
 * Generate a tile in a multidimensional grid for temporal points.
 *
 * Signatures
 * @code
 * stbox_multidim_tile(ArrayType coords, size double,
 *   sorigin geometry default DEFAULT_SPATIAL_ORIGIN)
 * stbox_multidim_tile(ArrayType coords, size double, interval Interval,
 *   sorigin geometry default DEFAULT_SPATIAL_ORIGIN,
 *   origin TimestampTz default DEFAULT_TIME_ORIGIN)
 * @endcode
*/
Datum stbox_multidim_tile(PG_FUNCTION_ARGS)
{
  ArrayType *coordarr = PG_GETARG_ARRAYTYPE_P(0);
  ensure_non_empty_array(coordarr);
  int ndims;
  int *coords = intarr_extract(coordarr, &ndims);
  if (ndims < 2 || ndims > 4)
    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
      errmsg("The number of coordinates must be between 2 and 4")));
  double size = PG_GETARG_FLOAT8(1);
  ensure_positive_double(size);
  GSERIALIZED *sorigin;
  int64 tsize = 0;
  bool hast = false;
  TimestampTz torigin = 0;

  if (PG_NARGS() == 3)
    sorigin = PG_GETARG_GSERIALIZED_P(2);
  else /* PG_NARGS() == 5 */
  {
    /* If time arguments are given */
    if (ndims == 2)
      ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
        errmsg("The number of coordinates must be at least 3 for the temporal dimension")));
    Interval *duration = PG_GETARG_INTERVAL_P(2);
    ensure_valid_duration(duration);
    tsize = get_interval_units(duration);
    sorigin = PG_GETARG_GSERIALIZED_P(3);
    torigin = PG_GETARG_TIMESTAMPTZ(4);
    hast = true;
  }

  ensure_non_empty(sorigin);
  ensure_point_type(sorigin);
  int32 srid = gserialized_get_srid(sorigin);
  bool hasz = (ndims == 4 || (ndims == 3 && ! hast));
  POINT3DZ p;
  if (FLAGS_GET_Z(sorigin->flags))
    p = datum_get_point3dz(PointerGetDatum(sorigin));
  else
  {
    /* Initialize to 0 the Z dimension if it is missing */
    memset(&p, 0, sizeof(POINT3DZ));
    const POINT2D *p2d = gs_get_point2d_p(sorigin);
    p.x = p2d->x;
    p.y = p2d->y;
  }
  STBOX *result = stbox_tile(hasz, hast, srid, p, torigin, size, tsize,
    coords);
  PG_FREE_IF_COPY(coordarr, 0);
  PG_RETURN_POINTER(result);
}

/*****************************************************************************/

