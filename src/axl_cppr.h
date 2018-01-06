/*
 * Copyright (c) 2009, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by Adam Moody <moody20@llnl.gov>.
 * LLNL-CODE-411039.
 * All rights reserved.
 * This file is part of The Scalable Checkpoint / Restart (SCR) library.
 * For details, see https://sourceforge.net/projects/scalablecr/
 * Please also read this file: LICENSE.TXT.
*/

/* All rights reserved. This program and the accompanying materials
* are made available under the terms of the BSD-3 license which accompanies this
* distribution in LICENSE.TXT
*
* This library is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the BSD-3  License in
* LICENSE.TXT for more details.
*
* GOVERNMENT LICENSE RIGHTS-OPEN SOURCE SOFTWARE
* The Government's rights to use, modify, reproduce, release, perform,
* display, or disclose this software are subject to the terms of the BSD-3
* License as provided in Contract No. B609815.
* Any reproduction of computer software, computer software documentation, or
* portions thereof marked with this legend must also reproduce the markings.
*
* Author: Christopher Holguin <christopher.a.holguin@intel.com>
*
* (C) Copyright 2015-2016 Intel Corporation.
*/

/* a possible code cleanup activity would be to make a table of function
 * pointers that can be switched out based upon which underlying file movement
 * service is being used */
struct axl_cppr_info {
  char *src_dir;
  char *dst_dir;
  char *filename;
  bool has_completed;
  bool alloced;
  unsigned long filesize;

  /* below is a placeholder */
  unsigned long previous_bytes_transferred;
};

/* global to contain all metadata for a cppr op
 * please see below; an index in this array contains the metadata for a handle
 * in the cppr_ops array*/
static struct axl_cppr_info* axl_flush_async_cppr_metadata = NULL;

/* global to contain only the CPPR op handles
 * the index in this array is the same index to use in the
 * scr_flush_async_cppr_metadata array for the metadata related to a given handle */
static struct cppr_op_info* cppr_ops = NULL;

/* global to contain the current count of cppr ops */
static int axl_flush_async_cppr_index = 0;

/* size of allocation blocks for calloc and realloc
 * note: this is only a unitless counter.  needs to be converted to bytes
 *   based upon the struct that is being allocated
 * TODO: SHOULD THIS BE USER CONFIGURABLE?? */
static const int axl_flush_async_cppr_alloc = 20;

/* indicates how many times calloc/realloc have been called */
static int axl_cppr_currently_alloced = 0;

/* free memory associated with the scr_cppr_info type
 * lengths of the two arrays will always be the same */
static void __free_axl_cppr_info(struct axl_cppr_info *metadata_ptr, struct cppr_op_info *handles_ptr, const int length);

static int axl_cppr_flush_async_test(scr_filemap* map, int id, double* bytes);
static int axl_cppr_flush_async_complete(scr_filemap* map, int id);
static int axl_cppr_flush_async_stop(void);
static int axl_cppr_flush_async_start(fu_filemap* map, int id);
static int axl_cppr_init();
