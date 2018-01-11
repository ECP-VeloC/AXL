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

#include "kvtree.h"
#include "scr_globals.h"
#include "axl_internals.h"

/** NOP all functions. Code is available in scr in scr_flush_async.c */
int axl_flush_async_test_daemon(int id) {
  return AXL_FAILURE;
}

int axl_flush_async_wait_daemon(int id){
  return AXL_FAILURE;
}

int axl_flush_async_stop_daemon(int id){
  return AXL_FAILURE;
}

int axl_flush_async_start_daemon(int id){
  return AXL_FAILURE;
}

int axl_flush_async_complete_daemon(int id){
  return AXL_FAILURE;
}

int axl_flush_async_init_daemon(){
  return AXL_FAILURE;
}

int axl_flush_async_finalize_daemon(){
  return AXL_FAILURE;
}
