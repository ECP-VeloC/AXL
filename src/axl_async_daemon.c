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

#ifdef 0
    //#ifdef HAVE_DAEMON
    /* daemon stuff */
    char* axl_transfer_file_name = "/axl_transfer.info";
    axl_transfer_file = malloc(strlen(axl_cntl_dir) + strlen(axl_transfer_file_name));
    strcpy(axl_transfer_file, axl_cntl_dir);
    strcat(axl_transfer_file, axl_transfer_file_name);

    axl_free(&axl_cntl_dir);

    /* wait until transfer daemon is stopped */
    axl_flush_async_stop();

    /* clear out the file */
    /* done by all ranks (to avoid mpi dependency)
     * Could go back to one/node (or other storage desc as appropriate
     */
    axl_file_unlink(axl_transfer_file);
#endif
    return AXL_FAILURE;
}

int axl_flush_async_finalize_daemon(){
#ifdef HAVE_DAEMON
    axl_free(&axl_transfer_file);
    return AXL_SUCCESS;
#endif
    return AXL_FAILURE;
}
