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

#ifdef HAVE_CPPR
#include "cppr.h"
#endif /* HAVE_CPPR */

/*
=========================================
Global Variables
========================================
*/

/* AXL's flush file, SCR has one as well */
char* axl_flush_file = NULL;

/* transfer file */
char* axl_transfer_file = NULL;

/*
=========================================
API Functions
========================================
*/

/* Read configuration from non-AXL-specific file
  Also, start up vendor specific services */
int AXL_Init (char* conf_file) {
    // TODO: parse config file to find a place where axl can write metadata
    char* axl_cntl_dir = "/tmp";

    char* axl_flush_file_name = "/axl_flush.info";
    axl_flush_file = malloc(strlen(axl_cntl_dir) + strlen(axl_flush_file_name));
    strcpy(axl_flush_file, axl_cntl_dir);
    strcat(axl_flush_file, axl_flush_file_name);

#ifdef HAVE_DAEMON
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

#ifdef HAVE_LIBCPPR
    /* attempt to init cppr */
    int cppr_ret = cppr_status();
    if (cppr_ret != CPPR_SUCCESS) {
        axl_abort(-1, "libcppr cppr_status() failed: %d '%s' @ %s:%d",
                  cppr_ret, cppr_err_to_str(cppr_ret), __FILE__, __LINE__
                  );
    }
    axl_dbg(1, "#bold CPPR is present @ %s:%d", __FILE__, __LINE__);
#endif /* HAVE_LIBCPPR */

#ifdef HAVE_BBAPI
    // TODO: BBAPI wants MPI rank information here?
    int rank = 0;
    int bbapi_ret = BB_InitLibrary(rank, BBAPI_CLIENTVERSIONSTR);
    if (bbapi_ret != 0) {
        axl_abort(-1, "BBAPI Failed to initialize");
    }
#endif

  return AXL_SUCCESS;
}

/* Shutdown any vendor services */
int AXL_Finalize (void) {
#ifdef HAVE_DAEMON
    axl_free(&axl_transfer_file);
#endif
    axl_file_unlink(axl_flush_file);

#ifdef HAVE_LIBCPPR

#endif

#ifdef HAVE_BBAPI
    int rc = BB_TerminateLibrary();
#endif

    return AXL_SUCCESS;
}

/* Create a transfer handle (used for 0+ files)
 * Type specifies a particular method to use
 * Name is a user/application provided string
 * Returns an ID to the transfer handle */
int AXL_Create (char* type, const char* name) {
    return AXL_SUCCESS;
}

/* Add a file to an existing transfer handle */
int AXL_Add (int id, char* source, char* destination) {
    return AXL_SUCCESS;
}

/* Initiate a transfer for all files in handle ID */
int AXL_Dispatch (int id) {
    return AXL_SUCCESS;
}

/* Test if a transfer has completed
 * Returns 1 if the transfer has completed */
int AXL_Test(int id) {
    return AXL_SUCCESS;
}

/* BLOCKING
 * Wait for a transfer to complete */
int AXL_Wait (int id) {
    return AXL_SUCCESS;
}

/* Cancel an existing transfer */
// TODO: Does cancel call free?
int AXL_Cancel (int id) {
    return AXL_SUCCESS;
}

/* Perform cleanup of internal data associated with ID */
int AXL_Free (int id) {
    return AXL_SUCCESS;
}
