/*
 * Copyright (c) 2009, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by Elsa Gonsiorowski <gonsie@llnl.gov>.
 * LLNL-CODE-411039.
 * All rights reserved.
 * This file is part of The Scalable Checkpoint / Restart (SCR) library.
 * For details, see https://sourceforge.net/projects/scalablecr/
 * Please also read this file: LICENSE.TXT.
*/

#include "axl_internal.h"

#ifdef HAVE_BBAPI
#include <bbapi.h>
#define AXL_IBM_TAG_OFFSET (100)

/* Check and print BBAPI Error messages */
static int bb_check(int rc) {
  if (rc) {
    char* errstring;
    getLastErrorDetails(BBERRORJSON, &errstring);
    axl_err("AXL Error with BBAPI rc:       %d\n", rc);
    axl_err("AXL Error with BBAPI details:  %s\n", errstring, errstring);
    free(errstring);

    //printf("Aborting due to failures\n");
    //exit(-1);
    return AXL_FAILURE;
  }
  return AXL_SUCCESS;
}
#endif

/* Called from AXL_Init */
int axl_flush_async_init_bbapi(void) {
#ifdef HAVE_BBAPI
    // TODO: BBAPI wants MPI rank information here?
    int rank = 0;
    int rc = BB_InitLibrary(rank, BBAPI_CLIENTVERSIONSTR);
    return bb_check(rc);
#endif
    return AXL_FAILURE;
}

/* Called from AXL_Finalize */
int axl_flush_async_finalize_bbapi(void) {
#ifdef HAVE_BBAPI
    int rc = BB_TerminateLibrary();
    return bb_check(rc);
#endif
    return AXL_FAILURE;
}


/* Called from AXL_Create
 * BBTransferHandle and BBTransferDef are created and stored */
int axl_flush_async_create_bbapi(int id) {
#ifdef HAVE_BBAPI
    kvtree* file_list = kvtree_util_get_kv_int(axl_flush_async_file_lists, AXL_HANDLE_UID, id);

    BBTransferDef_t *tdef;
    BBTransferHandle_t thandle;
    int tag = AXL_IBM_TAG_OFFSET + id;
    int rc = BB_GetTransferHandle(tag, 0, 0, &thandle);
    rc = BB_CreateTransferDef(&tdef);
    kvtree_util_set_unsigned_long(file_list, AXL_BBAPI_KEY_TRANSFERHANDLE, (unsigned long) thandle);
    kvtree_util_set_ptr(file_list, AXL_BBAPI_KEY_TRANSFERDEF, tdef);

    return bb_check(rc);
#endif
    return AXL_FAILURE;
}

/* Called from AXL_Add
 * Adds file source/destination to BBTransferDef */
int axl_flush_async_add_bbapi (int id, char* source, char* destination) {
#ifdef HAVE_BBAPI
    kvtree* file_list = kvtree_util_get_kv_int(axl_flush_async_file_lists, AXL_HANDLE_UID, id);

    BBTransferDef_t *tdef;
    kvtree_util_get_ptr(file_list, AXL_BBAPI_KEY_TRANSFERDEF, tdef);
    int rc = BB_AddFiles(tdef, source, dest, 0);

    return bb_check(rc);
#endif
    return AXL_FAILURE;
}

/* Called from AXL_Dispatch
 * Start the transfer, mark all files & set as INPROG
 * Assumes that mkdirs have already happened */
int axl_flush_async_start_bbapi (int id) {
#ifdef HAVE_BBAPI
    kvtree* file_list = kvtree_get_kv_int(axl_flush_async_file_lists, AXL_HANDLE_UID, id);
    kvtree_util_set_int(file_list, AXL_KEY_FLUSH_STATUS, AXL_FLUSH_STATUS_INPROG);

    /* Pull BB-Def and BB-Handle out of global var */
    BBTransferDef_t *tdef;
    BBTransferHandle_t thandle;
    kvtree_util_get_unsigned_long(file_list, AXL_BBAPI_KEY_TRANSFERHANDLE, (unsigned long) &thandle);
    kvtree_util_get_ptr(file_list, AXL_BBAPI_KEY_TRANSFERDEF, tdef);

    /* If there are 0 files, mark this as a success */
    kvtree* files = kvtree_get(file_list, AXL_KEY_FILES);
    int file_count = kvtree_util_hash_size(files);
    if (file_count == 0) {
        kvtree_util_set_int(file_list, AXL_KEY_FLUSH_STATUS, AXL_FLUSH_STATUS_DEST);
        return AXL_SUCCESS;
    }

    /* Launch the transfer */
    int rc = BB_StartTransfer(tdef, thandle);
    if (bb_check(rc) != AXL_SUCCESS) {
        kvtree_util_set_int(file_list, AXL_KEY_FLUSH_STATUS, AXL_FLUSH_STATUS_ERROR);
        return AXL_FAILURE;
    }

    /* Mark all files as INPROG */
    kvtree_elem* elem;
    for (elem = kvtree_elem_first(files); elem != NULL; elem = kvtree_elem_next(elem)) {
        kvtree* elem_hash = kvtree_elem_hash(elem);
        kvtree_util_set_int(elem_hash, AXL_KEY_FILE_STATUS, AXL_FLUSH_STATUS_INPROG);
    }

    return AXL_SUCCESS;
#endif
    return AXL_FAILURE;
}

int axl_flush_async_test_bbapi (int id) {
#ifdef HAVE_BBAPI
    kvtree* file_list = kvtree_get_kv_int(axl_flush_async_file_lists, AXL_HANDLE_UID, id);

    /* Get the BB-Handle to query the status */
    BB_TransferHandle thandle;
    kvtree_util_get_unsigned_long(file_list, AXL_BBAPI_KEY_TRANSFERHANDLE, (unsigned long) thandle);

    BBTransferInfo_t tinfo;
    int rc = BB_GetTransferInfo(thandle, &tinfo);
    int status = AXL_FLUSH_STATUS_INPROG;
    if (tinfo.status == BBFULLSUCCESS) {
        status = AXL_FLUSH_STATUS_DEST;
    }
    // TODO: add some finer-grain errror checking
    // BBSTATUS
    // - BBINPROGRESS
    // - BBCANCELED
    // - BBFAILED


    /* Mark files & set with appropriate status */
    kvtree_util_set_int(file_list, AXL_KEY_FLUSH_STATUS, status);

    kvtree* files = kvtree_get(file_list, AXL_KEY_FILES);
    kvtree_elem* elem;
    for (elem = kvtree_elem_first(files); elem != NULL; elem = kvtree_elem_next(elem)) {
        kvtree* elem_hash = kvtree_elem_hash(elem);
        kvtree_util_set_int(elem_hash, AXL_KEY_FILE_STATUS, status);
    }

    if (status == AXL_FLUSH_STATUS_DEST) {
        return 1;
    } else if (status == AXL_FLUSH_STATUS_INPROG) {
        return AXL_SUCCESS;
    } else if (status == AXL_FLUSH_STATUS_ERROR) {
        return ALX_FAILURE;
    }
#endif
    return AXL_FAILURE;
}

int axl_flush_async_wait_bbapi (int id) {
#ifdef HAVE_BBAPI
    kvtree* file_list = kvtree_get_kv_int(axl_flush_async_file_lists, AXL_HANDLE_UID, id);
    int status = AXL_FLUSH_STATUS_INPROG;

    /* Sleep until test changes set status */
    int rc;
    while (status == AXL_FLUSH_STATUS_INPROG) {
        rc = axl_flush_async_test_bbapi(id);
        kvtree_util_set_int(file_list, AXL_KEY_FLUSH_STATUS, &status);
        usleep(10*1000*1000);
    }

    if (rc == 1 || rc == AXL_SUCCESS) {
        return AXL_SUCCESS;
    } else {
        return AXL_FAILURE
    }
#endif
    return AXL_FAILURE;
}

int axl_flush_async_stop_bbapi (int id) {
#ifdef HAVE_BBAPI
    // TODO: implement
    return AXL_SUCCESS;
#endif
    return AXL_FAILURE;
}

int axl_flush_async_complete_bbapi (int id) {
#ifdef HAVE_BBAPI
    // TODO: implement
    return AXL_SUCCESS;
#endif
    return AXL_FAILURE;
}
