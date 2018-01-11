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

#include <bbapi.h>
#define AXL_IBM_TAG_OFFSET (100)

static int bb_check(int rc) {
  if (rc) {
    char* errstring;
    getLastErrorDetails(BBERRORJSON, &errstring);
    printf("Error rc:       %d\n", rc);
    printf("Error details:  %s\n", errstring, errstring);
    free(errstring);

    //printf("Aborting due to failures\n");
    //exit(-1);
  }
  return AXL_SUCCESS;
}

int axl_flush_async_create_bbapi(int id) {
    kvtree* file_list = kvtree_util_get_kv_int(axl_flush_async_file_lists, AXL_HANDLE_UID, id);

    BBTransferDef_t *tdef;
    BBTransferHandle_t thandle;
    int tag = AXL_IBM_TAG_OFFSET + id;
    int rc = BB_GetTransferHandle(tag, 0, 0, &thandle);
    rc = BB_CreateTransferDef(&tdef);
    kvtree_util_set_unsigned_long(file_list, AXL_BBAPI_KEY_TRANSFERHANDLE, (unsigned long) thandle);
    kvtree_util_set_ptr(file_list, AXL_BBAPI_KEY_TRANSFERDEF, tdef);

    return bb_check(rc);
}


int axl_flush_async_add_file_bbapi (int id, char* source, char* destination) {
    kvtree* file_list = kvtree_util_get_kv_int(axl_flush_async_file_lists, AXL_HANDLE_UID, id);

    BBTransferDef_t *tdef;
    kvtree_util_get_ptr(file_list, AXL_BBAPI_KEY_TRANSFERDEF, tdef);
    int rc = BB_AddFiles(tdef, source, dest, 0);

    return bb_check(rc);
}

int axl_flush_async_start_bbapi (int id) {
    kvtree* file_list = kvtree_get_kv_int(axl_flush_async_file_lists, AXL_HANDLE_UID, id);

    BBTransferDef_t *tdef;
    BBTransferHandle_t thandle;
    kvtree_util_get_unsigned_long(file_list, AXL_BBAPI_KEY_TRANSFERHANDLE, (unsigned long) &thandle);
    kvtree_util_get_ptr(file_list, AXL_BBAPI_KEY_TRANSFERDEF, tdef);

    kvtree* files = kvtree_get(file_list, AXL_KEY_FILES);
    int file_count = kvtree_util_hash_size(files);
    if (file_count == 0) {
        kvtree_util_set_int(file_list, AXL_KEY_FLUSH_STATUS, AXL_FLUSH_STATUS_DEST);
        return AXL_SUCCESS;
    }

    int rc = BB_StartTransfer(tdef, thandle);
    if (bb_check(rc) != AXL_SUCCESS) {
        kvtree_util_set_int(file_list, AXL_KEY_FLUSH_STATUS, AXL_FLUSH_STATUS_ERROR);
        return AXL_FAILURE;
    }

    kvtree_elem* elem;
    for (elem = kvtree_elem_first(files); elem != NULL; elem = kvtree_elem_next(elem)) {
        kvtree* elem_hash = kvtree_elem_hash(elem);
        kvtree_util_set_int(elem_hash, AXL_KEY_FILE_STATUS, AXL_FLUSH_STATUS_INPROG);
    }
    kvtree_util_set_int(file_list, AXL_KEY_FLUSH_STATUS, AXL_FLUSH_STATUS_INPROG);

    return AXL_SUCCESS;
}

int axl_async_flush_test_bbapi (int id) {
    kvtree* file_list = kvtree_get_kv_int(axl_flush_async_file_lists, AXL_HANDLE_UID, id);
    BB_TransferHandle thandle;
    kvtree_util_get_unsigned_long(file_list, AXL_BBAPI_KEY_TRANSFERHANDLE, (unsigned long) thandle);

    BBTransferInfo_t tinfo;
    int rc = BB_GetTransferInfo(thandle, &tinfo);
    if (tinfo.status == BBFULLSUCCESS) {
        transfer_complete = 1;
    } else {
        // BBSTATUS
        // - BBINPROGRESS
        // - BBCANCELED
        // - BBFAILED
        transfer_complete = 0;
    }

    return AXL_SUCCESS;
}
