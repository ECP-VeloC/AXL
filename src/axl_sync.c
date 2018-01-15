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

#include "axl_internal.h"

/* synchonous flush of files */
int axl_flush_sync_start (int id) {

    kvtree* file_list = kvtree_get_kv_int(axl_async_flush_file_lists, AXL_KEY_HANDLE_UID, id);
    kvtree_util_set_int(file_list, AXL_KEY_FLUSH_STATUS, AXL_FLUSH_STATUS_INPROG);

    int flushed = AXL_SUCCESS;

    /* flush each of my files and fill in summary data structure */
    kvtree_elem* elem = NULL;
    kvtree* files = kvtree_get(file_list, AXL_KEY_FILES);
    for (elem = kvtree_elem_first(files); elem != NULL; elem = kvtree_elem_next(elem)){
        /* get the filename */
        char* source = kvtree_elem_key(elem);

        /* get the hash for this file */
        kvtree* elem_hash = kvtree_elem_hash(elem);

        /* WEIRD case: we've restarted a sync flush that was going */
        int status;
        kvtree_util_get_int(elem_hash, AXL_KEY_FILE_STATUS, &status);
        if (status == AXL_FLUSH_STATUS_DEST) {
            /* this file was already flushed */
            continue;
        }

        /* Copy the file */
        char* destination;
        kvtree_util_get_str(elem_hash, AXL_KEY_FILE_DEST, &destination);
        int tmp_rc = axl_file_copy(source, destination, axl_file_buf_size, NULL);
        if (tmp_rc == AXL_SUCCESS) {
            kvtree_util_set_int(elem_hash, AXL_KEY_FILE_STATUS, AXL_FLUSH_STATUS_DEST);
        } else {
            kvtree_util_set_int(elem_hash, AXL_KEY_FILE_STATUS, AXL_FLUSH_STATUS_ERROR);
            flushed = AXL_FAILURE;
        }
        axl_dbg(2, "axl_flush_sync_start: Read and copied %s to %s with success code %d @ %s:%d",
                source, destination, tmp_rc, __FILE__, __LINE__
                );
    }

    if (flushed == AXL_SUCCESS) {
        kvtree_util_set_int(file_list, AXL_KEY_FLUSH_STATUS, AXL_FLUSH_STATUS_DEST);
    } else {
        kvtree_util_set_int(file_list, AXL_KEY_FLUSH_STATUS, AXL_FLUSH_STATUS_ERROR);
    }

    return flushed;
}

int axl_flush_sync_test (int id) {
    // something when really wrong, but let's just keep flushing
    return axl_flush_sync_start(id);
}

int axl_flush_sync_wait (int id) {
    // something when really wrong, but let's just keep flushing
    return axl_flush_sync_start(id);
}
