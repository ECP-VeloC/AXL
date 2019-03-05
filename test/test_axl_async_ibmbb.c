/*
 * Copyright (c) 2009, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by Adam Moody <moody20@llnl.gov>.
 * LLNL-CODE-411039.
 * All rights reserved.
 * KVTree was originally part of The Scalable Checkpoint / Restart (SCR) library.
 * For details, see https://sourceforge.net/projects/scalablecr/
 * Please also read this file: LICENSE.TXT.
*/

#include "test_axl.h"
#include "test_axl_async_ibmbb.h"

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <limits.h>
#include <errno.h>

#define TEST_PASS (0)
#define TEST_FAIL (1)

#define TEST_FILE "test_file"
#define TEST_STRING "I am a file"

#define TEST_NAME "test transfer"
#define TEST_DEST "test_file_moved"

int test_axl_async_ibmbb(struct test_args *test_args){
    int rc = TEST_PASS;
    const char *src_path = test_args->src_path;
    const char *dst_path = test_args->dst_path;
    printf("Copying %s to %s\n", src_path, dst_path);

    /* Launch axl, reate a transfer, add test file, dispatch */
    if (AXL_Init(NULL) != AXL_SUCCESS) {
        rc = TEST_FAIL;
        goto cleanup;
    }

    int id = AXL_Create(AXL_XFER_ASYNC_BBAPI, TEST_NAME);
    if (id < 0) {
        rc = TEST_FAIL;
        goto cleanup;
    }

    if (AXL_Add(id, src_path, dst_path) != AXL_SUCCESS) {
        rc = TEST_FAIL;
        goto cleanup;
    }

    if (AXL_Dispatch(id) != AXL_SUCCESS) {
        rc = TEST_FAIL;
        goto cleanup;
    }

#if 0
    if (AXL_Cancel(id) != AXL_SUCCESS) {
        rc = TEST_FAIL;
        goto cleanup;
    }

    /* will return AXL_SUCCESS if not cancelled,
     * and !AXL_SUCCESS if cancelled (or error) */
    AXL_Wait(id);
#else
    /* Wait for transfer to complete and finalize axl */
    if (AXL_Wait(id) != AXL_SUCCESS) {
        rc = TEST_FAIL;
        goto cleanup;
    }
#endif
    if (AXL_Free(id) != AXL_SUCCESS) {
        rc = TEST_FAIL;
        goto cleanup;
    }

    if (AXL_Finalize() != AXL_SUCCESS) {
        rc = TEST_FAIL;
        goto cleanup;
    }

    if (axl_compare_files_or_dirs(src_path, dst_path) != 0) {
        rc = TEST_FAIL;
        goto cleanup;
    }

cleanup:
    /* Unlink test files and return rc */
    printf("Removing %s and %s\n", src_path, dst_path);
    unlink(src_path);
    unlink(dst_path);
    return rc;
}

void test_axl_async_ibmbb_init(){
    register_test(test_axl_async_ibmbb, "test_axl_async_ibmbb");
}
