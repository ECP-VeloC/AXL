#include "axl_internal.h"
#include "axl_async_bbapi.h"

#ifdef HAVE_BBAPI
#include <bbapi.h>
#define AXL_IBM_TAG_OFFSET (100)

static void getLastErrorDetails(BBERRORFORMAT pFormat, char** pBuffer)
{
    int rc;
    size_t l_NumBytesAvailable;
    if(pBuffer)
    {
        rc = BB_GetLastErrorDetails(pFormat, &l_NumBytesAvailable, 0, NULL);
        if(rc == 0)
        {
            *pBuffer = (char*)malloc(l_NumBytesAvailable+1);
            BB_GetLastErrorDetails(pFormat, NULL, l_NumBytesAvailable, *pBuffer);
        }
        else
        {
            *pBuffer = NULL;
        }
    }
}

/* Check and print BBAPI Error messages */
static int bb_check(int rc) {
  if (rc) {
    char* errstring;
    getLastErrorDetails(BBERRORJSON, &errstring);
    AXL_ERR("AXL Error with BBAPI rc:       %d", rc);
    AXL_ERR("AXL Error with BBAPI details:  %s", errstring);
    free(errstring);

    //printf("Aborting due to failures\n");
    //exit(-1);
    return AXL_FAILURE;
  }
  return AXL_SUCCESS;
}
#endif

/* Called from AXL_Init */
int axl_async_init_bbapi(void) {
#ifdef HAVE_BBAPI
    // TODO: BBAPI wants MPI rank information here?
    int rank = 0;
    int rc = BB_InitLibrary(rank, BBAPI_CLIENTVERSIONSTR);
    return bb_check(rc);
#endif
    return AXL_FAILURE;
}

/* Called from AXL_Finalize */
int axl_async_finalize_bbapi(void) {
#ifdef HAVE_BBAPI
    int rc = BB_TerminateLibrary();
    return bb_check(rc);
#endif
    return AXL_FAILURE;
}


/* Called from AXL_Create
 * BBTransferHandle and BBTransferDef are created and stored */
int axl_async_create_bbapi(int id) {
#ifdef HAVE_BBAPI
    kvtree* file_list = kvtree_get_kv_int(axl_file_lists, AXL_KEY_HANDLE_UID, id);

    /* allocate a new transfer handle,
     * include AXL transfer id in IBM BB tag */
    BBTransferHandle_t thandle;
    int tag = AXL_IBM_TAG_OFFSET + id;
    int rc = BB_GetTransferHandle(tag, 0, 0, &thandle);

    /* TODO: skip this if transfer handle call failed */

    /* allocate a new transfer definition */
    BBTransferDef_t *tdef;
    rc = BB_CreateTransferDef(&tdef);

    /* TODO: free transfer handle if failed to create definition */

    /* record transfer handle and definition */
    kvtree_util_set_unsigned_long(file_list, AXL_BBAPI_KEY_TRANSFERHANDLE, (unsigned long) thandle);
    kvtree_util_set_ptr(file_list, AXL_BBAPI_KEY_TRANSFERDEF, tdef);

    return bb_check(rc);
#endif
    return AXL_FAILURE;
}

/* Called from AXL_Add
 * Adds file source/destination to BBTransferDef */
int axl_async_add_bbapi (int id, const char* source, const char* dest) {
#ifdef HAVE_BBAPI
    kvtree* file_list = kvtree_get_kv_int(axl_file_lists, AXL_KEY_HANDLE_UID, id);

    /* get transfer definition for this id */
    BBTransferDef_t *tdef;
    kvtree_util_get_ptr(file_list, AXL_BBAPI_KEY_TRANSFERDEF, tdef);

    /* add file to transfer definition */
    int rc = BB_AddFiles(tdef, source, dest, 0);
    return bb_check(rc);
#endif
    return AXL_FAILURE;
}

/* Called from AXL_Dispatch
 * Start the transfer, mark all files & set as INPROG
 * Assumes that mkdirs have already happened */
int axl_async_start_bbapi (int id) {
#ifdef HAVE_BBAPI
    kvtree* file_list = kvtree_get_kv_int(axl_file_lists, AXL_KEY_HANDLE_UID, id);

    /* mark this transfer as in progress */
    kvtree_util_set_int(file_list, AXL_KEY_STATUS, AXL_STATUS_INPROG);

    /* Pull BB-Def and BB-Handle out of global var */
    BBTransferDef_t *tdef;
    BBTransferHandle_t thandle;
    kvtree_util_get_unsigned_long(file_list, AXL_BBAPI_KEY_TRANSFERHANDLE, (unsigned long) &thandle);
    kvtree_util_get_ptr(file_list, AXL_BBAPI_KEY_TRANSFERDEF, tdef);

#if 0
    /* TODO: is this necessary? */
    /* If there are 0 files, mark this as a success */
    int file_count = kvtree_size(files);
    if (file_count == 0) {
        kvtree_util_set_int(file_list, AXL_KEY_STATUS, AXL_STATUS_DEST);
        return AXL_SUCCESS;
    }
#endif

    /* Launch the transfer */
    int rc = BB_StartTransfer(tdef, thandle);
    if (bb_check(rc) != AXL_SUCCESS) {
        /* something went wrong, update transfer to error state */
        kvtree_util_set_int(file_list, AXL_KEY_STATUS, AXL_STATUS_ERROR);
        return AXL_FAILURE;
    }

    /* Mark all files as INPROG */
    kvtree_elem* elem;
    kvtree* files = kvtree_get(file_list, AXL_KEY_FILES);
    for (elem = kvtree_elem_first(files); elem != NULL; elem = kvtree_elem_next(elem)) {
        kvtree* elem_hash = kvtree_elem_hash(elem);
        kvtree_util_set_int(elem_hash, AXL_KEY_FILE_STATUS, AXL_STATUS_INPROG);
    }

    return AXL_SUCCESS;
#endif
    return AXL_FAILURE;
}

int axl_async_test_bbapi (int id) {
#ifdef HAVE_BBAPI
    kvtree* file_list = kvtree_get_kv_int(axl_file_lists, AXL_KEY_HANDLE_UID, id);

    /* Get the BB-Handle to query the status */
    BBTransferHandle_t thandle;
    kvtree_util_get_unsigned_long(file_list, AXL_BBAPI_KEY_TRANSFERHANDLE, (unsigned long) thandle);

    /* get info about transfer */
    BBTransferInfo_t tinfo;
    int rc = BB_GetTransferInfo(thandle, &tinfo);

    /* check its status */
    int status = AXL_STATUS_INPROG;
    if (tinfo.status == BBFULLSUCCESS) {
        status = AXL_STATUS_DEST;
    }
    // TODO: add some finer-grain errror checking
    // BBSTATUS
    // - BBINPROGRESS
    // - BBCANCELED
    // - BBFAILED

    /* update status of set */
    kvtree_util_set_int(file_list, AXL_KEY_STATUS, status);

    /* update status of each file */
    kvtree* files = kvtree_get(file_list, AXL_KEY_FILES);
    kvtree_elem* elem;
    for (elem = kvtree_elem_first(files); elem != NULL; elem = kvtree_elem_next(elem)) {
        kvtree* elem_hash = kvtree_elem_hash(elem);
        kvtree_util_set_int(elem_hash, AXL_KEY_FILE_STATUS, status);
    }

    /* test returns AXL_SUCCESS if AXL_Wait will not block */
    if (status == AXL_STATUS_DEST) {
        /* AXL_Wait will not block, so return success */
        return AXL_SUCCESS;
    } else if (status == AXL_STATUS_INPROG) {
        return AXL_FAILURE;
    } else if (status == AXL_STATUS_ERROR) {
        /* AXL_Wait will not block, so return success */
        return AXL_SUCCESS;
    }
#endif
    return AXL_FAILURE;
}

int axl_async_wait_bbapi (int id) {
#ifdef HAVE_BBAPI
    kvtree* file_list = kvtree_get_kv_int(axl_file_lists, AXL_KEY_HANDLE_UID, id);

    /* Sleep until test changes set status */
    int rc;
    int status = AXL_STATUS_INPROG;
    while (status == AXL_STATUS_INPROG) {
        /* delegate work to test call to update status */
        axl_async_test_bbapi(id);

        /* if we're not done yet, sleep for some time and try again */
        kvtree_util_get_int(file_list, AXL_KEY_STATUS, &status);
        if (status == AXL_STATUS_INPROG) {
            usleep(10*1000*1000);
        }
    }

    /* we're done now, either with error or success */
    if (status == AXL_STATUS_DEST) {
        return AXL_SUCCESS;
    } else {
        return AXL_FAILURE;
    }
#endif
    return AXL_FAILURE;
}

int axl_async_stop_bbapi (int id) {
#ifdef HAVE_BBAPI
    // TODO: implement
    return AXL_SUCCESS;
#endif
    return AXL_FAILURE;
}
