#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <time.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/syscall.h>
#include "axl_internal.h"
#include "axl_async_bbapi.h"

#ifdef HAVE_BBAPI
#include <bbapi.h>

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

/*
 * HACK
 *
 * Return a unique ID number for this node (tied to the hostname).
 *
 * The IBM BB API requires the user assign a unique node ID for the
 * 'contribid' when you start up the library.  IBM assumes you'd specify
 * the MPI rank here, but the bbapi, nor AXL, explicitly requires MPI.
 * Therefore, we return the numbers at the end of our hostname:
 * "sierra123" would return 123.
 *
 * This result is stored in id.  Returns 0 on success, nonzero otherwise.
 */
static int axl_get_unique_node_id(int *id)
{
    char hostname[256] = {0}; /* Max hostname + \0 */
    int rc;
    int i;
    size_t len;
    int sawnum = 0;

    rc = gethostname(hostname, sizeof(hostname));
    if (rc) {
        fprintf(stderr, "Hostname too long\n");
        return 1;
    }

    len = strlen(hostname);

    rc = 1;
    /* Look from the back of the string to find the beginning of our number */
    for (i = len - 1; i >= 0; i--) {
        if (isdigit(hostname[i])) {
            sawnum = 1;
        } else {
            if (sawnum) {
                /*
                 * We were seeing a number, but now we've hit a non-digit.
                 * We're done.
                 */
                *id = atoi(&hostname[i + 1]);
                rc = 0;
                break;
            }
        }
    }
    return rc;
}

/* Called from AXL_Init */
int axl_async_init_bbapi(void) {
#ifdef HAVE_BBAPI
    // TODO: BBAPI wants MPI rank information here?
    int rank;
    int rc;

    rc = axl_get_unique_node_id(&rank);
    if (rc) {
            fprintf(stderr, "Couldn't get unique node id\n");
            return AXL_FAILURE;
    }

    rc = BB_InitLibrary(rank, BBAPI_CLIENTVERSIONSTR);
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

#ifdef HAVE_BBAPI
/*
 * Returns a unique BBTAG into *tag.  The tag returned must be unique
 * such that no two callers on the node will ever get the same tag
 * within a job.
 *
 * Returns 0 on success, 1 otherwise.
 */
static BBTAG axl_get_unique_tag(void)
{
    struct timespec now;
    pid_t tid;
    uint64_t timestamp;

    clock_gettime(CLOCK_MONOTONIC, &now);

    timestamp = now.tv_sec;

    /* Get thread ID.  This is non-portable, Linux only. */
    tid = syscall(__NR_gettid);

    /*
     * This is somewhat of a hack.  Create a unique ID using the UNIX timestamp
     * in the top 32-bits, and the thread ID in the lower 32.  This should be
     * fine unless the user wraps the TID in the same second.  In order to do
     * that they would have to spawn more than /proc/sys/kernel/pid_max
     * processes (currently 180k+ on my development system) in the same second.
     */
    return (timestamp << 32 | (uint32_t) tid);
}
#endif

/* Called from AXL_Create
 * BBTransferHandle and BBTransferDef are created and stored */
int axl_async_create_bbapi(int id) {
#ifdef HAVE_BBAPI
    int rc;
    kvtree* file_list = kvtree_get_kv_int(axl_file_lists, AXL_KEY_HANDLE_UID, id);
    BBTAG bbtag;

    /* allocate a new transfer definition */
    BBTransferDef_t *tdef;
    rc = BB_CreateTransferDef(&tdef);

    /* If failed to create definition then return error. There is no cleanup necessary. */
    if (rc) {
        return bb_check(rc);
    }

    /* allocate a new transfer handle,
     * include AXL transfer id in IBM BB tag */
    BBTransferHandle_t thandle;
    bbtag = axl_get_unique_tag();

    rc = BB_GetTransferHandle(bbtag, 1, NULL, &thandle);

    /* If transfer handle call failed then return. Do not record anything. */
    if (rc) {
        return bb_check(rc);
    }

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
    kvtree_util_get_ptr(file_list, AXL_BBAPI_KEY_TRANSFERDEF, (void**) &tdef);

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
    kvtree_util_get_unsigned_long(file_list, AXL_BBAPI_KEY_TRANSFERHANDLE, (unsigned long*) &thandle);
    kvtree_util_get_ptr(file_list, AXL_BBAPI_KEY_TRANSFERDEF, (void**) &tdef);

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

    /* free the transfer definition */
    rc = BB_FreeTransferDef(tdef);
    bb_check(rc);

    /* drop transfer definition from kvtree */
    kvtree_unset(file_list, AXL_BBAPI_KEY_TRANSFERDEF);

    return AXL_SUCCESS;
#endif
    return AXL_FAILURE;
}

int axl_async_test_bbapi (int id) {
#ifdef HAVE_BBAPI
    kvtree* file_list = kvtree_get_kv_int(axl_file_lists, AXL_KEY_HANDLE_UID, id);

    /* Get the BB-Handle to query the status */
    BBTransferHandle_t thandle;
    kvtree_util_get_unsigned_long(file_list, AXL_BBAPI_KEY_TRANSFERHANDLE, (unsigned long*) &thandle);

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

int axl_async_cancel_bbapi (int id) {
#ifdef HAVE_BBAPI
    kvtree* file_list = kvtree_get_kv_int(axl_file_lists, AXL_KEY_HANDLE_UID, id);

    /* Get the BB-Handle to query the status */
    BBTransferHandle_t thandle;
    kvtree_util_get_unsigned_long(file_list, AXL_BBAPI_KEY_TRANSFERHANDLE, (unsigned long*) &thandle);

    /* TODO: want all processes to do this, or just one? */
    /* attempt to cancel this transfer */
    int rc = BB_CancelTransfer(thandle, BBSCOPETAG);
    int ret = bb_check(rc);

    /* TODO: can we update status of transfer at this point? */

    return ret;
#endif
    return AXL_FAILURE;
}
