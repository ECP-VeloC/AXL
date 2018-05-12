#include <stdlib.h>
#include <string.h>

/* dirname */
#include <libgen.h>

/* mkdir */
#include <sys/types.h>
#include <sys/stat.h>

/* axl_xfer_t */
#include "axl.h"

/* kvtree & everything else */
#include "axl_internal.h"
#include "kvtree.h"
#include "kvtree_util.h"

#include "config.h"

/* xfer methods */
#include "axl_sync.h"
#include "axl_async_bbapi.h"
/*#include "axl_async_cppr.h" */
#ifdef HAVE_DAEMON
#include "axl_async_daemon.h"
#endif
#include "axl_async_datawarp.h"

/* define states for transfer handlesto help ensure
 * users call AXL functions in the correct order */
typedef enum {
    AXL_XFER_STATE_NULL,       /* placeholder for invalid state */
    AXL_XFER_STATE_CREATED,    /* handle has been created */
    AXL_XFER_STATE_DISPATCHED, /* transfer has been dispatched */
    AXL_XFER_STATE_COMPLETED,  /* wait has been called */
} axl_xfer_state_t;

/*
=========================================
Global Variables
========================================
*/

/* AXL's flush file, SCR has one as well */
char* axl_flush_file = NULL;

/* Transfer handle unique IDs */
static int axl_next_handle_UID = -1;

/* tracks list of files written with transfer */
kvtree* axl_file_lists = NULL;

/* given an id, lookup and return the file list and transfer type,
 * returns AXL_FAILURE if info could not be found */
static int axl_get_info(int id, kvtree** list, axl_xfer_t* type, axl_xfer_state_t* state)
{
    /* initialize output parameters to invalid values */
    *list  = NULL;
    *type  = AXL_XFER_NULL;
    *state = AXL_XFER_STATE_NULL;

    /* lookup transfer info for the given id */
    kvtree* file_list = kvtree_get_kv_int(axl_file_lists, AXL_KEY_HANDLE_UID, id);
    if (file_list == NULL) {
        axl_err("Could not find fileset for UID %d @ %s:%d", id, __FILE__, __LINE__);
        return AXL_FAILURE;
    }

    /* extract the transfer type */
    int itype;
    if (kvtree_util_get_int(file_list, AXL_KEY_XFER_TYPE, &itype) != KVTREE_SUCCESS) {
        axl_err("Could not find transfer type for UID %d @ %s:%d", id, __FILE__, __LINE__);
        return AXL_FAILURE;
    }
    axl_xfer_t xtype = (axl_xfer_t) itype;

    /* extract the transfer state */
    int istate;
    if (kvtree_util_get_int(file_list, AXL_KEY_STATE, &istate) != KVTREE_SUCCESS) {
        axl_err("Could not find transfer state for UID %d @ %s:%d", id, __FILE__, __LINE__);
        return AXL_FAILURE;
    }
    axl_xfer_state_t xstate = (axl_xfer_state_t) istate;

    /* set output parameters */
    *list  = file_list;
    *type  = xtype;
    *state = xstate;

    return AXL_SUCCESS;
}

/*
=========================================
API Functions
========================================
*/

/* Read configuration from non-AXL-specific file
  Also, start up vendor specific services */
int AXL_Init (const char* state_file)
{
    int rc = AXL_SUCCESS;

    /* TODO: set these by config file */
    axl_file_buf_size = (size_t) 1048576;

    /* make a copy of the path to file to record our state */
    if (state_file != NULL) {
        axl_flush_file = strdup(state_file);
    }

    axl_next_handle_UID = 0;
    axl_file_lists = kvtree_new();

    /* initialize values from state file if we have one */
    if (axl_flush_file != NULL) {
        /* initialize axl_file_lists from file if we have one */
        kvtree_read_file(axl_flush_file, axl_file_lists);

        /* initialize handle id using highest id in file */
        kvtree* ids = kvtree_get(axl_file_lists, AXL_KEY_HANDLE_UID);
        kvtree_sort_int(ids, KVTREE_SORT_DESCENDING);
        kvtree_elem* elem = kvtree_elem_first(ids);
        if (elem != NULL) {
            int id = kvtree_elem_key_int(elem);
            axl_next_handle_UID = id;
        }
    }

#ifdef HAVE_DAEMON
    char axl_async_daemon_path[] = "axld";
    char axl_async_daemon_file[] = "/dev/shm/axld";
    if (axl_async_init_daemon(axl_async_daemon_path, axl_async_daemon_file) != AXL_SUCCESS) {
        rc = AXL_FAILURE;
    }
#endif
#ifdef HAVE_BBAPI
    if (axl_async_init_bbapi() != AXL_SUCCESS) {
        rc = AXL_FAILURE;
    }
#endif
#ifdef HAVE_LIBCPPR
    if (axl_async_init_cppr() != AXL_SUCCESS) {
        rc = AXL_FAILURE;
    }
#endif

    return rc;
}

/* Shutdown any vendor services */
int AXL_Finalize (void)
{
    int rc = AXL_SUCCESS;

    /* TODO: ok to finalize if we have active transfer handles? */

#ifdef HAVE_DAEMON
    if (axl_async_finalize_daemon() != AXL_SUCCESS) {
        rc = AXL_FAILURE;
    }
#endif
#ifdef HAVE_BBAPI
    if (axl_async_finalize_bbapi() != AXL_SUCCESS) {
        rc = AXL_FAILURE;
    }
#endif
#ifdef HAVE_LIBCPPR
    if (axl_async_finalize_cppr() != AXL_SUCCESS) {
        rc = AXL_FAILURE;
    }
#endif

    /* delete flush file if we have one */
    if (axl_flush_file != NULL) {
        axl_file_unlink(axl_flush_file);
    }
    axl_free(&axl_flush_file);

    return rc;
}

/* Create a transfer handle (used for 0+ files)
 * Type specifies a particular method to use
 * Name is a user/application provided string
 * Returns an ID to the transfer handle */
int AXL_Create (axl_xfer_t xtype, const char* name)
{
    /* Generate next unique ID */
    int id = ++axl_next_handle_UID;

    /* Create an entry for this transfer handle
     * record user string and transfer type 
     * UID
     *   id
     *     NAME
     *       name
     *     TYPE
     *       type_enum
     *     STATUS
     *       SOURCE
     *     STATE
     *       CREATED */
    kvtree* file_list = kvtree_set_kv_int(axl_file_lists, AXL_KEY_HANDLE_UID, id);
    kvtree_util_set_str(file_list, AXL_KEY_UNAME, name);
    kvtree_util_set_int(file_list, AXL_KEY_XFER_TYPE, xtype);
    kvtree_util_set_int(file_list, AXL_KEY_STATUS, AXL_STATUS_SOURCE);
    kvtree_util_set_int(file_list, AXL_KEY_STATE, (int)AXL_XFER_STATE_CREATED);

    /* create a structure based on transfer type */
    int rc = AXL_SUCCESS;
    switch (xtype) {
    case AXL_XFER_SYNC:
        break;
#ifdef HAVE_DAEMON
    case AXL_XFER_ASYNC_DAEMON:
        break;
#endif
    case AXL_XFER_ASYNC_DW:
        break;
    case AXL_XFER_ASYNC_BBAPI:
        rc = axl_async_create_bbapi(id);
        break;
    case AXL_XFER_ASYNC_CPPR:
        break;
    default:
        axl_err("AXL_Create failed: unknown transfer type (%d)", (int) xtype);
        rc = AXL_FAILURE;
        break;
    }

    /* clear entry from our list if something went wrong */
    if (rc != AXL_SUCCESS) {
        kvtree_unset_kv_int(axl_file_lists, AXL_KEY_HANDLE_UID, id);
        id = -1;
    }

    /* write data to file if we have one */
    if (axl_flush_file) {
        kvtree_write_file(axl_flush_file, axl_file_lists);
    }

    return id;
}

/* Add a file to an existing transfer handle */
int AXL_Add (int id, const char* source, const char* destination)
{
    /* lookup transfer info for the given id */
    kvtree* file_list = NULL;
    axl_xfer_t xtype = AXL_XFER_NULL;
    axl_xfer_state_t xstate = AXL_XFER_STATE_NULL;
    if (axl_get_info(id, &file_list, &xtype, &xstate) != AXL_SUCCESS) {
        axl_err("%s failed: could not find transfer info for UID %d", __func__, id);
        return AXL_FAILURE;
    }

    /* check that handle is in correct state to add files */
    if (xstate != AXL_XFER_STATE_CREATED) {
        axl_err("%s failed: invalid state to add files for UID %d", __func__, id);
        return AXL_FAILURE;
    }

    /* add record for this file
     * UID
     *   id
     *     FILES
     *       /path/to/src/file
     *         DEST
     *           /path/to/dest/file
     *         STATUS
     *           SOURCE */
    kvtree* src_hash = kvtree_set_kv(file_list, AXL_KEY_FILES, source);
    kvtree_util_set_str(src_hash, AXL_KEY_FILE_DEST, destination);
    kvtree_util_set_int(src_hash, AXL_KEY_STATUS, AXL_STATUS_SOURCE);

    /* add file to transfer data structure, depending on its type */
    int rc = AXL_SUCCESS;
    switch (xtype) {
    case AXL_XFER_SYNC:
        break;
#ifdef HAVE_DAEMON
    case AXL_XFER_ASYNC_DAEMON:
        break;
#endif
    case AXL_XFER_ASYNC_DW:
        break;
    case AXL_XFER_ASYNC_BBAPI:
        rc = axl_async_add_bbapi(id, source, destination);
        break;
    case AXL_XFER_ASYNC_CPPR:
        break;
    default:
        axl_err("AXL_Add failed: unknown transfer type (%d)", (int) xtype);
        rc = AXL_FAILURE;
        break;
    }

    /* write data to file if we have one */
    if (axl_flush_file) {
        kvtree_write_file(axl_flush_file, axl_file_lists);
    }

    return rc;
}

/* Initiate a transfer for all files in handle ID */
int AXL_Dispatch (int id)
{
    /* lookup transfer info for the given id */
    kvtree* file_list = NULL;
    axl_xfer_t xtype = AXL_XFER_NULL;
    axl_xfer_state_t xstate = AXL_XFER_STATE_NULL;
    if (axl_get_info(id, &file_list, &xtype, &xstate) != AXL_SUCCESS) {
        axl_err("%s failed: could not find transfer info for UID %d", __func__, id);
        return AXL_FAILURE;
    }

    /* check that handle is in correct state to dispatch */
    if (xstate != AXL_XFER_STATE_CREATED) {
        axl_err("%s failed: invalid state to dispatch UID %d", __func__, id);
        return AXL_FAILURE;
    }
    kvtree_util_set_int(file_list, AXL_KEY_STATE, (int)AXL_XFER_STATE_DISPATCHED);

    /* create destination directories for each file */
    kvtree_elem* elem;
    kvtree* files = kvtree_get(file_list, AXL_KEY_FILES);
    for (elem = kvtree_elem_first(files); elem != NULL; elem = kvtree_elem_next(elem)) {
        /* get path to source file */
        char* source = kvtree_elem_key(elem);

        /* get hash for this file */
        kvtree* elem_hash = kvtree_elem_hash(elem);

        /* get destination for this file */
        char* destination;
        kvtree_util_get_str(elem_hash, AXL_KEY_FILE_DEST, &destination);

        /* figure out and create dirs that should exist */
        /* TODO: vendors may implement smarter functions for mkdir */
        char* dest_path = strdup(destination);
        char* dest_dir = dirname(dest_path);
        mode_t mode_dir = axl_getmode(1, 1, 1);
        axl_mkdir(dest_dir, mode_dir);
        axl_free(&dest_path);
    }

    /* NOTE FOR XFER INTERFACES
     * each interface should update AXL_KEY_STATUS
     * all well as AXL_KEY_FILE_STATUS for each file */
    int rc = AXL_SUCCESS;
    switch (xtype) {
    case AXL_XFER_SYNC:
        rc = axl_sync_start(id);
        break;
#ifdef HAVE_DAEMON
    case AXL_XFER_ASYNC_DAEMON:
        rc = axl_async_start_daemon(axl_file_lists, id);
        break;
#endif
    case AXL_XFER_ASYNC_DW:
        rc = axl_async_start_datawarp(id);
        break;
    case AXL_XFER_ASYNC_BBAPI:
        rc = axl_async_start_bbapi(id);
        break;
    /* case AXL_XFER_ASYNC_CPPR:
        rc = axl_async_start_cppr(id); */
        break;
    default:
        axl_err("AXL_Dispatch failed: unknown transfer type (%d)", (int) xtype);
        rc = AXL_FAILURE;
        break;
    }

    /* write data to file if we have one */
    if (axl_flush_file) {
        kvtree_write_file(axl_flush_file, axl_file_lists);
    }

    return rc;
}

/* Test if a transfer has completed
 * Returns AXL_SUCCESS if the transfer has completed */
int AXL_Test (int id)
{
    /* lookup transfer info for the given id */
    kvtree* file_list = NULL;
    axl_xfer_t xtype = AXL_XFER_NULL;
    axl_xfer_state_t xstate = AXL_XFER_STATE_NULL;
    if (axl_get_info(id, &file_list, &xtype, &xstate) != AXL_SUCCESS) {
        axl_err("%s failed: could not find transfer info for UID %d", __func__, id);
        return AXL_FAILURE;
    }

    /* check that handle is in correct state to test */
    if (xstate != AXL_XFER_STATE_DISPATCHED) {
        axl_err("%s failed: invalid state to test UID %d", __func__, id);
        return AXL_FAILURE;
    }

    int status;
    kvtree_util_get_int(file_list, AXL_KEY_STATUS, &status);
    if (status == AXL_STATUS_DEST) {
        return AXL_SUCCESS;
    } else if (status == AXL_STATUS_ERROR) {
        /* we return success since it's done, even on error,
         * caller must call wait to determine whether it was successful */
        return AXL_SUCCESS;
    } else if (status == AXL_STATUS_SOURCE) {
        axl_err("AXL_Test failed: testing a transfer which was never started UID=%d", id);
        return AXL_FAILURE;
    } /* else (status == AXL_STATUS_INPROG) send to XFER interfaces */

    double bytes_total, bytes_written;
    int rc = AXL_SUCCESS;
    switch (xtype) {
    case AXL_XFER_SYNC:
        rc = axl_sync_test(id);
        break;
#ifdef HAVE_DAEMON
    case AXL_XFER_ASYNC_DAEMON:
        rc = axl_async_test_daemon(axl_file_lists, id, &bytes_total, &bytes_written);
        break;
#endif
    case AXL_XFER_ASYNC_DW:
        rc = axl_async_test_datawarp(id);
        break;
    case AXL_XFER_ASYNC_BBAPI:
        rc = axl_async_test_bbapi(id);
        break;
    /* case AXL_XFER_ASYNC_CPPR:
        rc = axl_async_test_cppr(id); */
        break;
    default:
        axl_err("AXL_Test failed: unknown transfer type (%d)", (int) xtype);
        rc = AXL_FAILURE;
        break;
    }

    return rc;
}

/* BLOCKING
 * Wait for a transfer to complete */
int AXL_Wait (int id)
{
    /* lookup transfer info for the given id */
    kvtree* file_list = NULL;
    axl_xfer_t xtype = AXL_XFER_NULL;
    axl_xfer_state_t xstate = AXL_XFER_STATE_NULL;
    if (axl_get_info(id, &file_list, &xtype, &xstate) != AXL_SUCCESS) {
        axl_err("%s failed: could not find transfer info for UID %d", __func__, id);
        return AXL_FAILURE;
    }

    /* check that handle is in correct state to wait */
    if (xstate != AXL_XFER_STATE_DISPATCHED) {
        axl_err("%s failed: invalid state to wait UID %d", __func__, id);
        return AXL_FAILURE;
    }
    kvtree_util_set_int(file_list, AXL_KEY_STATE, (int)AXL_XFER_STATE_COMPLETED);

    /* lookup status for the transfer, return if done */
    int status;
    kvtree_util_get_int(file_list, AXL_KEY_STATUS, &status);
    if (status == AXL_STATUS_DEST) {
        return AXL_SUCCESS;
    } else if (status == AXL_STATUS_ERROR) {
        return AXL_FAILURE;
    } else if (status == AXL_STATUS_SOURCE) {
        axl_err("AXL_Wait failed: testing a transfer which was never started UID=%d", id);
        return AXL_FAILURE;
    } /* else (status == AXL_STATUS_INPROG) send to XFER interfaces */

    /* if not done, call vendor API to wait */
    int rc = AXL_SUCCESS;
    switch (xtype) {
    case AXL_XFER_SYNC:
        rc = axl_sync_wait(id);
        break;
#ifdef HAVE_DAEMON
    case AXL_XFER_ASYNC_DAEMON:
        rc = axl_async_wait_daemon(axl_file_lists, id);
        break;
#endif
    case AXL_XFER_ASYNC_DW:
        rc = axl_async_wait_datawarp(id);
        break;
    case AXL_XFER_ASYNC_BBAPI:
        rc = axl_async_wait_bbapi(id);
        break;
    /* case AXL_XFER_ASYNC_CPPR:
        rc = axl_async_wait_cppr(id); */
        break;
    default:
        axl_err("AXL_Wait failed: unknown transfer type (%d)", (int) xtype);
        rc = AXL_FAILURE;
        break;
    }

    /* write data to file if we have one */
    if (axl_flush_file) {
        kvtree_write_file(axl_flush_file, axl_file_lists);
    }

    return rc;
}

/* Cancel an existing transfer */
/* TODO: Does cancel call free? */
int AXL_Cancel (int id)
{
    /* lookup transfer info for the given id */
    kvtree* file_list = NULL;
    axl_xfer_t xtype = AXL_XFER_NULL;
    axl_xfer_state_t xstate = AXL_XFER_STATE_NULL;
    if (axl_get_info(id, &file_list, &xtype, &xstate) != AXL_SUCCESS) {
        axl_err("%s failed: could not find transfer info for UID %d", __func__, id);
        return AXL_FAILURE;
    }

    /* check that handle is in correct state to cancel */
    if (xstate != AXL_XFER_STATE_DISPATCHED) {
        axl_err("%s failed: invalid state to cancel UID %d", __func__, id);
        return AXL_FAILURE;
    }

    /* lookup status for the transfer, return if done */
    int status;
    kvtree_util_get_int(file_list, AXL_KEY_STATUS, &status);
    if (status == AXL_STATUS_DEST) {
        return AXL_SUCCESS;
    } else if (status == AXL_STATUS_ERROR) {
        /* we return success since it's done, even on error */
        return AXL_SUCCESS;
    }

    /* TODO: if it hasn't started, we don't want to call backend cancel */

    /* if not done, call vendor API to wait */
    int rc = AXL_SUCCESS;
    switch (xtype) {
#if 0
    case AXL_XFER_SYNC:
        rc = axl_sync_cancel(id);
        break;
#endif
#ifdef HAVE_DAEMON
    case AXL_XFER_ASYNC_DAEMON:
        rc = axl_async_cancel_daemon(axl_file_lists, id);
        break;
#endif
#if 0
    case AXL_XFER_ASYNC_DW:
        rc = axl_async_cancel_datawarp(id);
        break;
    case AXL_XFER_ASYNC_BBAPI:
        rc = axl_async_cancel_bbapi(id);
        break;
    /* case AXL_XFER_ASYNC_CPPR:
        rc = axl_async_cancel_cppr(id); */
        break;
#endif
    default:
        axl_err("AXL_Cancel failed: unknown transfer type (%d)", (int) xtype);
        rc = AXL_FAILURE;
        break;
    }

    /* write data to file if we have one */
    if (axl_flush_file) {
        kvtree_write_file(axl_flush_file, axl_file_lists);
    }

    return rc;
}

/* Perform cleanup of internal data associated with ID */
int AXL_Free (int id)
{
    /* lookup transfer info for the given id */
    kvtree* file_list = NULL;
    axl_xfer_t xtype = AXL_XFER_NULL;
    axl_xfer_state_t xstate = AXL_XFER_STATE_NULL;
    if (axl_get_info(id, &file_list, &xtype, &xstate) != AXL_SUCCESS) {
        axl_err("%s failed: could not find transfer info for UID %d", __func__, id);
        return AXL_FAILURE;
    }

    /* check that handle is in correct state to free */
    if (xstate != AXL_XFER_STATE_CREATED &&
        xstate != AXL_XFER_STATE_COMPLETED)
    {
        axl_err("%s failed: invalid state to free UID %d", __func__, id);
        return AXL_FAILURE;
    }

    /* forget anything we know about this id */
    kvtree_unset_kv_int(axl_file_lists, AXL_KEY_HANDLE_UID, id);

    /* write data to file if we have one */
    if (axl_flush_file) {
        kvtree_write_file(axl_flush_file, axl_file_lists);
    }

    return AXL_SUCCESS;
}

int AXL_Stop ()
{
    int rc = AXL_SUCCESS;

#ifdef HAVE_DAEMON
    /* halt the daemon, this will stop it and clear
     * all files from its transfer list */
    if (axl_async_stop_daemon() != AXL_SUCCESS) {
        rc = AXL_FAILURE;
    }
#endif

    /* get list of ids */
    kvtree* ids_hash = kvtree_get(axl_file_lists, AXL_KEY_HANDLE_UID);

    /* get list of ids */
    int numids;
    int* ids;
    kvtree_list_int(ids_hash, &numids, &ids);

    /* cancel and free each active id */
    int i;
    for (i = 0; i < numids; i++) {
        /* get id for this transfer */
        int id = ids[i];

        /* cancel it */
        if (AXL_Cancel(id) != AXL_SUCCESS) {
            rc = AXL_FAILURE;
        }

        /* wait */
        AXL_Wait(id);

        /* and free it */
        if (AXL_Free(id) != AXL_SUCCESS) {
            rc = AXL_FAILURE;
        }
    }

    /* free the list of ids */
    axl_free(&ids);

    return rc;
}
