#include <stdlib.h>
#include <string.h>

/* dirname */
#include <libgen.h>

/* uLong type for CRCs */
#include <zlib.h>

/* mkdir */
#include <sys/types.h>
#include <sys/stat.h>

/* axl_xfer_t */
#include "axl.h"

/* kvtree & everything else */
#include "axl_internal.h"
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

/*
=========================================
Helper Functions
========================================
*/

/* TODO: implement this */
axl_xfer_t axl_parse_type_string (const char* type)
{
    if (strcmp(type, "AXL_XFER_SYNC") == 0) {
      return AXL_XFER_SYNC;
    } else if (strcmp(type, "AXL_XFER_ASYNC_DAEMON") == 0) {
      return AXL_XFER_ASYNC_DAEMON;
    } else {
      return -1;
    }
}

/*
=========================================
API Functions
========================================
*/

/* Read configuration from non-AXL-specific file
  Also, start up vendor specific services */
int AXL_Init (const char* conf_file)
{
    int rc = AXL_SUCCESS;

    axl_next_handle_UID = 0;
    axl_file_lists = kvtree_new();

    char* axl_cntl_dir = NULL;
    axl_read_config(&axl_cntl_dir);

    /* TODO: what is the flush file for? */
    char* axl_flush_file_name = "/axl_flush.info";
    axl_flush_file = malloc(strlen(axl_cntl_dir) + strlen(axl_flush_file_name));
    strcpy(axl_flush_file, axl_cntl_dir);
    strcat(axl_flush_file, axl_flush_file_name);

    axl_free(&axl_cntl_dir);

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

/*    axl_file_unlink(axl_flush_file); */

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

    return rc;
}

/* Create a transfer handle (used for 0+ files)
 * Type specifies a particular method to use
 * Name is a user/application provided string
 * Returns an ID to the transfer handle */
int AXL_Create (const char* type, const char* name)
{
    /* Parse type of transfer */
    axl_xfer_t xtype = axl_parse_type_string(type);

    /* Generate next unique ID */
    int id = ++axl_next_handle_UID;

    /* Create an entry for this transfer handle
     * record user string and transfer type 
     * UID
     *   id
     *     NAME
     *       name
     *     TYPE
     *       typestr
     *     TYPE
     *       type_enum
     *     STATUS
     *       SOURCE */
    kvtree* file_list = kvtree_set_kv_int(axl_file_lists, AXL_KEY_HANDLE_UID, id);
    kvtree_util_set_str(file_list, AXL_KEY_UNAME, name);
    kvtree_util_set_str(file_list, AXL_KEY_XFER_TYPE_STR, type);
    kvtree_util_set_int(file_list, AXL_KEY_XFER_TYPE_INT, xtype);
    kvtree_util_set_int(file_list, AXL_KEY_STATUS, AXL_STATUS_SOURCE);

    /* create a structure based on transfer type */
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
        axl_async_create_bbapi(id);
        break;
    case AXL_XFER_ASYNC_CPPR:
        break;
    default:
        axl_err("AXL_Create failed: unknown transfer type '%s' (%d)", type, (int) xtype);
        break;
    }

    return id;
}

/* Add a file to an existing transfer handle */
int AXL_Add (int id, const char* source, const char* destination)
{
    /* lookup transfer info for the given id */
    kvtree* file_list = kvtree_get_kv_int(axl_file_lists, AXL_KEY_HANDLE_UID, id);
    if (file_list == NULL) {
        axl_err("AXL_Add failed: could not find fileset for UID %d", id);
        return AXL_FAILURE;
    }

    /* extract the transfer type */
    int itype;
    kvtree_util_get_int(file_list, AXL_KEY_XFER_TYPE_INT, &itype);
    axl_xfer_t xtype = (axl_xfer_t) itype;

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
        return axl_async_add_bbapi(id, source, destination);
    case AXL_XFER_ASYNC_CPPR:
        break;
    default:
        axl_err("AXL_Add failed: unknown transfer type (%d)", (int) xtype);
        break;
    }

    return AXL_SUCCESS;
}

/* Initiate a transfer for all files in handle ID */
int AXL_Dispatch (int id)
{
    /* lookup transfer info for the given id */
    kvtree* file_list = kvtree_get_kv_int(axl_file_lists, AXL_KEY_HANDLE_UID, id);
    if (file_list == NULL) {
        axl_err("AXL_Dispatch failed: could not find fileset for UID %d", id);
        return AXL_FAILURE;
    }

    /* extract the transfer type */
    int itype;
    kvtree_util_get_int(file_list, AXL_KEY_XFER_TYPE_INT, &itype);
    axl_xfer_t xtype = (axl_xfer_t) itype;

    /* create destination directories and compute CRC32 for each file */
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

        /* calculate CRCs for each file */
        uLong crc;
        int rc = kvtree_util_get_crc32(elem_hash, AXL_KEY_FILE_CRC, &crc);
        if (rc != KVTREE_SUCCESS) {
            axl_crc32(source, &crc);
            kvtree_util_set_crc32(elem_hash, AXL_KEY_FILE_CRC, crc);
        }
    }

    /* NOTE FOR XFER INTERFACES
     * each interface should update AXL_KEY_STATUS
     * all well as AXL_KEY_FILE_STATUS for each file */
    switch (xtype) {
    case AXL_XFER_SYNC:
        return axl_sync_start(id);
#ifdef HAVE_DAEMON
    case AXL_XFER_ASYNC_DAEMON:
        return axl_async_start_daemon(axl_file_lists, id);
#endif
    case AXL_XFER_ASYNC_DW:
        return axl_async_start_datawarp(id);
    case AXL_XFER_ASYNC_BBAPI:
        return axl_async_start_bbapi(id);
    /* case AXL_XFER_ASYNC_CPPR:
        return axl_async_start_cppr(id); */
    default:
        axl_err("AXL_Dispatch failed: unknown transfer type (%d)", (int) xtype);
        break;
    }

    return AXL_SUCCESS;
}

/* Test if a transfer has completed
 * Returns AXL_SUCCESS if the transfer has completed */
int AXL_Test(int id)
{
    /* lookup transfer info for the given id */
    kvtree* file_list = kvtree_get_kv_int(axl_file_lists, AXL_KEY_HANDLE_UID, id);
    if (file_list == NULL) {
        axl_err("AXL_Test failed: could not find fileset UID=%d", id);
        return AXL_FAILURE;
    }

    /* extract the transfer type */
    int itype;
    kvtree_util_get_int(file_list, AXL_KEY_XFER_TYPE_INT, &itype);
    axl_xfer_t xtype = (axl_xfer_t) itype;

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
    switch (xtype) {
    case AXL_XFER_SYNC:
        return axl_sync_test(id);
#ifdef HAVE_DAEMON
    case AXL_XFER_ASYNC_DAEMON:
        return axl_async_test_daemon(axl_file_lists, id, &bytes_total, &bytes_written);
#endif
    case AXL_XFER_ASYNC_DW:
        return axl_async_test_datawarp(id);
    case AXL_XFER_ASYNC_BBAPI:
        return axl_async_test_bbapi(id);
    /* case AXL_XFER_ASYNC_CPPR:
        return axl_async_test_cppr(id); */
    default:
        axl_err("AXL_Test failed: unknown transfer type (%d)", (int) xtype);
        break;
    }

    /* assume failure if we fall through */
    return AXL_FAILURE;
}

/* BLOCKING
 * Wait for a transfer to complete */
int AXL_Wait (int id)
{
    /* lookup transfer info for the given id */
    kvtree* file_list = kvtree_get_kv_int(axl_file_lists, AXL_KEY_HANDLE_UID, id);
    if (file_list == NULL) {
        axl_err("AXL_Wait failed: could not find fileset UID=%d", id);
        return AXL_FAILURE;
    }

    /* extract the transfer type */
    int itype;
    kvtree_util_get_int(file_list, AXL_KEY_XFER_TYPE_INT, &itype);
    axl_xfer_t xtype = (axl_xfer_t) itype;

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
    switch (xtype) {
    case AXL_XFER_SYNC:
        return axl_sync_wait(id);
#ifdef HAVE_DAEMON
    case AXL_XFER_ASYNC_DAEMON:
        return axl_async_wait_daemon(axl_file_lists, id);
#endif
    case AXL_XFER_ASYNC_DW:
        return axl_async_wait_datawarp(id);
    case AXL_XFER_ASYNC_BBAPI:
        return axl_async_wait_bbapi(id);
    /* case AXL_XFER_ASYNC_CPPR:
        return axl_async_wait_cppr(id); */
    default:
        axl_err("AXL_Wait failed: unknown transfer type (%d)", (int) xtype);
        break;
    }

    /* assume failure if we fall through */
    return AXL_FAILURE;
}

/* Cancel an existing transfer */
/* TODO: Does cancel call free? */
int AXL_Cancel (int id)
{
    return AXL_SUCCESS;
}

/* Perform cleanup of internal data associated with ID */
int AXL_Free (int id)
{
    return AXL_SUCCESS;
}
