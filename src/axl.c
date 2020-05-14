#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdio.h>

/* PATH_MAX */
#include <limits.h>

/* dirname */
#include <libgen.h>

/* mkdir */
#include <sys/types.h>
#include <sys/stat.h>

/* opendir */
#include <dirent.h>

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
#include "axl_async_datawarp.h"
#include "axl_pthread.h"

/* define states for transfer handlesto help ensure
 * users call AXL functions in the correct order */
typedef enum {
    AXL_XFER_STATE_NULL,       /* placeholder for invalid state */
    AXL_XFER_STATE_CREATED,    /* handle has been created */
    AXL_XFER_STATE_DISPATCHED, /* transfer has been dispatched */
    AXL_XFER_STATE_WAITING,    /* wait has been called */
    AXL_XFER_STATE_COMPLETED,  /* files are all copied */
    AXL_XFER_STATE_CANCELED,   /* transfer was AXL_Cancel'd */
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
        AXL_ERR("Could not find fileset for UID %d", id);
        return AXL_FAILURE;
    }

    /* extract the transfer type */
    int itype;
    if (kvtree_util_get_int(file_list, AXL_KEY_XFER_TYPE, &itype) != KVTREE_SUCCESS) {
        AXL_ERR("Could not find transfer type for UID %d", id);
        return AXL_FAILURE;
    }
    axl_xfer_t xtype = (axl_xfer_t) itype;

    /* extract the transfer state */
    int istate;
    if (kvtree_util_get_int(file_list, AXL_KEY_STATE, &istate) != KVTREE_SUCCESS) {
        AXL_ERR("Could not find transfer state for UID %d", id);
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
 * Return the native underlying transfer API for this particular node.  If you're
 * running on an IBM node, use the BB API.  If you're running on a Cray, use
 * DataWarp.  Otherwise use sync.
 */
static
axl_xfer_t axl_detect_native_xfer(void)
{
    axl_xfer_t xtype = AXL_XFER_NULL;

    /*
     * In an ideal world, we would detect our node type at runtime, since
     * *technically* we could be compiled with support for both the BB API and
     * DataWarp libraries.  In the real world, our supercomputer is only going
     * to have one of those libraries, so just use whatever we find at
     * build time.
     */
#ifdef HAVE_BBAPI
    xtype = AXL_XFER_ASYNC_BBAPI;
#elif HAVE_DATAWARP
    xtype = AXL_XFER_ASYNC_DW;
#else
    xtype = AXL_XFER_SYNC;
#endif
    return xtype;
}

/*
 * Return the fastest API that's also compatible with all AXL transfers.  We
 * need this since there may be some edge case transfers the native APIs don't
 * support.
 */
static
axl_xfer_t axl_detect_default_xfer(void)
{
    axl_xfer_t xtype = axl_detect_native_xfer();

    /* BBAPI doesn't support shmem, so we can't use it by default. */
    if (xtype == AXL_XFER_ASYNC_BBAPI) {
        xtype = AXL_XFER_SYNC;
    }

    return xtype;
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

    /* initialize our debug level for filterin AXL_DBG messages */
    axl_debug = 0;
    char* val = getenv("AXL_DEBUG");
    if (val != NULL) {
        axl_debug = atoi(val);
    }

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

    if (xtype == AXL_XFER_DEFAULT) {
        xtype = axl_detect_default_xfer();
    } else if (xtype == AXL_XFER_NATIVE) {
        xtype = axl_detect_native_xfer();
    }

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
    case AXL_XFER_ASYNC_DAEMON:
    case AXL_XFER_ASYNC_DW:
    case AXL_XFER_ASYNC_CPPR:
    case AXL_XFER_PTHREAD:
        break;
    case AXL_XFER_ASYNC_BBAPI:
        rc = axl_async_create_bbapi(id);
        break;
    default:
        AXL_ERR("Unknown transfer type (%d)", (int) xtype);
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

/* Is this path a file or a directory?  Return the type. */
enum {PATH_UNKNOWN = 0, PATH_FILE, PATH_DIR};
static int path_type(const char *path) {
    struct stat s;
    if (stat(path, &s) != 0) {
        return 0;
    }
    if (S_ISREG(s.st_mode)) {
        return PATH_FILE;
    }
    if (S_ISDIR(s.st_mode)) {
        return PATH_DIR;
    }
    return PATH_UNKNOWN;
}

/*
 * Add a file to an existing transfer handle.  No directories.
 *
 * If the file's destination path doesn't exist, then automatically create the
 * needed directories.
 */
static int
__AXL_Add (int id, const char* src, const char* dest)
{
    kvtree* file_list = NULL;

    axl_xfer_t xtype = AXL_XFER_NULL;
    axl_xfer_state_t xstate = AXL_XFER_STATE_NULL;
    if (axl_get_info(id, &file_list, &xtype, &xstate) != AXL_SUCCESS) {
        AXL_ERR("Could not find transfer info for UID %d", id);
        return AXL_FAILURE;
    }

    /* check that handle is in correct state to add files */
    if (xstate != AXL_XFER_STATE_CREATED) {
        AXL_ERR("Invalid state to add files for UID %d", id);
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
    kvtree* src_hash = kvtree_set_kv(file_list, AXL_KEY_FILES, src);
    kvtree_util_set_str(src_hash, AXL_KEY_FILE_DEST, dest);
    kvtree_util_set_int(src_hash, AXL_KEY_STATUS, AXL_STATUS_SOURCE);

    /* add file to transfer data structure, depending on its type */
    int rc = AXL_SUCCESS;
    switch (xtype) {
    case AXL_XFER_SYNC:
    case AXL_XFER_PTHREAD:
        break;
    case AXL_XFER_ASYNC_DAEMON:
        break;
    case AXL_XFER_ASYNC_DW:
        break;
    case AXL_XFER_ASYNC_BBAPI:
        /*
         * Special case:
         * The BB API checks to see if the destination path exists at
         * BB_AddFiles() time (analogous to AXL_Add()).  This is an issue
         * since the destination paths get mkdir'd in AXL_Dispatch()
         * and thus aren't available yet.  That's why we hold off on
         * doing our BB_AddFiles() calls until AXL_Dispatch().
         */
        break;
    case AXL_XFER_ASYNC_CPPR:
        break;
    default:
        AXL_ERR("Unknown transfer type (%d)", (int) xtype);
        rc = AXL_FAILURE;
        break;
    }

    /* write data to file if we have one */
    if (axl_flush_file) {
        kvtree_write_file(axl_flush_file, axl_file_lists);
    }

    return rc;
}

/*
 * Add a file or directory to the transfer handle.  If the src is a
 * directory, recursively add all the files and directories in that
 * directory.
 */
int AXL_Add (int id, const char *src, const char *dest)
{
    int rc;
    DIR *dir;
    struct dirent *de;
    char *src_copy, *dest_copy;
    char *src_basename;
    unsigned int src_path_type, dest_path_type;
    char *new_dest, *new_src, *final_dest;

    new_dest = calloc(PATH_MAX, 1);
    if (!new_dest) {
        return ENOMEM;
    }
    new_src = calloc(PATH_MAX, 1);
    if (!new_src) {
        free(new_dest);
        return ENOMEM;
    }
    final_dest = calloc(PATH_MAX, 1);
    if (!final_dest) {
        free(new_dest);
        free(new_src);
        return ENOMEM;
    }

    src_copy = strdup(src);
    dest_copy = strdup(dest);

    src_path_type = path_type(src);
    dest_path_type = path_type(dest);
    src_basename = basename(src_copy);

    switch (src_path_type) {
    case PATH_FILE:
        if (dest_path_type == PATH_DIR) {
            /*
             * They passed a source file, with dest directory.  Append the
             * filename to dest.
             *
             * Before:
             * src          dest
             * /tmp/file1   /tmp/mydir
             *
             * After:
             * /tmp/file1   /tmp/mydir/file1
             */

            snprintf(new_dest, PATH_MAX, "%s/%s", dest, src_basename);
            rc = __AXL_Add(id, src, new_dest);
        } else {
            /* The destination is a filename */
            rc = __AXL_Add(id, src, dest);
        }
        break;
    case PATH_DIR:
        /* Add the directory itself first... */
        if (dest_path_type == PATH_FILE) {
            /* We can't copy a directory onto a file */
            rc = EINVAL;
            break;
       } else if (dest_path_type == PATH_DIR) {
            snprintf(new_dest, PATH_MAX, "%s/%s", dest, src_basename);
        } else {
            /* Our destination doesn't exist */
            snprintf(new_dest, PATH_MAX, "%s", dest);
        }

        /* Traverse all files/dirs in the directory. */
        dir = opendir(src);
        if (!dir) {
            rc = ENOENT;
            break;
        }
        while ((de = readdir(dir)) != NULL) {
            /* Skip '.' and '..' directories */
            if ((strcmp(de->d_name, ".") == 0) || (strcmp(de->d_name, "..") == 0)) {
                continue;
            }
            snprintf(new_src, PATH_MAX, "%s/%s", src, de->d_name);
            snprintf(final_dest, PATH_MAX, "%s/%s", new_dest, de->d_name);

            rc = AXL_Add(id, new_src, final_dest);
            if (rc != AXL_SUCCESS) {
                rc = EINVAL;
                break;
            }
        }
        break;

    default:
        rc = EINVAL;
        break;
    }

    free(dest_copy);
    free(src_copy);
    free(final_dest);
    free(new_src);
    free(new_dest);
    return rc;
}

/* Initiate a transfer for all files in handle ID */
int AXL_Dispatch (int id)
{
    /* lookup transfer info for the given id */
    kvtree* file_list = NULL;
    axl_xfer_t xtype = AXL_XFER_NULL;
    axl_xfer_state_t xstate = AXL_XFER_STATE_NULL;
    kvtree_elem *elem = NULL;
    char *dest;

    if (axl_get_info(id, &file_list, &xtype, &xstate) != AXL_SUCCESS) {
        AXL_ERR("Could not find transfer info for UID %d", id);
        return AXL_FAILURE;
    }

    /* check that handle is in correct state to dispatch */
    if (xstate != AXL_XFER_STATE_CREATED) {
        AXL_ERR("Invalid state to dispatch UID %d", id);
        return AXL_FAILURE;
    }
    kvtree_util_set_int(file_list, AXL_KEY_STATE, (int)AXL_XFER_STATE_DISPATCHED);

    /* create destination directories for each file */
    while ((elem = axl_get_next_path(id, elem, NULL, &dest))) {
        char* dest_path = strdup(dest);
        char* dest_dir = dirname(dest_path);
        mode_t mode_dir = axl_getmode(1, 1, 1);
        axl_mkdir(dest_dir, mode_dir);
        axl_free(&dest_path);
    }

    /*
     * Special case: The BB API checks if the destination path exists at
     * its equivalent of AXL_Add() time.  That's why we do its "AXL_Add"
     * here, after the full path to the file has been created.
     */
    if (xtype == AXL_XFER_ASYNC_BBAPI) {
        /* Set if we're in BBAPI fallback mode */
        if (axl_all_paths_are_bbapi_compatible(id)) {
             kvtree_util_set_int(file_list, AXL_BBAPI_KEY_FALLBACK, 0);
        } else {
             kvtree_util_set_int(file_list, AXL_BBAPI_KEY_FALLBACK, 1);
        }

        if (!axl_bbapi_in_fallback(id)) {
            char *src = NULL;
            int rc;

            /*
             * We're in regular BBAPI mode.  Add the paths before we transfer
             * them.
             */
            elem = NULL;
            while ((elem = axl_get_next_path(id, elem, &src, &dest))) {
                rc = axl_async_add_bbapi(id, src, dest);
                if (rc != AXL_SUCCESS) {
                    return rc;
                }
            }
        }
    }

    /* NOTE FOR XFER INTERFACES
     * each interface should update AXL_KEY_STATUS
     * all well as AXL_KEY_FILE_STATUS for each file */
    int rc = AXL_SUCCESS;
    switch (xtype) {
    case AXL_XFER_SYNC:
        rc = axl_sync_start(id);
        break;
    case AXL_XFER_PTHREAD:
        rc = axl_pthread_start(id);
        break;
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
        AXL_ERR("Unknown transfer type (%d)", (int) xtype);
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
        AXL_ERR("Could not find transfer info for UID %d", id);
        return AXL_FAILURE;
    }

    /* check that handle is in correct state to test */
    if (xstate != AXL_XFER_STATE_DISPATCHED) {
        AXL_ERR("Invalid state to test UID %d", id);
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
        AXL_ERR("Testing a transfer which was never started UID=%d", id);
        return AXL_FAILURE;
    } /* else (status == AXL_STATUS_INPROG) send to XFER interfaces */

    int rc = AXL_SUCCESS;
    switch (xtype) {
    case AXL_XFER_SYNC:
        rc = axl_sync_test(id);
        break;
    case AXL_XFER_PTHREAD:
        rc = axl_pthread_test(id);
        break;
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
        AXL_ERR("Unknown transfer type (%d)", (int) xtype);
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
        AXL_ERR("Could not find transfer info for UID %d", id);
        return AXL_FAILURE;
    }

    /* check that handle is in correct state to wait */
    if (xstate != AXL_XFER_STATE_DISPATCHED) {
        AXL_ERR("Invalid state to wait UID %d", id);
        return AXL_FAILURE;
    }
    kvtree_util_set_int(file_list, AXL_KEY_STATE, (int)AXL_XFER_STATE_WAITING);

    /* lookup status for the transfer, return if done */
    int status;
    kvtree_util_get_int(file_list, AXL_KEY_STATUS, &status);

    if (status == AXL_STATUS_DEST) {
        kvtree_util_set_int(file_list, AXL_KEY_STATE, (int)AXL_XFER_STATE_COMPLETED);
        return AXL_SUCCESS;
    } else if (status == AXL_STATUS_ERROR) {
        return AXL_FAILURE;
    } else if (status == AXL_STATUS_SOURCE) {
        AXL_ERR("Testing a transfer which was never started UID=%d", id);
        return AXL_FAILURE;
    } /* else (status == AXL_STATUS_INPROG) send to XFER interfaces */

    /* if not done, call vendor API to wait */
    int rc = AXL_SUCCESS;
    switch (xtype) {
    case AXL_XFER_SYNC:
        rc = axl_sync_wait(id);
        break;
    case AXL_XFER_PTHREAD:
        rc = axl_pthread_wait(id);
        break;
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
        AXL_ERR("Unknown transfer type (%d)", (int) xtype);
        rc = AXL_FAILURE;
        break;
    }
    kvtree_util_set_int(file_list, AXL_KEY_STATE, (int)AXL_XFER_STATE_COMPLETED);

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
        AXL_ERR("Could not find transfer info for UID %d", id);
        return AXL_FAILURE;
    }

    /* check that handle is in correct state to cancel */
    if (xstate != AXL_XFER_STATE_DISPATCHED &&
        xstate != AXL_XFER_STATE_WAITING) {
        AXL_ERR("Invalid state to cancel UID %d", id);
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
/* TODO: add cancel to backends */
#if 0
    case AXL_XFER_SYNC:
        rc = axl_sync_cancel(id);
        rc = AXL_FAILURE;
        break;
#endif
/* TODO: add cancel to backends */
#if 0
    case AXL_XFER_ASYNC_DW:
        rc = axl_async_cancel_datawarp(id);
        break;
#endif
    case AXL_XFER_ASYNC_BBAPI:
        rc = axl_async_cancel_bbapi(id);
        break;
#if 0
    /* case AXL_XFER_ASYNC_CPPR:
        rc = axl_async_cancel_cppr(id); */
        break;
#endif
    case AXL_XFER_PTHREAD:
        rc = axl_pthread_cancel(id);
        break;
    default:
        AXL_ERR("Unknown transfer type (%d)", (int) xtype);
        rc = AXL_FAILURE;
        break;
    }

    kvtree_util_set_int(file_list, AXL_KEY_STATE, (int)AXL_XFER_STATE_CANCELED);

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
        AXL_ERR("Could not find transfer info for UID %d", id);
        return AXL_FAILURE;
    }

    /* check that handle is in correct state to free */
    if (xstate != AXL_XFER_STATE_CREATED &&
        xstate != AXL_XFER_STATE_COMPLETED &&
        xstate != AXL_XFER_STATE_CANCELED)
    {
        AXL_ERR("Invalid state to free UID %d", id);
        return AXL_FAILURE;
    }

    if (xtype == AXL_XFER_PTHREAD) {
        axl_pthread_free(id);
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

        /* and free it */
        if (AXL_Free(id) != AXL_SUCCESS) {
            rc = AXL_FAILURE;
        }
    }

    /* free the list of ids */
    axl_free(&ids);

    return rc;
}
