#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <time.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/syscall.h>
#include <stdint.h>
#include "axl_internal.h"
#include "axl_async_bbapi.h"
#include "axl_pthread.h"

/*
 * This is the IBM Burst Buffer transfer implementation.  The BB API only
 * supports transferring files between filesystems that support extents.  If
 * the user tries to transfer to an unsupported filesystem, we fallback to
 * a pthread transfer for the entire transfer.
 */
#ifdef HAVE_BBAPI
#include <bbapi.h>
#include <sys/statfs.h>
#include <linux/magic.h>
#include <libgen.h>

/* These aren't always defined in linux/magic.h */
#ifndef GPFS_SUPER_MAGIC
#define GPFS_SUPER_MAGIC 0x47504653 /* "GPFS" in ASCII */
#endif
#ifndef XFS_SUPER_MAGIC
#define XFS_SUPER_MAGIC 0x58465342  /* "XFSB" in ASCII */
#endif

/* Return the filesystem magic value for a given file */
static __fsword_t axl_get_fs_magic(char* path)
{
    /* Stat the path itself */
    struct statfs st;
    int rc = statfs(path, &st);
    if (rc != 0) {
        /*
         * Couldn't statfs the path, which could be normal if the path is the
         * destination path.  Next, try stating the underlying path's directory
         * (which should exist for both the source and destination), for the
         * FS type.
         */
        char* dir = strdup(path);
        if (! dir) {
            return 0;
        } 

        path = dirname(dir);

        rc = statfs(path, &st);
        if (rc != 0) {
            free(dir);
            return 0;
        }
 
        free(dir);
    }

    return st.f_type;
}

/*
 * Returns 1 if its possible to transfer a file from the src to dst using the
 * BBAPI. In general (but not all cases) BBAPI can transfer between filesystems
 * if they both support extents.  EXT4 <-> gpfs is one exception.
 *
 * Returns 0 if BBAPI can not transfer from src to dst.
 */
int bbapi_copy_is_compatible(char* src, char* dst)
{
    /* List all filesystem types that are (somewhat) compatible with BBAPI */
    const __fsword_t whitelist[] = {
        XFS_SUPER_MAGIC,
        GPFS_SUPER_MAGIC,
        EXT4_SUPER_MAGIC,
    };

    __fsword_t source = axl_get_fs_magic(src);
    __fsword_t dest   = axl_get_fs_magic(dst);

    /* Exception: EXT4 <-> GPFS transfers are not supported by BBAPI */
    if ((source == EXT4_SUPER_MAGIC && dest == GPFS_SUPER_MAGIC) ||
        (source == GPFS_SUPER_MAGIC && dest == EXT4_SUPER_MAGIC))
    {
        return 0;
    }

    int found_source = 0;
    int found_dest   = 0;

    int i;
    for (i = 0; i < sizeof(whitelist) / sizeof(whitelist[0]); i++) {
        if (source == whitelist[i]) {
            found_source = 1;
        }

        if (dest == whitelist[i]) {
            found_dest = 1;
        }
    }

    if (found_source && found_dest) {
        return 1;
    }

    return 0;
}

static void getLastErrorDetails(BBERRORFORMAT pFormat, char** pBuffer)
{
    if (pBuffer) {
        size_t l_NumBytesAvailable;
        int rc = BB_GetLastErrorDetails(pFormat, &l_NumBytesAvailable, 0, NULL);
        if (rc == 0) {
            *pBuffer = (char*) malloc(l_NumBytesAvailable + 1);
            BB_GetLastErrorDetails(pFormat, NULL, l_NumBytesAvailable, *pBuffer);
        } else {
            *pBuffer = NULL;
        }
    }
}

/* Check and print BBAPI Error messages */
static int bb_check(int rc)
{
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
static int axl_get_unique_node_id(int* id)
{
    char hostname[256] = {0}; /* Max hostname + \0 */
    int rc = gethostname(hostname, sizeof(hostname));
    if (rc) {
        fprintf(stderr, "Hostname too long\n");
        return 1;
    }

    size_t len = strlen(hostname);

    rc = 1;

    /* Look from the back of the string to find the beginning of our number */
    int i;
    int sawnum = 0;
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
#endif

/* Called from AXL_Init */
int axl_async_init_bbapi(void)
{
#ifdef HAVE_BBAPI
    // TODO: BBAPI wants MPI rank information here?
    int rank;
    int rc = axl_get_unique_node_id(&rank);
    if (rc) {
        AXL_ERR("Couldn't get unique node id");
        return AXL_FAILURE;
    }

    rc = BB_InitLibrary(rank, BBAPI_CLIENTVERSIONSTR);
    return bb_check(rc);
#endif
    return AXL_FAILURE;
}

/* Called from AXL_Finalize */
int axl_async_finalize_bbapi(void)
{
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
    clock_gettime(CLOCK_MONOTONIC, &now);
    uint64_t timestamp = now.tv_sec;

    /* Get thread ID.  This is non-portable, Linux only. */
    pid_t tid = syscall(__NR_gettid);

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
int axl_async_create_bbapi(int id)
{
#ifdef HAVE_BBAPI
    /* allocate a new transfer definition */
    BBTransferDef_t* tdef;
    int rc = BB_CreateTransferDef(&tdef);
    if (rc) {
        /* If failed to create definition then return error. There is no cleanup necessary. */
        return bb_check(rc);
    }

    /* allocate a new transfer handle,
     * include AXL transfer id in IBM BB tag */
    BBTAG bbtag = axl_get_unique_tag();

    BBTransferHandle_t thandle;
    rc = BB_GetTransferHandle(bbtag, 1, NULL, &thandle);
    if (rc) {
        /* If transfer handle call failed then return. Do not record anything. */
        return bb_check(rc);
    }

    /* record transfer handle and definition */
    kvtree* file_list = axl_kvtrees[id];
    kvtree_util_set_unsigned_long(file_list, AXL_BBAPI_KEY_TRANSFERHANDLE, (unsigned long) thandle);
    kvtree_util_set_ptr(file_list, AXL_BBAPI_KEY_TRANSFERDEF, tdef);

    return bb_check(rc);
#endif
    return AXL_FAILURE;
}

/*
 * Return the BBTransferHandle_t (which is just a uint64_t) for a given AXL id.
 *
 * You can only call this function after axl_async_create_bbapi(id) has been
 * called.
 *
 * Returns 0 on success, 1 on error.  On success, thandle contains the transfer
 * handle value.
 */
int axl_async_get_bbapi_handle(int id, uint64_t* thandle)
{
#ifdef HAVE_BBAPI
    kvtree* file_list = axl_kvtrees[id];
    if (kvtree_util_get_unsigned_long(file_list, AXL_BBAPI_KEY_TRANSFERHANDLE,
        thandle) != KVTREE_SUCCESS)
    {
        return AXL_FAILURE;
    }

    return AXL_SUCCESS;
#endif
    return AXL_FAILURE;
}

/* Called from AXL_Add
 * Adds file source/destination to BBTransferDef */
int axl_async_add_bbapi (int id, const char* source, const char* dest)
{
#ifdef HAVE_BBAPI
    kvtree* file_list = axl_kvtrees[id];

    /* get transfer definition for this id */
    BBTransferDef_t* tdef;
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
int __axl_async_start_bbapi (int id, int resume) {
#ifdef HAVE_BBAPI
    int old_status;
    kvtree* file_list = axl_kvtrees[id];

    if (axl_bbapi_in_fallback(id)) {
        /*
         * We're in fallback mode because some of the paths we want to
         * transfer from/to are not compatible with the BBAPI transfers (like
         * if the underlying filesystem doesn't support extent).
         */
        if (resume) {
            axl_pthread_resume(id);
        } else {
            axl_pthread_start(id);
        }
    }

    if (resume) {
        kvtree_util_get_int(file_list, AXL_KEY_STATUS, &old_status);
        if (old_status == AXL_STATUS_INPROG) {
            /* Our transfers are already going.  Nothing to do */
            return AXL_SUCCESS;
        }
    }

    /* mark this transfer as in progress */
    kvtree_util_set_int(file_list, AXL_KEY_STATUS, AXL_STATUS_INPROG);

    /* Pull BB-Def and BB-Handle out of global var */
    BBTransferHandle_t thandle;
    kvtree_util_get_unsigned_long(file_list, AXL_BBAPI_KEY_TRANSFERHANDLE, (unsigned long*) &thandle);

    BBTransferDef_t* tdef;
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
    for (elem = kvtree_elem_first(files);
         elem != NULL;
         elem = kvtree_elem_next(elem))
    {
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

int axl_async_start_bbapi (int id) {
    return __axl_async_start_bbapi(id, 0);
}

int axl_async_resume_bbapi (int id) {
    return __axl_async_start_bbapi(id, 1);
}

int axl_async_test_bbapi (int id) {
#ifdef HAVE_BBAPI
    if (axl_bbapi_in_fallback(id)) {
        return axl_pthread_test(id);
    }
    kvtree* file_list = axl_kvtrees[id];

    /* Get the BB-Handle to query the status */
    BBTransferHandle_t thandle;
    kvtree_util_get_unsigned_long(file_list, AXL_BBAPI_KEY_TRANSFERHANDLE, (unsigned long*) &thandle);

    /* get info about transfer */
    BBTransferInfo_t tinfo;
    int rc = BB_GetTransferInfo(thandle, &tinfo);
    bb_check(rc);

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
    for (elem = kvtree_elem_first(files);
         elem != NULL;
         elem = kvtree_elem_next(elem))
    {
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
    kvtree* file_list = axl_kvtrees[id];

    if (axl_bbapi_in_fallback(id)) {
        return axl_pthread_wait(id);
    }

    /* Sleep until test changes set status */
    int status = AXL_STATUS_INPROG;
    while (status == AXL_STATUS_INPROG) {
        /* delegate work to test call to update status */
        axl_async_test_bbapi(id);

        /* if we're not done yet, sleep for some time and try again */
        kvtree_util_get_int(file_list, AXL_KEY_STATUS, &status);
        if (status == AXL_STATUS_INPROG) {
            usleep(100 * 1000);   /* 100ms */
        }
    }
    /* we're done now, either with error or success */
    if (status == AXL_STATUS_DEST) {
        /* Look though all our list of files */
        char* src;
        char* dst;
        kvtree_elem* elem = NULL;
        while ((elem = axl_get_next_path(id, elem, &src, &dst))) {
            AXL_DBG(2, "Read and copied %s to %s sucessfully", src, dst);
        }
        return AXL_SUCCESS;
    } else {
        return AXL_FAILURE;
    }
#endif
    return AXL_FAILURE;
}

int axl_async_cancel_bbapi (int id)
{
#ifdef HAVE_BBAPI
    if (axl_bbapi_in_fallback(id)) {
        return axl_pthread_cancel(id);
    }

    kvtree* file_list = axl_kvtrees[id];

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

/*
 * Return 1 if all paths for a given id's filelist in the kvtree are compatible
 * with the BBAPI (all source and destination paths are on filesystems that
 * support extents).  Return 0 if any of the source or destination paths do not
 * support extents.
 */
int axl_all_paths_are_bbapi_compatible(int id)
{
#ifdef HAVE_BBAPI
    /* Look though all our list of files */
    char* src;
    char* dst;
    kvtree_elem* elem = NULL;
    while ((elem = axl_get_next_path(id, elem, &src, &dst))) {
        if (! bbapi_copy_is_compatible(src, dst)) {
            /* This file copy isn't compatible with BBAPI */
            return 0;
        }
    }

    /* All files copies are BBAPI compatible */
    return 1;

#endif
    return 0;
}

/*
 * If the BBAPI is in fallback mode return 1, else return 0.
 *
 * Fallback mode happens when we can't transfer the files using the BBAPI due
 * to the source or destination nor supporting extents (which BBAPI requires).
 * If we're in fallback mode, we use a more compatible transfer method.
 *
 * Fallback mode is DISABLED by default.  You need to pass
 * -DENABLE_BBAPI_FALLBACK to cmake to enable it.
 */
int axl_bbapi_in_fallback(int id)
{
    int bbapi_fallback = 0;

#ifdef HAVE_BBAPI_FALLBACK
    kvtree* file_list = axl_kvtrees[id];
    if (kvtree_util_get_int(file_list, AXL_BBAPI_KEY_FALLBACK, &bbapi_fallback) !=
        KVTREE_SUCCESS)
    {
        /* Value isn't set, so we're not in fallback mode */
        return 0;
    }
#endif

    return bbapi_fallback;
}
