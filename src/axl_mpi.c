#include <stdlib.h>
#include <string.h>
#include <libgen.h>

#include "axl.h"
#include "axl_mpi.h"
#include "axl_internal.h"

#include "dtcmp.h"

#include "kvtree.h"
#include "kvtree_util.h"

#include "config.h"

#include "mpi.h"

static int axl_alltrue(int valid, MPI_Comm comm)
{
    int all_valid;
    MPI_Allreduce(&valid, &all_valid, 1, MPI_INT, MPI_LAND, comm);
    return all_valid;
}

/* caller really passes in a void**, but we define it as just void* to avoid printing
 * a bunch of warnings */
void axl_free2(void* p) {
    /* verify that we got a valid pointer to a pointer */
    if (p != NULL) {
        /* free memory if there is any */
        void* ptr = *(void**)p;
        if (ptr != NULL) {
            free(ptr);
        }

        /* set caller's pointer to NULL */
        *(void**)p = NULL;
    }
}

int AXL_Init_comm (
    MPI_Comm comm)    /**< [IN]  - communicator used for coordination and flow control */
{
    /* initialize AXL */
    int rc = AXL_Init();

    /* return same value on all ranks */
    if (! axl_alltrue(rc == AXL_SUCCESS, comm)) {
        /* someone failed, so everyone fails */

        /* if our call to init succeeded,
         * cal finalize to clean up */
        if (rc == AXL_SUCCESS) {
            AXL_Finalize();
        }

        /* return failure to everyone */
        return AXL_FAILURE;
    }

    return rc;
}

int AXL_Finalize_comm (
    MPI_Comm comm)    /**< [IN]  - communicator used for coordination and flow control */
{
    int rc = AXL_SUCCESS;

    int axl_rc = AXL_Finalize();
    if (axl_rc != AXL_SUCCESS) {
        rc = axl_rc;
    }

    /* return same value on all ranks */
    if (! axl_alltrue(rc == AXL_SUCCESS, comm)) {
        /* someone failed, so everyone fails */

        /* return failure to everyone */
        rc = AXL_FAILURE;
    }

    return rc;
}

int AXL_Create_comm (
    axl_xfer_t type,  /**< [IN]  - AXL transfer type (AXL_XFER_SYNC, AXL_XFER_PTHREAD, etc) */
    const char* name, 
    const char* file,
    MPI_Comm comm)    /**< [IN]  - communicator used for coordination and flow control */
{
    int id = AXL_Create(type, name, file);

    /* NOTE: We do not force id to be the same on all ranks.
     * It may be useful to do that, but then we need collective
     * allocation. */

    /* return same value on all ranks */
    if (! axl_alltrue(id != -1, comm)) {
      /* someone failed, so everyone fails */

      /* if this process succeeded in create,
       * free its handle to clean up */
      if (id != -1) {
          AXL_Free(id);
      }

      /* return -1 to everyone */
      id = -1;
    }

    return id;
}

int AXL_Add_comm (
  int id,           /**< [IN]  - transfer hander ID returned from AXL_Create */
  int num,          /**< [IN]  - number of files in src and dst file lists */
  const char** src, /**< [IN]  - list of source paths of length num */
  const char** dst, /**< [IN]  - list of destination paths of length num */
  MPI_Comm comm)    /**< [IN]  - communicator used for coordination and flow control */
{
    /* assume we'll succeed */
    int rc = AXL_SUCCESS;

    /* add files to transfer list */
    int i;
    for (i = 0; i < num; i++) {
      const char* src_file  = src[i];
      const char* dest_file = dst[i];
      if (AXL_Add(id, src_file, dest_file) != AXL_SUCCESS) {
        /* remember that we failed to add a file */
        rc = AXL_FAILURE;
      }
    }

    /* return same value on all ranks */
    if (! axl_alltrue(rc == AXL_SUCCESS, comm)) {
        /* someone failed, so everyone fails */
        rc = AXL_FAILURE;
    }

    return rc;
}

#if 0
/* True if calling rank is designated leader for file */
int scr_filemap_leader_rank(scr_filemap* map, const char *file)
{
    scr_meta* meta = scr_meta_new();
    int group_rank;

    scr_filemap_get_meta(map, file, meta);
    scr_meta_get_group_rank(meta, &group_rank);

    scr_meta_delete(&meta);

    return group_rank == 0;
}
#endif


static int axl_prep_shared_files(int id, MPI_Comm comm)
{
    int rc = AXL_SUCCESS;
    kvtree* file_hash = axl_kvtrees[id];
    kvtree* files = kvtree_get(file_hash, AXL_KEY_FILES);
    int numfiles = kvtree_size(files);

    char**    filelist    = (char**)    malloc(sizeof(char*)    * numfiles);
    uint64_t* group_id    = (uint64_t*) malloc(sizeof(uint64_t) * numfiles);
    uint64_t* group_ranks = (uint64_t*) malloc(sizeof(uint64_t) * numfiles);
    uint64_t* group_rank  = (uint64_t*) malloc(sizeof(uint64_t) * numfiles);

    int i = 0;
    kvtree_elem* elem;
    for (elem = kvtree_elem_first(files); elem != NULL; elem = kvtree_elem_next(elem)) {
        char* destination;
        kvtree_util_get_str(kvtree_elem_hash(elem), AXL_KEY_FILE_DEST, &destination);
        filelist[i] = strdup(destination);
        i++;
    }

    /* identify the set of unique files across all ranks */
    uint64_t groups;
    int dtcmp_rc = DTCMP_Rankv_strings(
        numfiles, (const char **) filelist, &groups, group_id, group_ranks, group_rank,
        DTCMP_FLAG_NONE, comm
    );
    if (dtcmp_rc == DTCMP_SUCCESS) {
        for (i = 0; i < numfiles; i++) {
            kvtree* f = kvtree_get(files, filelist[i]);
            kvtree_util_set_int(f, AXL_KEY_FILE_GROUP_RANK, group_rank[i]);
        }
    }
    else {
        rc = AXL_FAILURE;
    }

    if (rc == AXL_SUCCESS) {
        for (i = 0; i < numfiles; i++) {
            free(filelist[i]);
        }
    }

    free(filelist);
    free(group_id);
    free(group_ranks);
    free(group_rank);

    return rc;
}

int AXL_Dispatch_comm (
    int id,        /**< [IN]  - transfer hander ID returned from AXL_Create */
    MPI_Comm comm) /**< [IN]  - communicator used for coordination and flow control */
{
    /*
     * Before dispatching work to regular dispatch, post-process the list of destination
     * files to identify files that are shared across multiple ranks.
     */
    int rc = axl_prep_shared_files(id, comm);

    /* return same value on all ranks */
    if (! axl_alltrue(rc == AXL_SUCCESS, comm)) {
        return AXL_FAILURE;
    }

    /* delegate remaining work to regular dispatch */
    rc = AXL_Dispatch(id);

    /* return same value on all ranks */
    if (! axl_alltrue(rc == AXL_SUCCESS, comm)) {
        /* someone failed, so everyone fails */

        /* TODO: do we have another option than cancel/wait? */
        /* If dispatch succeeded on this process, cancel and wait.
         * This is ugly but necessary since the caller will free
         * the handle when we return, since we're telling the caller
         * that the collective dispatch failed.  The handle needs
         * to be in a state that can be freed. */
        if (rc == AXL_SUCCESS) {
            AXL_Cancel(id);
            AXL_Wait(id);

            /* TODO: should we also delete files,
             * since they may have already been transferred? */
        }

        /* return failure to everyone */
        rc = AXL_FAILURE;
    }

    return rc;
}

int AXL_Test_comm (
    int id,        /**< [IN]  - transfer hander ID returned from AXL_Create */
    MPI_Comm comm) /**< [IN]  - communicator used for coordination and flow control */
{
    int rc = AXL_Test(id);

    /* return same value on all ranks */
    if (! axl_alltrue(rc == AXL_SUCCESS, comm)) {
        /* someone failed, so everyone fails */
        rc = AXL_FAILURE;
    }

    return rc;
}

int AXL_Wait_comm (
    int id,        /**< [IN]  - transfer hander ID returned from AXL_Create */
    MPI_Comm comm) /**< [IN]  - communicator used for coordination and flow control */
{
    int rc = AXL_Wait(id);

    /* return same value on all ranks */
    if (! axl_alltrue(rc == AXL_SUCCESS, comm)) {
        /* someone failed, so everyone fails */
        rc = AXL_FAILURE;
    }

    return rc;
}

int AXL_Cancel_comm (
    int id,        /**< [IN]  - transfer hander ID returned from AXL_Create */
    MPI_Comm comm) /**< [IN]  - communicator used for coordination and flow control */
{
    int rc = AXL_Cancel(id);

    /* return same value on all ranks */
    if (! axl_alltrue(rc == AXL_SUCCESS, comm)) {
        /* someone failed, so everyone fails */
        rc = AXL_FAILURE;
    }

    return rc;
}

int AXL_Free_comm (
    int id,        /**< [IN]  - transfer hander ID returned from AXL_Create */
    MPI_Comm comm) /**< [IN]  - communicator used for coordination and flow control */
{
    int rc = AXL_Free(id);

    /* return same value on all ranks */
    if (! axl_alltrue(rc == AXL_SUCCESS, comm)) {
        /* someone failed, so everyone fails */
        rc = AXL_FAILURE;
    }

    return rc;
}

int AXL_Stop_comm (
    MPI_Comm comm) /**< [IN]  - communicator used for coordination and flow control */
{
    int rc = AXL_Stop();

    /* return same value on all ranks */
    if (! axl_alltrue(rc == AXL_SUCCESS, comm)) {
        /* someone failed, so everyone fails */
        rc = AXL_FAILURE;
    }

    return rc;
}
