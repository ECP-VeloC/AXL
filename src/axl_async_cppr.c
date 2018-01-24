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

/* All rights reserved. This program and the accompanying materials
* are made available under the terms of the BSD-3 license which accompanies this
* distribution in LICENSE.TXT
*
* This library is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the BSD-3  License in
* LICENSE.TXT for more details.
*
* GOVERNMENT LICENSE RIGHTS-OPEN SOURCE SOFTWARE
* The Government's rights to use, modify, reproduce, release, perform,
* display, or disclose this software are subject to the terms of the BSD-3
* License as provided in Contract No. B609815.
* Any reproduction of computer software, computer software documentation, or
* portions thereof marked with this legend must also reproduce the markings.
*
* Author: Christopher Holguin <christopher.a.holguin@intel.com>
*
* (C) Copyright 2015-2016 Intel Corporation.
*/

#ifdef HAVE_LIBCPPR
#include "cppr.h"
#include "axl_async_cppr.h"

static void __free_axl_cppr_info(struct axl_cppr_info *metadata_ptr, struct cppr_op_info *handles_ptr, const int length) {
  struct axl_cppr_info* tmp_ptr = NULL;
  int i;
  if (metadata_ptr != NULL) {
    for (i = 0; i < length; i++) {
      tmp_ptr = &metadata_ptr[i];

      if (tmp_ptr->alloced == true) {
        free(tmp_ptr->src_dir);
        free(tmp_ptr->dst_dir);
        free(tmp_ptr->filename);
      }
    }

    free(metadata_ptr);
    metadata_ptr = NULL;
  }

  if (handles_ptr != NULL) {
    free(handles_ptr);
    handles_ptr = NULL;
  }
}
#endif

/*
=========================================
CPPR Asynchronous flush wrapper functions
========================================
*/

/* check whether the flush from cache to parallel file system has completed */
static int axl_flush_async_test_cppr(int id) {
#ifdef HAVE_LIBCPPR
    /* CPPR: essentially test the grp
     * cppr_return_t cppr_test_all(uint32_t count, struct cppr_op_info info[]);
     * each rank 0 on a node needs to call test_all, then report that in
     * transfer_complete, then call scr_alltrue(transfer_complete)
     * make sure transfer_complete is set to 1 in all non 0 ranks
     */

    /* assume the transfer is complete */
    int transfer_complete = 1;

    /* current operation for easier access */
    struct axl_cppr_info* current_cppr_metadata = NULL;

    /* current handle for easier access */
    struct cppr_op_info* current_cppr_handle = NULL;

    cppr_return_t cppr_retval;

    /* have master on each node check whether the flush is complete */

    // TODO: FIX (was previously scr store desc) !!!!!
    int rank = 0;

    double bytes_written = 0.0;
    if (rank == 0) {
        cppr_retval = cppr_test_all(scr_flush_async_cppr_index + 1, cppr_ops);

        /* if this fails, treat it as just an incomplete transfer for now */
        if (cppr_retval != CPPR_SUCCESS) {
            axl_dbg(0, "CPPR ERROR WITH initial call to cppr_test(): %d",
                    cppr_retval
                    );
            transfer_complete = 0;
            goto mpi_collectives;
        }

        /* loop through all the responses and check status */
        int i;
        for (i = 0; i < scr_flush_async_cppr_index; i++) {
            axl_dbg(1, "cppr async test being called for index %d", i);

            /* set the current pointer to the correct index */
            current_cppr_metadata = &axl_flush_async_cppr_metadata[i];
            current_cppr_handle = &cppr_ops[i];

            if (current_cppr_metadata->has_completed == true) {
                /* can skip this handle because it was already marked complete */
                axl_dbg(1, "handle [%d] %x %s was marked complete", i,
                        current_cppr_handle->handle,
                        current_cppr_metadata->filename
                        );
                continue;
            } else {
                /* check the state of the handle */
                /* check the status */
                if (current_cppr_handle->status == CPPR_STATUS_COMPLETE) {
                    /* mark as complete */
                    current_cppr_metadata->has_completed = true;
                    axl_dbg(1, "cppr op status is COMPLETE, so setting transfer complete to \
1: handle %d, file '%s' @ %s:%d",
                            i, current_cppr_metadata->filename, __FILE__, __LINE__
                            );

                    /* check for bad values: */
                    if (current_cppr_handle->retcode != CPPR_SUCCESS) {
                        axl_dbg(1, "CPPR cppr_test unsuccessful async flush for '%s' %d",
                                current_cppr_metadata->filename, current_cppr_handle->retcode
                                );
                    } else {
                        /* the file was transferred successfully */
                        bytes_written += current_cppr_metadata->filesize;
                        axl_dbg(2, "#bold CPPR successfully transfered file '%s' in async mode",
                                current_cppr_metadata->filename
                                );
                    }
                } else if (current_cppr_handle->retcode == CPPR_OP_EXECUTING) {
                    /* if the operation is still executing, handle accordingly */
                    /* calculate bytes written */
                    double percent_written = (double)
                        (current_cppr_handle->progress) / 100;

                    /* bytes_written += percent_written * current_cppr_metadata->filesize; */
                    transfer_complete = 0;
                    axl_dbg(1,"cppr op status is EXECUTING for file '%s'; percent: \
(int %d, double %f), bytes written %f @ %s:%d",
                            current_cppr_metadata->filename, current_cppr_handle->progress,
                            percent_written, bytes_written,
                            __FILE__, __LINE__
                            );
                } else {
                    axl_dbg(0,"CPPR ERROR UNHANDLED: cppr_test: unhandled values for \
src:'%s', dst:'%s', file:'%s' status %d and retcode %d; handle:[%d]: %x",
                            current_cppr_metadata->src_dir,
                            current_cppr_metadata->dst_dir,
                            current_cppr_metadata->filename,
                            current_cppr_handle->status,
                            current_cppr_handle->retcode,
                            i,
                            current_cppr_handle->handle
                            );
                }
            }
        }
    }

mpi_collectives:
    /* determine whether the transfer is complete on all tasks */
    if (transfer_complete) {
        axl_dbg(0, "#demo CPPR successfully transferred dset %d", id);
        return AXL_SUCCESS;
    }
    axl_dbg(1, "about to return failure from axl_cppr_flush_async_test @ %s:%d",
            __FILE__, __LINE__
            );
#endif
    return AXL_FAILURE;
}

/* complete the flush from cache to parallel file system */
static int axl_flush_async_complete_cppr(int id) {
#ifdef HAVE_LIBCPPR
    int flushed = SCR_SUCCESS;

    axl_dbg(0,"axl_cppr_flush_async_complete called @ %s:%d",
            __FILE__, __LINE__
            );


    /* allocate structure to hold metadata info */
    kvtree* data = kvtree_new();

    /* fill in metadata info for the files this process flushed */
    kvtree* files = kvtree_get(scr_flush_async_file_list, SCR_KEY_FILE);
    kvtree_elem* elem = NULL;
    for (elem = kvtree_elem_first(files);
         elem != NULL;
         elem = kvtree_elem_next(elem))
        {
            /* get the filename */
            char* file = kvtree_elem_key(elem);

            /* get the hash for this file */
            kvtree* hash = kvtree_elem_hash(elem);

            /* record the filename in the hash, and get reference to a hash for
             * this file */
            scr_path* path_file = scr_path_from_str(file);
            scr_path_basename(path_file);

            char* name = scr_path_strdup(path_file);
            kvtree* file_hash = kvtree_set_kv(data, SCR_SUMMARY_6_KEY_FILE, name);

            scr_free(&name);
            scr_path_delete(&path_file);

            /* get meta data for this file */
            scr_meta* meta = kvtree_get(hash, SCR_KEY_META);

            /* successfully flushed this file, record the filesize */
            unsigned long filesize = 0;
            if (scr_meta_get_filesize(meta, &filesize) == SCR_SUCCESS) {
                kvtree_util_set_bytecount(file_hash, SCR_SUMMARY_6_KEY_SIZE, filesize);
            }
            scr_dbg(1, "filesize is %d @ %s:%d", filesize, __FILE__, __LINE__);

            /* record the crc32 if one was computed */
            uLong flush_crc32;
            if (scr_meta_get_crc32(meta, &flush_crc32) == SCR_SUCCESS) {
                kvtree_util_set_crc32(file_hash, SCR_SUMMARY_6_KEY_CRC, flush_crc32);
            }
        }

    /* write summary file */
    if (scr_flush_complete(id, scr_flush_async_file_list, data) != SCR_SUCCESS) {
        scr_dbg(1, "axl_cppr_flush_async_complete is @ %s:%d", __FILE__, __LINE__);
        flushed = SCR_FAILURE;
    }

    /* have master on each node cleanup the list of CPPR handles and files */
    if (scr_storedesc_cntl->rank == 0) {
        __free_axl_cppr_info(scr_flush_async_cppr_metadata,
                             cppr_ops,
                             scr_flush_async_cppr_index
                             );
        scr_dbg(1, "axl_cppr_flush_async_complete is @ %s:%d",
                __FILE__, __LINE__
                );
        scr_flush_async_cppr_index = 0;
        axl_cppr_currently_alloced = 0;
        scr_flush_async_cppr_metadata = NULL;
        cppr_ops = NULL;
    }

    /* mark that we've stopped the flush */
    scr_flush_async_in_progress = 0;
    scr_flush_file_location_unset(id, SCR_FLUSH_KEY_LOCATION_FLUSHING);

    /* free data structures */
    kvtree_delete(&data);

    /* free the file list for this checkpoint */
    kvtree_delete(&scr_flush_async_hash);
    kvtree_delete(&scr_flush_async_file_list);
    scr_flush_async_hash      = NULL;
    scr_flush_async_file_list = NULL;

    /* stop timer, compute bandwidth, and report performance */
    if (scr_my_rank_world == 0) {
        double time_end = MPI_Wtime();
        double time_diff = time_end - scr_flush_async_time_start;
        double bw = scr_flush_async_bytes / (1024.0 * 1024.0 * time_diff);
        scr_dbg(0, "scr_flush_async_complete: %f secs, %e bytes, %f MB/s, \
%f MB/s per proc",
                time_diff,
                scr_flush_async_bytes,
                bw,
                bw/scr_ranks_world
                );

        /* log messages about flush */
        if (flushed == SCR_SUCCESS) {
            /* the flush worked, print a debug message */
            scr_dbg(1, "scr_flush_async_complete: Flush of dataset %d succeeded", id);

            /* log details of flush */
            if (scr_log_enable) {
                time_t now = scr_log_seconds();
                scr_log_event("ASYNC FLUSH SUCCEEDED WITH CPPR",
                              NULL,
                              &id,
                              &now,
                              &time_diff
                              );
            }
        } else {
            /* the flush failed, this is more serious so print an error message */
            scr_err("-----------FAILED:scr_flush_async_complete: Flush failed @ %s:%d",
                    __FILE__, __LINE__
                    );
            scr_dbg(1, "axl_cppr_flush_async_complete is at FAILURE @ %s:%d",
                    __FILE__, __LINE__
                    );

            /* log details of flush */
            if (scr_log_enable) {
                time_t now = scr_log_seconds();
                scr_log_event("ASYNC FLUSH FAILED", NULL, &id, &now, &time_diff);
            }
        }
    }

    return flushed;
#endif
    return AXL_FAILURE;
}


/* stop all ongoing asynchronous flush operations */
static int axl_flush_async_stop_cppr(void)
{
  /* if user has disabled flush, return failure */
  if (scr_flush <= 0) {
    return SCR_FAILURE;
  }

  /* this may take a while, so tell user what we're doing */
  if (scr_my_rank_world == 0) {
    scr_dbg(1, "axl_cppr_flush_async_stop: Stopping flush");
  }

  /* wait until all tasks know the transfer is stopped */
  axl_flush_async_wait(NULL);

  /* cleanup CPPR handles */
  if (scr_storedesc_cntl->rank == 0) {
          __free_axl_cppr_info(scr_flush_async_cppr_metadata,
                               cppr_ops,
                               scr_flush_async_cppr_index
          );

          scr_flush_async_cppr_index = 0;
          axl_cppr_currently_alloced = 0;
          scr_flush_async_cppr_metadata = NULL;
          cppr_ops = NULL;
  }

  /* set global status to 0 */
  scr_flush_async_in_progress = 0;

  /* clear internal flush_async variables to indicate there is no flush */
  if (scr_flush_async_hash != NULL) {
    kvtree_delete(&scr_flush_async_hash);
  }
  if (scr_flush_async_file_list != NULL) {
    kvtree_delete(&scr_flush_async_file_list);
  }

  /* make sure all processes have made it this far before we leave */
  MPI_Barrier(scr_comm_world);
  return SCR_SUCCESS;
}

/* start an asynchronous flush from cache to parallel file system
 * under SCR_PREFIX */
int axl_flush_async_start_cppr(int id) {
#ifdef HAVE_LIBCPPR
    /* todo: consider using CPPR grp API when available */
    kvtree* file_list = kvtree_get_kv_int(axl_flush_async_file_lists, AXL_HANDLE_UID, id);
    kvtree_util_set_int(file_list, AXL_KEY_FLUSH_STATUS, AXL_FLUSH_STATUS_INPROG);

    /* allocate the cppr hash (free them when shutting down)
     * first check to ensure it is NULL.  if it is not NULL, this may be the
     * case if an asyncflush failed. need to free it first if not NULL  */
    if (axl_flush_async_cppr_metadata != NULL) {
        axl_dbg(3, "#bold WHY FREE METADATA AGAIN?? ");
        __free_axl_cppr_info(axl_flush_async_cppr_metadata,
                             cppr_ops,
                             axl_flush_async_cppr_index
                             );
    }

    axl_dbg(3, "#bold about to calloc @ %s:%d", __FILE__, __LINE__);
    axl_flush_async_cppr_metadata = calloc(axl_flush_async_cppr_alloc, sizeof(struct axl_cppr_info));
    axl_dbg(3, "#bold after calloc @ %s:%d", __FILE__, __LINE__);

    if (axl_flush_async_cppr_metadata == NULL) {
        axl_dbg(1,"couldn't allocate enough memory for cppr operation metadata");
        return AXL_FAILURE;
    }

    cppr_ops = calloc(axl_flush_async_cppr_alloc, sizeof(struct cppr_op_info));

    if (cppr_ops == NULL) {
        axl_dbg(1,"couldn't allocate enough memory for cppr operation handles");
        return AXL_FAILURE;
    }

    /* update the currently alloced size */
    axl_cppr_currently_alloced++;

    /*  CPPR just needs to iterate through this combined hash */
    /* call cppr_mv and save the handles */
    kvtree_elem* elem;
    kvtree* files = kvtree_get(file_list, AXL_KEY_FILE);
    for (elem = kvtree_elem_first(files); elem != NULL; elem = kvtree_elem_next(elem)) {
        /* get the filename */
        char* file = kvtree_elem_key(elem);

        /* get the hash for this file */
        kvtree* file_hash = kvtree_elem_hash(elem);

        /* get directory to flush file to */
        char* dest_dir;
        if (kvtree_util_get_str(file_hash, AXL_KEY_PATH, &dest_dir) != AXL_SUCCESS) {
            continue;
        }

        /* get meta data for file */
        scr_meta* meta = kvtree_get(file_hash, SCR_KEY_META);

        /* get just the file name */
        char *plain_filename = NULL;
        if (scr_meta_get_filename(meta, &plain_filename) != SCR_SUCCESS) {
            scr_dbg(0,"couldn't get the file name from meta '%s'", file);
            continue;
        }

        /* get the file size */
        unsigned long filesize = 0;
        if (scr_meta_get_filesize(meta, &filesize) != SCR_SUCCESS) {
            continue;
        }

        /* get the information for this file */
        kvtree* transfer_file_hash = kvtree_get_kv(scr_flush_async_hash, SCR_TRANSFER_KEY_FILES, file);
        if (transfer_file_hash != NULL) {
            /* break file into path and name components */
            scr_path* path_dest_file = scr_path_from_str(file);

            /* get full path */
            char *full_path = scr_path_strdup(path_dest_file);

            /* get only the src dir */
            scr_path_dirname(path_dest_file);
            char *only_path_dest = scr_path_strdup(path_dest_file);

            /* get only the file name */
            scr_path_basename(path_dest_file);
            char *basename_path = scr_path_strdup(path_dest_file);

            /* add dest dir to the file name */
            scr_path_prepend_str(path_dest_file, dest_dir);

            char* dest_file = scr_path_strdup(path_dest_file);

            if (scr_path_dirname(path_dest_file) != SCR_SUCCESS) {
                return SCR_FAILURE;
            }

            if (full_path == NULL || basename_path == NULL || dest_file == NULL) {
                scr_dbg(1,"CPPR error allocating for the file paths");
                return SCR_FAILURE;
            }
            scr_dbg(2,"CPPR async dest file paths:'%s', base:'%s', dest:'%s' lone \
filename: '%s' src path? '%s'", full_path,
                    basename_path,
                    dest_file,
                    plain_filename,
                    only_path_dest
                    );

            if ((scr_flush_async_cppr_index+1) >= (axl_cppr_currently_alloced * scr_flush_async_cppr_alloc)) {
                    scr_dbg(1, "CPPR reallocating the CPPR handles array @ %s:%d",
                            __FILE__, __LINE__
                            );
                    int bytes_currently_used = 0;

                    /* increment the counter to indicate another alloc has happened */
                    axl_cppr_currently_alloced++;

                    /* reallocate the cppr metadata array */
                    int new_size_to_alloc = sizeof(struct axl_cppr_info) *
                        scr_flush_async_cppr_alloc *
                        axl_cppr_currently_alloced;
                    void *new_ptr = realloc((void *)scr_flush_async_cppr_metadata,
                                            new_size_to_alloc);
                    if (new_ptr == NULL) {
                        scr_dbg(1, "not enough mem for CPPR metadata @ %s:%d",
                                __FILE__, __LINE__
                                );
                        axl_cppr_currently_alloced--;
                        return SCR_FAILURE;
                    }

                    /* update the pointer */
                    scr_flush_async_cppr_metadata = (struct axl_cppr_info *)new_ptr;

                    /* clear out only the newly allocated space */
                    bytes_currently_used = (scr_flush_async_cppr_index+1) * sizeof(struct axl_cppr_info);
                    memset((void *) scr_flush_async_cppr_metadata + bytes_currently_used, 0x00, new_size_to_alloc - bytes_currently_used );

                    /* realloc the cppr handles array */
                    new_size_to_alloc = sizeof(struct cppr_op_info) *
                        scr_flush_async_cppr_alloc *
                        axl_cppr_currently_alloced;

                    /* clear the new_ptr before reusing it */
                    new_ptr = NULL;

                    new_ptr = realloc((void *) cppr_ops, new_size_to_alloc);
                    if (new_ptr == NULL) {
                        scr_dbg(1, "not enough mem for CPPR handles @ %s:%d",
                                __FILE__, __LINE__
                                );
                        axl_cppr_currently_alloced--;
                        return SCR_FAILURE;
                    }
                    /* update the pointer */
                    cppr_ops = (struct cppr_op_info *) new_ptr;

                    /* clear out the newly allocated space */
                    bytes_currently_used = (scr_flush_async_cppr_index+1)*
                        sizeof(struct cppr_op_info);
                    memset((void *) cppr_ops + bytes_currently_used,
                           0x00,
                           new_size_to_alloc - bytes_currently_used);

                    scr_dbg(1, "CPPR reallocate done @ %s:%d", __FILE__, __LINE__);
            }

            scr_dbg(1, "executing cppr_mv for %s", plain_filename);
            scr_flush_async_cppr_metadata[scr_flush_async_cppr_index].src_dir = strdup(only_path_dest);
            scr_flush_async_cppr_metadata[scr_flush_async_cppr_index].dst_dir = strdup(dest_dir);
            scr_flush_async_cppr_metadata[scr_flush_async_cppr_index].filename = strdup(plain_filename);
            scr_flush_async_cppr_metadata[scr_flush_async_cppr_index].filesize = filesize;
            scr_flush_async_cppr_metadata[scr_flush_async_cppr_index].alloced = true;

            if (cppr_mv(&(cppr_ops[scr_flush_async_cppr_index]),
                        NULL,
                        CPPR_FLAG_TRACK_PROGRESS,
                        NULL,
                        dest_dir,
                        only_path_dest,
                        plain_filename) != CPPR_SUCCESS) {
                scr_dbg(0, "----CPPR FAILED KICKING OFF MV FOR: %s", basename_path);
                return SCR_FAILURE;
            }
            scr_dbg(1, "cppr handle array position %d is value %x",
                    scr_flush_async_cppr_index,
                    cppr_ops[scr_flush_async_cppr_index].handle
                    );

            /* critical to update the handle counter */
            scr_flush_async_cppr_index++;

            /* delete path and string for the file name */
            scr_free(&dest_file);
            scr_free(&full_path);
            scr_free(&basename_path);
            scr_path_delete(&path_dest_file);
        } else {
            /* confirmed the bug?? */
            scr_dbg(0,"ERROR NEED TO CHECK THIS why was this value null BUG confirmed?: %s", file);
        }
    }

    /* delete the hash */
    kvtree_delete(&hash);
} else {
    /* send our transfer hash data to the master on this node */
    kvtree_send(scr_flush_async_hash, 0, scr_storedesc_cntl->comm);
  }

  /* get the total number of bytes to write */
  scr_flush_async_bytes = 0.0;
  MPI_Allreduce(&my_bytes, &scr_flush_async_bytes, 1, MPI_DOUBLE, MPI_SUM, scr_comm_world);

  /* make sure all processes have started before we leave */
  MPI_Barrier(scr_comm_world);

  return SCR_SUCCESS;
}

/*
=========================================
END: CPPR Asynchronous flush wrapper functions
========================================
*/

static int axl_init_cppr() {
#ifdef HAVE_LIBCPPR
    /* attempt to init cppr */
    int cppr_ret = cppr_status();
    if (cppr_ret != CPPR_SUCCESS) {
        axl_abort(-1, "libcppr cppr_status() failed: %d '%s' @ %s:%d",
                  cppr_ret, cppr_err_to_str(cppr_ret), __FILE__, __LINE__
                  );
    }
    axl_dbg(1, "#bold CPPR is present @ %s:%d", __FILE__, __LINE__);
    return AXL_SUCCESS;
#endif /* HAVE_LIBCPPR */
    return AXL_FAILURE;
}
