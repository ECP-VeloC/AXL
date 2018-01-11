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

#include "kvtree.h"
#include "scr_globals.h"
#include "axl_internals.h"

#ifdef HAVE_LIBCPPR
#include "axl_cppr.h"
#endif

#ifdef HAVE_DATAWARP
#include "axl_async_datawarp.h"
#endif

#ifdef HAVE_BBABI
#include "bbapi.h"
#include <stdlib.h>
#endif


/*
=========================================
TODO: Things to clean up
========================================
*/

#define AXL_SUCCESS 0
#define AXL_FAILURE 1

#ifdef HAVE_MPI
#include <mpi.h>
#endif

/*
=========================================
Global Variables
========================================
*/

/* transfer file */
char* axl_transfer_file = NULL;

/* AXL's flush file, SCR has one as well */
char* axl_flush_file = NULL;

/* tracks whether an async flush is currently underway */
int axl_flush_async_in_progress = 0;

/* tracks the id of the checkpoint being flushed */
int axl_flush_async_dataset_id = -1;

/* records the time the async flush started */
static time_t axl_flush_async_timestamp_start;

/* records the time the async flush started */
static double axl_flush_async_time_start;

/* tracks list of lists of files written with flush */
static kvtree* axl_flush_async_file_lists = kvtree_new();

/* tracks list of files written with flush */
static kvtree* axl_flush_async_hash = NULL;

/* records the number of files this process must flush */
static int axl_flush_async_num_files = 0;

/*
=========================================
Asynchronous flush helper functions
========================================
*/

double axl_get_time() {
    double t;
#ifdef HAVE_MPI
    t = MPI_Wtime() * 1e9;
#else
    time_t timer;
    time(&timer);
    t = (double) timer;
#endif
    return t;
}

/* dequeues files listed in hash2 from hash1 */
static int axl_flush_async_file_dequeue(kvtree* hash1, kvtree* hash2)
{
  /* for each file listed in hash2, remove it from hash1 */
  kvtree* file_hash = kvtree_get(hash2, AXL_TRANSFER_KEY_FILES);
  if (file_hash != NULL) {
    kvtree_elem* elem;
    for (elem = kvtree_elem_first(file_hash);
         elem != NULL;
         elem = kvtree_elem_next(elem))
    {
      /* get the filename, and dequeue it */
      char* file = kvtree_elem_key(elem);
      kvtree_unset_kv(hash1, AXL_TRANSFER_KEY_FILES, file);
    }
  }
  return AXL_SUCCESS;
}

/*
=========================================
Asynchronous flush functions
=========================================
*/

/* given a hash, test whether the files in that hash have completed their flush */
static int axl_flush_async_file_test(const kvtree* hash, double* bytes)
{
  /* initialize bytes to 0 */
  *bytes = 0.0;

  /* get the FILES hash */
  kvtree* files_hash = kvtree_get(hash, AXL_TRANSFER_KEY_FILES);
  if (files_hash == NULL) {
    /* can't tell whether this flush has completed */
    return AXL_FAILURE;
  }

  /* assume we're done, look for a file that says we're not */
  int transfer_complete = 1;

  /* for each file, check whether the WRITTEN field matches the SIZE field,
   * which indicates the file has completed its transfer */
  kvtree_elem* elem;
  for (elem = kvtree_elem_first(files_hash);
       elem != NULL;
       elem = kvtree_elem_next(elem))
  {
    /* get the hash for this file */
    kvtree* file_hash = kvtree_elem_hash(elem);
    if (file_hash == NULL) {
      transfer_complete = 0;
      continue;
    }

    /* lookup the values for the size and bytes written */
    unsigned long size, written;
    if (kvtree_util_get_bytecount(file_hash, "SIZE",    &size)    != AXL_SUCCESS ||
        kvtree_util_get_bytecount(file_hash, "WRITTEN", &written) != AXL_SUCCESS)
    {
      transfer_complete = 0;
      continue;
    }

    /* check whether the number of bytes written is less than the filesize */
    if (written < size) {
      transfer_complete = 0;
    }

    /* add up number of bytes written */
    *bytes += (double) written;
  }

  /* return our decision */
  if (transfer_complete) {
    return AXL_SUCCESS;
  }
  return AXL_FAILURE;
}

/* writes the specified command to the transfer file */
static int axl_flush_async_command_set(char* command)
{
  /* have the master on each node write this command to the file */
  // TODO: Clean up paralleism
  //if (scr_storedesc_cntl->rank == 0) {
    /* get a hash to store file data */
    kvtree* hash = kvtree_new();

    /* read the file */
    int fd = -1;
    kvtree_lock_open_read(axl_transfer_file, &fd, hash);

    /* set the command */
    kvtree_util_set_str(hash, AXL_TRANSFER_KEY_COMMAND, command);

    /* write the hash back */
    kvtree_write_close_unlock(axl_transfer_file, &fd, hash);

    /* delete the hash */
    kvtree_delete(&hash);
  //}
  return AXL_SUCCESS;
}

/* waits until all transfer processes are in the specified state */
static int axl_flush_async_state_wait(char* state)
{
  /* wait until each process matches the state */
  int all_valid = 0;
  while (! all_valid) {
    /* assume we match the specified state */
    int valid = 1;

    /* have the master on each node check the state in the transfer file */
    // TODO: Clean up paralleism
    //if (scr_storedesc_cntl->rank == 0) {
      /* get a hash to store file data */
      kvtree* hash = kvtree_new();

      /* open transfer file with lock */
      kvtree_read_with_lock(axl_transfer_file, hash);

      /* check for the specified state */
      kvtree* state_hash = kvtree_get_kv(hash,
                                             AXL_TRANSFER_KEY_STATE,
                                             state);
      if (state_hash == NULL) {
        valid = 0;
      }

      /* delete the hash */
      kvtree_delete(&hash);
    //}

    /* check whether everyone is at the specified state */
    // TODO: Clean up paralleism
    //if (scr_alltrue(valid)) {
    //  all_valid = 1;
    //}
      if (valid) {
          all_valid = 1;
      }

    /* if we're not there yet, sleep for sometime and they try again */
    if (! all_valid) {
      usleep(10*1000*1000);
    }
  }
  return AXL_SUCCESS;
}

/* removes all files from the transfer file */
static int axl_flush_async_file_clear_all()
{
  /* have the master on each node clear the FILES field */
  // TODO: Clean up paralleism
  //if (scr_storedesc_cntl->rank == 0) {
    /* get a hash to store file data */
    kvtree* hash = kvtree_new();

    /* read the file */
    int fd = -1;
    kvtree_lock_open_read(axl_transfer_file, &fd, hash);

    /* clear the FILES entry */
    kvtree_unset(hash, AXL_TRANSFER_KEY_FILES);

    /* write the hash back */
    kvtree_write_close_unlock(axl_transfer_file, &fd, hash);

    /* delete the hash */
    kvtree_delete(&hash);
  //}
  return AXL_SUCCESS;
}

/* stop all ongoing asynchronous flush operations */
// TODO: Caller must check value of scr_flush
int axl_flush_async_stop(int id, axl_xfer_t TYPE)
{
  /* cppr: just call wait_all, once complete, set state, notify everyone */
  /*  cppr_return_t cppr_wait_all(uint32_t count,
   *                       struct cppr_op_info info[],
   *                    uint32_t timeoutMS) */
#ifdef HAVE_LIBCPPR
        return axl_flush_async_stop_cppr();
#endif

  /* this may take a while, so tell user what we're doing */
  axl_dbg(1, "axl_flush_async_stop_all: Stopping flush");

  /* write stop command to transfer file */
  axl_flush_async_command_set(AXL_TRANSFER_KEY_COMMAND_STOP);

  /* wait until all tasks know the transfer is stopped */
  axl_flush_async_state_wait(AXL_TRANSFER_KEY_STATE_STOP);

  /* remove the files list from the transfer file */
  axl_flush_async_file_clear_all();

  /* remove FLUSHING state from flush file */
  axl_flush_async_in_progress = 0;
  /*
  scr_flush_file_location_unset(id, AXL_FLUSH_KEY_LOCATION_FLUSHING);
  */

  /* clear internal flush_async variables to indicate there is no flush */
  if (axl_flush_async_hash != NULL) {
    kvtree_delete(&axl_flush_async_hash);
  }

  kvtree* file_list = kvtree_getf(axl_flush_async_file_lists, "%s %d", AXL_HANDLE_UID, id);
  if (file_list != NULL) {
    kvtree_delete(&file_list);
  }

  return AXL_SUCCESS;
  // TODO: Caller may need to use MPI_Barrier
}

/* start an asynchronous flush from cache to parallel file
 * system under SCR_PREFIX */
// TODO: Caller must check value of scr_flush
int axl_flush_async_start(fu_filemap* map, int id, axl_xfer_t TYPE)

{
#ifdef HAVE_LIBCPPR
  return axl_flush_async_start_cppr(map, id) ;
#endif

  // TODO: Caller may need to do some of this error checking
  // TODO: Caller may need to call MPI_Barrier

  /* if we don't need a flush, return right away with success */
  if (! axl_flush_file_need_flush(id)) {
    return AXL_SUCCESS;
  }

  /* this may take a while, so tell user what we're doing */
  axl_dbg(1, "axl_flush_async_start: Initiating flush of dataset %d", id);

  /* start timer */
  //if (scr_my_rank_world == 0) {
    //scr_flush_async_timestamp_start = scr_log_seconds();
    axl_flush_async_time_start = axl_get_time();

    /* log the start of the flush */
  //}

  /* mark that we've started a flush */
  axl_flush_async_in_progress = 1;
  axl_flush_async_dataset_id = id;
  axl_flush_file_location_set(id, AXL_FLUSH_KEY_LOCATION_FLUSHING);

  /* get list of files to flush and create directories */
  kvtree* file_list = kvtree_getf(axl_flush_async_file_lists, "%s %d", AXL_HANDLE_UID, id);
//  file_list = kvtree_new();
  if (scr_flush_prepare(map, id, file_list) != AXL_SUCCESS) {
      axl_err("axl_flush_async_start: Failed to prepare flush @ %s:%d",
        __FILE__, __LINE__
      );
    kvtree_delete(&file_list);
    file_list = NULL;
    return AXL_FAILURE;
  }

  /* add each of my files to the transfer file list */
  axl_flush_async_hash = kvtree_new();
  axl_flush_async_num_files = 0;
  double my_bytes = 0.0;
  kvtree_elem* elem;
  kvtree* files = kvtree_get(file_list, AXL_KEY_FILE);
  for (elem = kvtree_elem_first(files);
       elem != NULL;
       elem = kvtree_elem_next(elem))
  {
     /* get the filename */
     char* file = kvtree_elem_key(elem);

     /* get the hash for this file */
     kvtree* file_hash = kvtree_elem_hash(elem);

     /* get directory to flush file to */
     char* dest_dir;
     if (kvtree_util_get_str(file_hash, AXL_KEY_PATH, &dest_dir) !=
         AXL_SUCCESS) {
       continue;
     }

     /* get meta data for file */
     scr_meta* meta = kvtree_get(file_hash, AXL_KEY_META);

     /* get the file size */
     unsigned long filesize = 0;
     if (scr_meta_get_filesize(meta, &filesize) != AXL_SUCCESS) {
       continue;
     }
     my_bytes += (double) filesize;

     /* add this file to the hash, and add its filesize
      * to the number of bytes written */
     kvtree* transfer_file_hash = kvtree_set_kv(axl_flush_async_hash,
                                                    AXL_TRANSFER_KEY_FILES,
                                                    file);
     /* TODO BUG FIX HERE: file_hash should be transfer_file_hash?? */
     if (file_hash != NULL) {
       /* break file into path and name components */
             scr_path* path_dest_file = scr_path_from_str(file);
             scr_path_basename(path_dest_file);
             scr_path_prepend_str(path_dest_file, dest_dir);
             char* dest_file = scr_path_strdup(path_dest_file);

             kvtree_util_set_str(transfer_file_hash,
                                   AXL_TRANSFER_KEY_DESTINATION,
                                   dest_file);
             kvtree_util_set_bytecount(transfer_file_hash,
                                         AXL_TRANSFER_KEY_SIZE,
                                         filesize);
             kvtree_util_set_bytecount(transfer_file_hash,
                                         AXL_TRANSFER_KEY_WRITTEN,
                                         0);

             /* delete path and string for the file name */
             scr_free(&dest_file);
             scr_path_delete(&path_dest_file);
     }
     else{
       axl_dbg(1,"-----file_hash was null BUG?-----'%s' @ %s:%d", file, __FILE__, __LINE__);

     }

     /* add this file to our total count */
     axl_flush_async_num_files++;
  }

  /* have master on each node write the transfer file, everyone else sends data to it */
  // TODO: Clean up paralleism
  //if (scr_storedesc_cntl->rank == 0) {
    /* receive hash data from other processes on the same node and merge with our data */
    //int i;
    //for (i=1; i < scr_storedesc_cntl->ranks; i++) {
    //  kvtree* h = kvtree_new();
    //  kvtree_recv(h, i, scr_storedesc_cntl->comm);
    //  kvtree_merge(axl_flush_async_hash, h);
    //  kvtree_delete(&h);
    //}
    /* get a hash to store file data */
    kvtree* hash = kvtree_new();

    /* open transfer file with lock */
    int fd = -1;
    kvtree_lock_open_read(axl_transfer_file, &fd, hash);

    /* merge our data to the file data */
    kvtree_merge(hash, axl_flush_async_hash);

    /* set BW if it's not already set */
    /* TODO: somewhat hacky way to determine number of nodes and therefore number of writers */
    //int writers;
    //MPI_Comm_size(scr_comm_node_across, &writers);
    // TODO: Clean up parallelism
    int writers = 1;
    double bw;
    if (kvtree_util_get_double(hash, AXL_TRANSFER_KEY_BW, &bw) !=
        AXL_SUCCESS) {
      bw = (double) scr_flush_async_bw / (double) writers;
      kvtree_util_set_double(hash, AXL_TRANSFER_KEY_BW, bw);
    }

    /* set PERCENT if it's not already set */
    double percent;
    if (kvtree_util_get_double(hash, AXL_TRANSFER_KEY_PERCENT, &percent) !=
        AXL_SUCCESS) {
      kvtree_util_set_double(hash, AXL_TRANSFER_KEY_PERCENT,
                               scr_flush_async_percent);
    }

    /* set the RUN command */
    kvtree_util_set_str(hash, AXL_TRANSFER_KEY_COMMAND,
                          AXL_TRANSFER_KEY_COMMAND_RUN);

    /* unset the DONE flag */
    kvtree_unset_kv(hash, AXL_TRANSFER_KEY_FLAG, AXL_TRANSFER_KEY_FLAG_DONE);

    /* close the transfer file and release the lock */
    kvtree_write_close_unlock(axl_transfer_file, &fd, hash);

    /* delete the hash */
    kvtree_delete(&hash);
  //} else {
  //  /* send our transfer hash data to the master on this node */
  //  kvtree_send(axl_flush_async_hash, 0, scr_storedesc_cntl->comm);
  //}

  /* get the total number of bytes to write */
  scr_flush_async_bytes = 0.0;
  MPI_Allreduce(&my_bytes, &scr_flush_async_bytes, 1, MPI_DOUBLE, MPI_SUM,
                scr_comm_world);

  /* TODO: start transfer thread / process */

  return AXL_SUCCESS;
  // TODO: Caller may need to call MPI_Barrier

}

/* check whether the flush from cache to parallel file system has completed */
// TODO: Caller must check value of scr_flush
int axl_flush_async_test(int id, double* bytes, axl_xfer_t TYPE)
{

#ifdef HAVE_LIBCPPR
  axl_dbg(1, "axl_flush_async_cppr_test being called by axl_flush_async_test \
@ %s:%d", __FILE__, __LINE__);
  return axl_flush_async_test_cppr(id, bytes);
#endif

  /* initialize bytes to 0 */
  *bytes = 0.0;

  axl_dbg(1,"axl_flush_async_test called @ %s:%d", __FILE__, __LINE__);
  /* assume the transfer is complete */
  int transfer_complete = 1;

  /* have master on each node check whether the flush is complete */
  double bytes_written = 0.0;
  // TODO: Clean up paralleism
  //if (scr_storedesc_cntl->rank == 0) {
    /* create a hash to hold the transfer file data */
    kvtree* hash = kvtree_new();

    /* read transfer file with lock */
    if (kvtree_read_with_lock(axl_transfer_file, hash) == AXL_SUCCESS) {
      /* test each file listed in the transfer hash */
      if (axl_flush_async_file_test(hash, &bytes_written) != AXL_SUCCESS) {
        transfer_complete = 0;
      }
    } else {
      /* failed to read the transfer file, can't determine whether the flush is complete */
      transfer_complete = 0;
    }

    /* free the hash */
    kvtree_delete(&hash);
  //}

  /* compute the total number of bytes written */
  // TODO: Clean up parallelism
  //MPI_Allreduce(&bytes_written, bytes, 1, MPI_DOUBLE, MPI_SUM, scr_comm_world);

  /* determine whether the transfer is complete on all tasks */
  if (transfer_complete) {
      axl_dbg(0, "#demo AXL async daemon successfully transferred dset %d", id);
      return AXL_SUCCESS;
  }
  return AXL_FAILURE;
}

/* complete the flush from cache to parallel file system */
// TODO: Caller must check value of scr_flush
int axl_flush_async_complete(int id, axl_xfer_t TYPE)
{
#ifdef HAVE_LIBCPPR
  return axl_flush_async_complete_cppr(id);
#endif
  int flushed = AXL_SUCCESS;

  /* TODO: have master tell each rank on node whether its files were written successfully */
  axl_dbg(1,"axl_flush_async_complete called @ %s:%d", __FILE__, __LINE__);
  /* allocate structure to hold metadata info */
  kvtree* data = kvtree_new();

  /* fill in metadata info for the files this process flushed */
  kvtree* file_list = kvtree_getf(axl_flush_async_file_lists, "%s %d", AXL_HANDLE_UID, id);
  kvtree* files = kvtree_get(file_list, AXL_KEY_FILE);
  kvtree_elem* elem = NULL;
  for (elem = kvtree_elem_first(files);
       elem != NULL;
       elem = kvtree_elem_next(elem))
  {
    /* get the filename */
    char* file = kvtree_elem_key(elem);

    /* get the hash for this file */
    kvtree* hash = kvtree_elem_hash(elem);

    /* record the filename in the hash, and get reference to a hash for this file */
    scr_path* path_file = scr_path_from_str(file);
    scr_path_basename(path_file);
    char* name = scr_path_strdup(path_file);
    kvtree* file_hash = kvtree_set_kv(data, SCR_SUMMARY_6_KEY_FILE, name);
    scr_free(&name);
    scr_path_delete(&path_file);

    /* TODO: check that this file was written successfully */

    /* get meta data for this file */
    scr_meta* meta = kvtree_get(hash, AXL_KEY_META);

    /* successfully flushed this file, record the filesize */
    unsigned long filesize = 0;
    if (scr_meta_get_filesize(meta, &filesize) == AXL_SUCCESS) {
      kvtree_util_set_bytecount(file_hash, SCR_SUMMARY_6_KEY_SIZE, filesize);
    }

    /* record the crc32 if one was computed */
    uLong flush_crc32;
    if (scr_meta_get_crc32(meta, &flush_crc32) == AXL_SUCCESS) {
      kvtree_util_set_crc32(file_hash, SCR_SUMMARY_6_KEY_CRC, flush_crc32);
    }
  }

  /* write summary file */
  if (scr_flush_complete(id, file_list, data) != AXL_SUCCESS) {
    flushed = AXL_FAILURE;
  }

  /* have master on each node remove files from the transfer file */
  // TODO: Clean up paralleism
  //if (scr_storedesc_cntl->rank == 0) {
    /* get a hash to read from the file */
    kvtree* transfer_hash = kvtree_new();

    /* lock the transfer file, open it, and read it into the hash */
    int fd = -1;
    kvtree_lock_open_read(axl_transfer_file, &fd, transfer_hash);

    /* remove files from the list */
    axl_flush_async_file_dequeue(transfer_hash, axl_flush_async_hash);

    /* set the STOP command */
    kvtree_util_set_str(transfer_hash, AXL_TRANSFER_KEY_COMMAND, AXL_TRANSFER_KEY_COMMAND_STOP);

    /* write the hash back to the file */
    kvtree_write_close_unlock(axl_transfer_file, &fd, transfer_hash);

    /* delete the hash */
    kvtree_delete(&transfer_hash);
  //}

  /* mark that we've stopped the flush */
  axl_flush_async_in_progress = 0;
  scr_flush_file_location_unset(id, AXL_FLUSH_KEY_LOCATION_FLUSHING);

  /* free data structures */
  kvtree_delete(&data);

  /* free the file list for this checkpoint */
  kvtree_delete(&axl_flush_async_hash);
  kvtree_delete(&file_list);
  axl_flush_async_hash      = NULL;
  file_list = NULL;

  /* stop timer, compute bandwidth, and report performance */
  // TODO: Clean up parallelism
  //if (scr_my_rank_world == 0) {
    double time_end = axl_get_time();
    double time_diff = time_end - axl_flush_async_time_start;
    double bw = scr_flush_async_bytes / (1024.0 * 1024.0 * time_diff);
    axl_dbg(1, "axl_flush_async_complete: %f secs, %e bytes, %f MB/s, %f MB/s per proc",
      time_diff, scr_flush_async_bytes, bw, bw/scr_ranks_world
    );

    /* log messages about flush */
    if (flushed == AXL_SUCCESS) {
      /* the flush worked, print a debug message */
      axl_dbg(1, "axl_flush_async_complete: Flush of dataset %d succeeded", id);

      /* log details of flush */
    } else {
      /* the flush failed, this is more serious so print an error message */
      axl_err("axl_flush_async_complete: Flush failed");
    }
  }

  return flushed;
}

/* wait until the checkpoint currently being flushed completes */
int axl_flush_async_wait(int id, axl_xfer_t TYPE)
{
  if (axl_flush_async_in_progress) {
    while (scr_flush_file_is_flushing(axl_flush_async_dataset_id)) {
      /* test whether the flush has completed, and if so complete the flush */
      double bytes = 0.0;
      if (axl_flush_async_test(id, &bytes, TYPE) == AXL_SUCCESS) {
        /* complete the flush */
        axl_flush_async_complete(id, TYPE);
      } else {
        /* otherwise, sleep to get out of the way */
        axl_dbg(1, "Flush of checkpoint %d is %d%% complete",
            axl_flush_async_dataset_id,
            (int) (bytes / scr_flush_async_bytes * 100.0)
        );
        usleep(10*1000*1000);
      }
    }
  }
  return AXL_SUCCESS;
}

/* start any processes for later asynchronous flush operations */
int axl_flush_async_init(char* cntl_dir){

    char* axl_cntl_dir;
    if (cntl_dir){
        axl_cntl_dir = strcpy(cntl_dir);
    }else{
        axl_cntl_dir = getcwd(NULL, 0);
    }

    /* TODO: AXL or SCR owns flush file?
       For now, AXL has its own version, kept in cntl_dir */
    char* axl_flush_file_name = "/axl_flush.info";
    axl_flush_file = malloc(strlen(axl_cntl_dir) + strlen(axl_flush_file_name));
    strcpy(axl_flush_file, axl_cntl_dir);
    strcat(axl_flush_file, axl_flush_file_name);

#ifdef HAVE_DAEMON
  /* daemon stuff */
  char* axl_transfer_file_name = "/axl_transfer.info";
  axl_transfer_file = malloc(strlen(axl_cntl_dir) + strlen(axl_transfer_file_name));
  strcpy(axl_transfer_file, axl_cntl_dir);
  strcat(axl_transfer_file, axl_transfer_file_name);

  axl_free(&axl_cntl_dir);

  /* wait until transfer daemon is stopped */
  axl_flush_async_stop(id, TYPE);

  /* clear out the file */
  /* done by all ranks (to avoid mpi dependency)
   * Could go back to one/node (or other storage desc as appropriate
   */
  axl_file_unlink(axl_transfer_file);
#endif

  return AXL_SUCCESS;
}

/* stop all ongoing asynchronous flush operations */
int axl_flush_async_finalize()
{
#ifdef HAVE_DAEMON
  axl_free(&axl_transfer_file);
#endif

  axl_file_unlink(axl_flush_file);

  return AXL_SUCCESS;
}
