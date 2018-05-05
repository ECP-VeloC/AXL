#include <string.h>

#include "kvtree.h"
#include "kvtree_util.h"

#include "axl_internal.h"
#include "axl_async_daemon.h"
#include "axl_async_daemon_keys.h"

static double axl_async_daemon_bw = 0.0;
static double axl_async_daemon_percent = 0.0;
static char* axl_async_daemon_file = NULL;

/*
=========================================
Asynchronous transfer functions
=========================================
*/

/* given a hash of data from transfer file, test whether the files have completed their transfer,
 * update corresponding files in map if done */
static int axl_async_daemon_file_test(kvtree* map, int id, const kvtree* transfer_hash, double* bytes_total, double* bytes_written)
{
  /* initialize bytes to 0 */
  *bytes_total   = 0.0;
  *bytes_written = 0.0;

  /* get pointer to source file list in map */
  kvtree* map_list = kvtree_get_kv_int(map, AXL_KEY_HANDLE_UID, id);
  kvtree* map_files = kvtree_get(map_list, AXL_KEY_FILES);

  /* get hash for this id */
  kvtree* hash = kvtree_get_kv_int(transfer_hash, AXL_TRANSFER_KEY_ID, id);

  /* get the FILES hash */
  kvtree* files_hash = kvtree_get(hash, AXL_TRANSFER_KEY_FILES);
  if (files_hash == NULL) {
    /* can't tell whether this transfer has completed */
    return AXL_FAILURE;
  }

  /* assume we're done, look for a file that says we're not */
  int transfer_complete = 1;
  int transfer_error = 0;

  /* for each file, check whether the WRITTEN field matches the SIZE field,
   * which indicates the file has completed its transfer */
  kvtree_elem* elem;
  for (elem = kvtree_elem_first(files_hash);
       elem != NULL;
       elem = kvtree_elem_next(elem))
  {
    /* get source name of file */
    const char* file = kvtree_elem_key(elem);

    /* get the hash for this file */
    kvtree* file_hash = kvtree_elem_hash(elem);
    if (file_hash == NULL) {
      transfer_complete = 0;
      continue;
    }

    /* lookup the values for the size and bytes written */
    unsigned long size, written;
    if (kvtree_util_get_bytecount(file_hash, AXL_TRANSFER_KEY_SIZE,    &size)    != KVTREE_SUCCESS ||
        kvtree_util_get_bytecount(file_hash, AXL_TRANSFER_KEY_WRITTEN, &written) != KVTREE_SUCCESS)
    {
      transfer_complete = 0;
      continue;
    }

    /* add up number of bytes written */
    *bytes_total   += (double) size;
    *bytes_written += (double) written;

    /* check for error message */
    char* errmsg = NULL;
    if (kvtree_util_get_str(file_hash, AXL_TRANSFER_KEY_ERROR, &errmsg) == KVTREE_SUCCESS) {
      /* found an error */
      transfer_complete = 0;
      transfer_error = 1;

      /* mark this file as having an error in map */
      kvtree* src_hash = kvtree_get(map_files, file);
      kvtree_util_set_int(src_hash, AXL_KEY_FLUSH_STATUS, AXL_FLUSH_STATUS_ERROR);
      continue;
    }

    /* check whether the number of bytes written is less than the filesize */
    if (written < size) {
      /* not done with this file yet */
      transfer_complete = 0;
    } else {
      /* mark this file as being complete in map */
      kvtree* src_hash = kvtree_get(map_files, file);
      kvtree_util_set_int(src_hash, AXL_KEY_FLUSH_STATUS, AXL_FLUSH_STATUS_DEST);
    }
  }

  /* return our decision */
  if (transfer_error) {
    /* mark entire transfer as being failed in map,
     * return SUCCESS to indicate that we're done waiting */
    kvtree_set_kv_int(map_list, AXL_KEY_FLUSH_STATUS, AXL_FLUSH_STATUS_ERROR);
    return AXL_SUCCESS;
  }
  if (transfer_complete) {
    /* mark entire transfer as being complete in map */
    kvtree_util_set_int(map_list, AXL_KEY_FLUSH_STATUS, AXL_FLUSH_STATUS_DEST);
    return AXL_SUCCESS;
  }
  return AXL_FAILURE;
}

/* writes the specified command to the transfer file */
static int axl_async_daemon_command_set(char* command)
{
  /* get a hash to store file data */
  kvtree* hash = kvtree_new();

  /* read the file */
  int fd = -1;
  kvtree_lock_open_read(axl_async_daemon_file, &fd, hash);

  /* set the command */
  kvtree_util_set_str(hash, AXL_TRANSFER_KEY_COMMAND, command);

  /* write the hash back */
  kvtree_write_close_unlock(axl_async_daemon_file, &fd, hash);

  /* delete the hash */
  kvtree_delete(&hash);

  return AXL_SUCCESS;
}

/* waits until transfer process is in the specified state */
static int axl_async_daemon_state_wait(char* state)
{
  /* wait until process matches the state */
  int valid = 0;
  while (! valid) {
    /* assume we match the specified state */
    valid = 1;

    /* get a hash to store file data */
    kvtree* hash = kvtree_new();

    /* open transfer file with lock */
    kvtree_read_with_lock(axl_async_daemon_file, hash);

    /* check for the specified state */
    kvtree* state_hash = kvtree_get_kv(hash, AXL_TRANSFER_KEY_STATE, state);
    if (state_hash == NULL) {
      valid = 0;
    }

    /* delete the hash */
    kvtree_delete(&hash);

    /* if we're not there yet, sleep for sometime and they try again */
    if (! valid) {
      usleep(10*1000*1000);
    }
  }
  return AXL_SUCCESS;
}

/* removes all files from the transfer file */
static int axl_async_daemon_file_clear_all()
{
  /* get a hash to store file data */
  kvtree* hash = kvtree_new();

  /* read the file */
  int fd = -1;
  kvtree_lock_open_read(axl_async_daemon_file, &fd, hash);

  /* clear the ID entry */
  kvtree_unset(hash, AXL_TRANSFER_KEY_ID);

  /* write the hash back */
  kvtree_write_close_unlock(axl_async_daemon_file, &fd, hash);

  /* delete the hash */
  kvtree_delete(&hash);

  return AXL_SUCCESS;
}

/* cancel all ongoing asynchronous transfer operations */
int axl_flush_async_stop_daemon()
{
  /* write stop command to transfer file */
  axl_async_daemon_command_set(AXL_TRANSFER_KEY_COMMAND_STOP);

  /* wait until the daemon has stopped */
  axl_async_daemon_state_wait(AXL_TRANSFER_KEY_STATE_STOP);

  /* remove the files list from the transfer file */
  axl_async_daemon_file_clear_all();

  return AXL_SUCCESS;
}

/* start an asynchronous transfer */
int axl_flush_async_start_daemon(kvtree* map, int id)
{
  /* assume success */
  int rc = AXL_SUCCESS;

  /* lookup transfer info for id */
  kvtree* file_list = kvtree_get_kv_int(map, AXL_KEY_HANDLE_UID, id);

  /* add each file to the transfer file list */
  kvtree* transfer_list = kvtree_new();
  kvtree_elem* elem;
  kvtree* files = kvtree_get(file_list, AXL_KEY_FILES);
  for (elem = kvtree_elem_first(files);
       elem != NULL;
       elem = kvtree_elem_next(elem))
  {
     /* get the filename */
     char* file = kvtree_elem_key(elem);

     /* get the hash for this file */
     kvtree* file_hash = kvtree_elem_hash(elem);

     /* get destination path */
     char* dest_file;
     if (kvtree_util_get_str(file_hash, AXL_KEY_FILE_DEST, &dest_file) != KVTREE_SUCCESS) {
       rc = AXL_FAILURE;
       continue;
     }

     /* get the file size */
     unsigned long filesize = axl_file_size(file);

     /* add this file and its filesize to the list */
     kvtree* transfer_id_hash = kvtree_set_kv_int(transfer_list, AXL_TRANSFER_KEY_ID, id);
     kvtree* transfer_file_hash = kvtree_set_kv(transfer_id_hash, AXL_TRANSFER_KEY_FILES, file);
     kvtree_util_set_str(transfer_file_hash, AXL_TRANSFER_KEY_DESTINATION, dest_file);
     kvtree_util_set_bytecount(transfer_file_hash, AXL_TRANSFER_KEY_SIZE, filesize);
     kvtree_util_set_bytecount(transfer_file_hash, AXL_TRANSFER_KEY_WRITTEN, 0);
  }

  /* get a hash to store file data */
  kvtree* hash = kvtree_new();

  /* open transfer file with lock */
  int fd = -1;
  kvtree_lock_open_read(axl_async_daemon_file, &fd, hash);

  /* merge our data to the file data */
  kvtree_merge(hash, transfer_list);

  /* set BW if it's not already set */
  double bw;
  if (kvtree_util_get_double(hash, AXL_TRANSFER_KEY_BW, &bw) != KVTREE_SUCCESS) {
    kvtree_util_set_double(hash, AXL_TRANSFER_KEY_BW, axl_async_daemon_bw);
  }

  /* set PERCENT if it's not already set */
  double percent;
  if (kvtree_util_get_double(hash, AXL_TRANSFER_KEY_PERCENT, &percent) != KVTREE_SUCCESS) {
    kvtree_util_set_double(hash, AXL_TRANSFER_KEY_PERCENT, axl_async_daemon_percent);
  }

  /* set the RUN command */
  kvtree_util_set_str(hash, AXL_TRANSFER_KEY_COMMAND, AXL_TRANSFER_KEY_COMMAND_RUN);

  /* unset the DONE flag */
  kvtree_unset_kv(hash, AXL_TRANSFER_KEY_FLAG, AXL_TRANSFER_KEY_FLAG_DONE);

  /* close the transfer file and release the lock */
  kvtree_write_close_unlock(axl_async_daemon_file, &fd, hash);

  /* delete the hash */
  kvtree_delete(&hash);

  /* delete transfer list */
  kvtree_delete(&transfer_list);

  /* update transfer status as started */
  if (rc == AXL_SUCCESS) {
    kvtree_set_kv_int(file_list, AXL_KEY_FLUSH_STATUS, AXL_FLUSH_STATUS_INPROG);
  } else {
    kvtree_set_kv_int(file_list, AXL_KEY_FLUSH_STATUS, AXL_FLUSH_STATUS_ERROR);
  }

  return rc;
}

/* complete the transfer by removing entries from the transfer file,
 * stop daemon if that was the last active transfer */
static int axl_async_daemon_complete(kvtree* map, int id)
{
  /* get a hash to read from the file */
  kvtree* transfer_hash = kvtree_new();

  /* lock the transfer file, open it, and read it into the hash */
  int fd = -1;
  kvtree_lock_open_read(axl_async_daemon_file, &fd, transfer_hash);

  /* remove files from the list */
  kvtree_unset_kv_int(transfer_hash, AXL_TRANSFER_KEY_ID, id);

  /* if that was the last active transfer, stop the daemon */
  kvtree* transfers = kvtree_get(transfer_hash, AXL_TRANSFER_KEY_ID);
  if (transfers == NULL) {
    /* set the STOP command */
    kvtree_util_set_str(transfer_hash, AXL_TRANSFER_KEY_COMMAND, AXL_TRANSFER_KEY_COMMAND_STOP);
  }

  /* write the hash back to the file */
  kvtree_write_close_unlock(axl_async_daemon_file, &fd, transfer_hash);

  /* delete the hash */
  kvtree_delete(&transfer_hash);

  /* TODO: wait until transfer process is stopped? */

  return AXL_SUCCESS;
}

/* check whether the specified transfer id has completed */
int axl_flush_async_test_daemon(kvtree* map, int id, double* bytes_total, double* bytes_written)
{
  /* initialize bytes to 0 */
  *bytes_total = 0.0;
  *bytes_written = 0.0;

  /* assume the transfer is complete */
  int transfer_complete = 1;

  /* read transfer file with lock */
  kvtree* hash = kvtree_new();
  if (kvtree_read_with_lock(axl_async_daemon_file, hash) == KVTREE_SUCCESS) {
    /* test each file listed in the transfer hash */
    if (axl_async_daemon_file_test(map, id, hash, bytes_total, bytes_written) != AXL_SUCCESS) {
      transfer_complete = 0;
    } else {
      /* finished, so complete the transfer */
      axl_async_daemon_complete(map, id);
    }
  } else {
    /* failed to read the transfer file, can't determine whether the transfer is complete */
    transfer_complete = 0;
  }
  kvtree_delete(&hash);

  /* determine whether the transfer is complete */
  if (transfer_complete) {
    return AXL_SUCCESS;
  }
  return AXL_FAILURE;
}

/* wait until the specified id completes */
int axl_flush_async_wait_daemon(kvtree* map, int id)
{
  /* keep testing until it's done */
  int done = 0;
  while (! done) {
    /* test whether the transfer has completed, and if so complete the transfer */
    double bytes_total, bytes_written;
    if (axl_flush_async_test_daemon(map, id, &bytes_total, &bytes_written) == AXL_SUCCESS) {
      /* complete the transfer */
      axl_async_daemon_complete(map, id);
      done = 1;
    } else {
      /* otherwise, sleep to get out of the way */
      usleep(10*1000*1000);
    }
  }
  return AXL_SUCCESS;
}

/* start process for transfer operations */
int axl_flush_async_init_daemon(const char* transfer_file)
{
  /* copy name for transfer file */
  axl_async_daemon_file = strdup(transfer_file);

  /* TODO: read configuration to set bw and percent limits */

  /* TODO: launch daemon process here? */

  return AXL_SUCCESS;
}

/* stop all ongoing transfer operations, wait for daemon process to exit */
int axl_flush_async_finalize_daemon()
{
  /* write stop command to transfer file */
  axl_async_daemon_command_set(AXL_TRANSFER_KEY_COMMAND_EXIT);

  /* wait until all tasks know the transfer is shutdown */
  axl_async_daemon_state_wait(AXL_TRANSFER_KEY_STATE_EXIT);

  /* delete our transfer file */
  axl_file_unlink(axl_async_daemon_file);
  axl_free(&axl_async_daemon_file);

  return AXL_SUCCESS;
}
