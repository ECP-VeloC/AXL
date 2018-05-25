#include "axl_internal.h"
#include "fu_filemap.h"
#include "kvtree.h"

#ifdef HAVE_LIBDTCMP
#include "dtcmp.h"
#endif /* HAVE_LIBDTCMP */

/*
=========================================
Common flush functions
=========================================
*/

/* returns true if the named file needs to be flushed, 0 otherwise */
int axl_bool_flush_file(
  const fu_filemap* map,
  int dset,
  int rank,
  const char* file)
{
  /* assume we need to flush this file */
  int flush = 1;

  /* read meta info for file */
  fu_meta* meta = fu_meta_new();
  if (fu_filemap_get_meta(map, dset, rank, file, meta) == AXL_SUCCESS) {
    /* don't flush XOR files */
    if (fu_meta_check_filetype(meta, FU_META_FILE_XOR) == AXL_SUCCESS) {
      flush = 0;
    }
  } else {
    /* TODO: print error */
  }
  fu_meta_delete(&meta);

  return flush;
}

/*
=========================================
Prepare for flush by building list of files and creating directories
=========================================
*/

#define AXL_FLUSH_SCAN_COUNT (0)
#define AXL_FLUSH_SCAN_RANKS (1)
#define AXL_FLUSH_SCAN_RANK  (2)
int axl_flush_pick_writer(
  int level,
  unsigned long count,
  int* outwriter,
  int* outranks)
{
  /* use a segment size of 1MB */
  unsigned long segsize = 1024*1024;

  /* get communicator info */
  MPI_Comm comm  = scr_comm_world;
  int rank       = scr_my_rank_world;
  int ranks      = scr_ranks_world;

  /* first find our offset */
  unsigned long offset;
  MPI_Scan(&count, &offset, 1, MPI_UNSIGNED_LONG, MPI_SUM, comm);
  offset -= count;

  /* determine whether we start a new segment */
  int starts_new = 0;
  if (rank == 0) {
    /* force rank 0 to start a segment, even if it has no bytes */
    starts_new = 1;
  } else if (count > 0) {
    /* otherwise we only start a segment if our bytes overflow the boundary */
    unsigned long segindex = offset / segsize;
    unsigned long seglastbyte = (segindex + 1) * segsize - 1;
    unsigned long mylastbyte  = offset + count - 1;
    if (mylastbyte > seglastbyte) {
      starts_new = 1;
    }
  }

  /* initialize our send data */
  int send[3], recv[3];
  send[SCR_FLUSH_SCAN_COUNT] = starts_new;
  send[SCR_FLUSH_SCAN_RANKS] = 1;
  send[SCR_FLUSH_SCAN_RANK]  = MPI_PROC_NULL;
  if (starts_new) {
    send[SCR_FLUSH_SCAN_RANK] = rank;
  }

  /* first execute the segmented scan */
  int step = 1;
  MPI_Request request[4];
  MPI_Status  status[4];
  while (step < ranks) {
    int k = 0;

    /* if we have a left partner, recv its right-going data */
    int left = rank - step;
    if (left >= 0) {
      MPI_Irecv(recv, 3, MPI_INT, left, 0, comm, &request[k]);
      k++;
    }

    /* if we have a right partner, send it our right-going data */
    int right = rank + step;
    if (right < ranks) {
      MPI_Isend(send, 3, MPI_INT, right, 0, comm, &request[k]);
      k++;
    }

    /* wait for all communication to complete */
    if (k > 0) {
      MPI_Waitall(k, request, status);
    }

    /* if we have a left partner, merge its data with our result */
    if (left >= 0) {
      /* reduce data into right-going buffer */
      send[SCR_FLUSH_SCAN_COUNT] += recv[SCR_FLUSH_SCAN_COUNT];
      if (send[SCR_FLUSH_SCAN_RANK] == MPI_PROC_NULL) {
        send[SCR_FLUSH_SCAN_RANKS] += recv[SCR_FLUSH_SCAN_RANKS];
        send[SCR_FLUSH_SCAN_RANK]   = recv[SCR_FLUSH_SCAN_RANK];
      }
    }

    /* go to next round */
    step *= 2;
  }

  /* we don't use this for now, but keep it in case we go back to it */
/* int writer_id = send[SCR_FLUSH_SCAN_COUNT] - 1; */

  /* set output parameters */
  *outwriter = send[SCR_FLUSH_SCAN_RANK];

  /* determine whether we've finished, the last rank knows the total
   * in the group */
  *outranks = send[SCR_FLUSH_SCAN_RANKS];
  MPI_Bcast(outranks, 1, MPI_INT, ranks-1, comm);

  return AXL_SUCCESS;
}

/* given a dataset, return a newly allocated string specifying the
 * metadata directory for that dataset, must be freed by caller */
static char* scr_dataset_metadir(const scr_dataset* dataset)
{
  /* get the name of the dataset */
  int id;
  if (scr_dataset_get_id(dataset, &id) != AXL_SUCCESS) {
    scr_abort(-1, "Failed to get dataset id @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* define metadata directory for dataset */
  scr_path* path = scr_path_from_str(scr_prefix_scr);
  scr_path_append_strf(path, "scr.dataset.%d", id);
  char* dir = scr_path_strdup(path);
  scr_path_delete(&path);

  return dir;
}

/* fills in hash with a list of filenames and associated meta data
 * that should be flushed for specified dataset id */
static int scr_flush_identify_files(
  const fu_filemap* map,
  int id,
  scr_hash* file_list)
{
  int rc = AXL_SUCCESS;

  /* lookup dataset from filemap and store in file list */
  scr_dataset* dataset = scr_hash_new();
  fu_filemap_get_dataset(map, id, scr_my_rank_world, dataset);
  scr_hash_set(file_list, SCR_KEY_DATASET, dataset);

  /* identify which files we need to flush as part of the specified
   * dataset id */
  scr_hash_elem* elem = NULL;
  for (elem = fu_filemap_first_file(map, id, scr_my_rank_world);
       elem != NULL;
       elem = scr_hash_elem_next(elem))
  {
    /* get the filename */
    char* file = scr_hash_elem_key(elem);

    /* read meta data for file and attach it to file list */
    fu_meta* meta = fu_meta_new();
    if (fu_filemap_get_meta(map, id, scr_my_rank_world, file, meta) == AXL_SUCCESS) {
      /* don't flush XOR files */
      int flush = 1;
      if (fu_meta_check_filetype(meta, FU_META_FILE_XOR) == AXL_SUCCESS) {
        flush = 0;
      }

      /* TODO: shouldn't we avoid flushing partner files? */

      /* if we need to flush this file, add it to the list and attach
       * its meta data */
      if (flush) {
        scr_hash* file_hash = scr_hash_set_kv(file_list, SCR_KEY_FILE, file);
        scr_hash_set(file_hash, SCR_KEY_META, meta);
        meta = NULL;
      }
    } else {
      /* TODO: print error */
      rc = AXL_FAILURE;
    }

    /* if we didn't attach the meta data, we need to delete it */
    if (meta != NULL) {
      fu_meta_delete(&meta);
    }
  }

  return rc;
}

/* build list of directories needed for file list (one per file) */
static int scr_flush_identify_dirs(scr_hash* file_list)
{
  /* get the dataset for this list of files */
  scr_dataset* dataset = scr_hash_get(file_list, SCR_KEY_DATASET);

  /* get the name of the dataset */
  char* name;
  if (scr_dataset_get_name(dataset, &name) != AXL_SUCCESS) {
    scr_abort(-1, "Failed to get dataset name @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* record dataset name for flush */
  scr_hash_util_set_str(file_list, SCR_KEY_NAME, name);

  /* record path for dataset metatdata directory */
  char* metadir = scr_dataset_metadir(dataset);
  scr_hash_util_set_str(file_list, SCR_KEY_PATH, metadir);
  scr_free(&metadir);

  if (scr_preserve_directories) {
    /* preserving user-defined directories, identify them here */

    /* TODO: need to list dirs in order from parent to child */

    /* TODO: PRESERVE need to track directory names in summary
     * file so we can delete them later */

    /* get pointer to file hash */
    scr_hash* files = scr_hash_get(file_list, SCR_KEY_FILE);

    /* count the number of files that we need to flush */
    int count = scr_hash_size(files);

    /* allocate buffers to hold the directory needed for each file */
    const char** dirs     = (const char**) SCR_MALLOC(sizeof(const char*) * count);
    uint64_t* group_id    = (uint64_t*)    SCR_MALLOC(sizeof(uint64_t)    * count);
    uint64_t* group_ranks = (uint64_t*)    SCR_MALLOC(sizeof(uint64_t)    * count);
    uint64_t* group_rank  = (uint64_t*)    SCR_MALLOC(sizeof(uint64_t)    * count);

    /* lookup directory from meta data for each file */
    int i = 0;
    scr_hash_elem* elem = NULL;
    for (elem = scr_hash_elem_first(files);
         elem != NULL;
         elem = scr_hash_elem_next(elem))
    {
      /* get meta data for this file */
      scr_hash* hash = scr_hash_elem_hash(elem);
      fu_meta* meta = scr_hash_get(hash, SCR_KEY_META);

      /* lookup original path where application wants file to go */
      dirs[i] = NULL;
      char* dir;
      if (fu_meta_get_origpath(meta, &dir) == AXL_SUCCESS) {
        /* record pointer to directory name */
        dirs[i] = dir;

        /* record original path as path to flush to in file list */
        scr_hash_util_set_str(hash, SCR_KEY_PATH, dir);

#ifndef HAVE_LIBDTCMP
        /* if we're preserving directories but if we don't have DTCMP,
         * then we'll just issue a mkdir for each file, lots of extra
         * load on the file system, but this works */

        /* add this directory to be created */
        scr_hash_set_kv(file_list, SCR_KEY_DIRECTORY, dirs[i]);
#endif
      } else {
        scr_abort(-1, "Failed to read original path name for a file @ %s:%d",
          __FILE__, __LINE__
        );
      }

      /* add one to our file count */
      i++;
    }

#ifdef HAVE_LIBDTCMP
    /* with DTCMP we identify a single process to create each directory */

    /* identify the set of unique directories */
    uint64_t groups;
    int dtcmp_rc = DTCMP_Rankv_strings(
      count, dirs, &groups, group_id, group_ranks, group_rank,
      DTCMP_FLAG_NONE, scr_comm_world
    );
    if (dtcmp_rc != DTCMP_SUCCESS) {
      scr_abort(-1, "Failed to rank strings during flush @ %s:%d",
        __FILE__, __LINE__
      );
    }

    /* select leader for each directory */
    for (i = 0; i < count; i++) {
      if (group_rank[i] == 0) {
        scr_hash_set_kv(file_list, SCR_KEY_DIRECTORY, dirs[i]);
      }
    }
#endif /* HAVE_LIBDTCMP */

    /* free buffers */
    scr_free(&group_id);
    scr_free(&group_ranks);
    scr_free(&group_rank);
    scr_free(&dirs);
  } else {
    /* build the dataset directory name */
    scr_path* path_dir = scr_path_dup(scr_prefix_path);
    scr_path_append_str(path_dir, name);
    char* dir = scr_path_strdup(path_dir);

    /* have rank 0 be responsible for creating dataset directory */
    if (scr_my_rank_world == 0) {
      scr_hash_set_kv(file_list, SCR_KEY_DIRECTORY, dir);
    }

    /* add the flush directory to each file in the list */
    scr_hash_elem* elem = NULL;
    scr_hash* files = scr_hash_get(file_list, SCR_KEY_FILE);
    for (elem = scr_hash_elem_first(files);
         elem != NULL;
         elem = scr_hash_elem_next(elem))
    {
      /* get meta data for this file */
      scr_hash* hash = scr_hash_elem_hash(elem);
      scr_hash_util_set_str(hash, SCR_KEY_PATH, dir);
    }

    /* free the string and path */
    scr_free(&dir);
    scr_path_delete(&path_dir);
  }

  return AXL_SUCCESS;
}

/* given file map and dataset id, identify files and
 * directories needed for flush and return in file list hash */
static int scr_flush_identify(
  const fu_filemap* map,
  int id,
  scr_hash* file_list)
{
  /* check that we have all of our files */
  int have_files = 1;
  if (scr_cache_check_files(map, id) != AXL_SUCCESS) {
    scr_err("Missing one or more files for dataset %d @ %s:%d",
      id, __FILE__, __LINE__
    );
    have_files = 0;
  }
  if (! scr_alltrue(have_files)) {
    if (scr_my_rank_world == 0) {
      scr_err("One or more processes are missing files for dataset %d @ %s:%d",
        id, __FILE__, __LINE__
      );
    }
    return AXL_FAILURE;
  }

  /* build the list of files to flush, which includes meta data for each one */
  if (scr_flush_identify_files(map, id, file_list) != AXL_SUCCESS) {
    scr_abort(-1, "Failed to get list of files for dataset %d @ %s:%d",
      id, __FILE__, __LINE__
    );
  }

  /* build the list of directories to create */
  if (scr_flush_identify_dirs(file_list) != AXL_SUCCESS) {
    scr_abort(-1, "Failed to get list of directories for dataset %d @ %s:%d",
      id, __FILE__, __LINE__
    );
  }

  return AXL_SUCCESS;
}

/* create all directories needed for file list */
static int scr_flush_create_dirs(scr_hash* file_list)
{
  /* get file mode for directory permissions */
  mode_t mode_dir = scr_getmode(1, 1, 1);

  /* have rank 0 create the dataset directory */
  if (scr_my_rank_world == 0) {
    /* get the dataset for this list of files */
    scr_dataset* dataset = scr_hash_get(file_list, SCR_KEY_DATASET);

    /* get the id of the dataset */
    int id;
    if (scr_dataset_get_id(dataset, &id) != AXL_SUCCESS) {
      scr_abort(-1, "Failed to get dataset id @ %s:%d",
        __FILE__, __LINE__
      );
    }

    /* get the name of the dataset */
    char* name;
    if (scr_dataset_get_name(dataset, &name) != AXL_SUCCESS) {
      scr_abort(-1, "Failed to get dataset name @ %s:%d",
        __FILE__, __LINE__
      );
    }

    /* add the name to our index file, and record the flush timestamp */
    scr_hash* index_hash = scr_hash_new();
    scr_index_read(scr_prefix_path, index_hash);
    scr_index_set_dataset(index_hash, id, name, dataset, 0);
    scr_index_add_name(index_hash, id, name);
    scr_index_mark_flushed(index_hash, id, name);
    scr_index_write(scr_prefix_path, index_hash);
    scr_hash_delete(&index_hash);

    /* create the dataset metadata directory */
    char* dataset_dir = scr_dataset_metadir(dataset);
    if (scr_mkdir(dataset_dir, mode_dir) == AXL_SUCCESS) {
      /* created the directory successfully */
      scr_dbg(1, "Flushing dataset %s", name);
    } else {
      /* failed to create the directory */
      scr_abort(-1, "Failed to make dataset directory mkdir(%s) @ %s:%d",
        dataset_dir, __FILE__, __LINE__
      );
    }
    scr_free(&dataset_dir);
  }

  /* wait for rank 0 */
  MPI_Barrier(scr_comm_world);

  /* TODO: add flow control here */
  /* create other directories in file list */
  int success = 1;
  scr_hash_elem* elem = NULL;
  scr_hash* dirs = scr_hash_get(file_list, SCR_KEY_DIRECTORY);
  for (elem = scr_hash_elem_first(dirs);
       elem != NULL;
       elem = scr_hash_elem_next(elem))
  {
    /* create directory */
    char* dir = scr_hash_elem_key(elem);
    if (scr_mkdir(dir, mode_dir) != AXL_SUCCESS) {
      success = 0;
    }
  }

  /* TODO: PRESERVE need to track directory names in summary file so we can delete them later */

  /* determine whether all leaders successfully created their directories */
  if (! scr_alltrue((success == 1))) {
    return AXL_FAILURE;
  }
  return AXL_SUCCESS;
}

/* given a filemap and a dataset id, prepare and return a list of
 * files to be flushed, also create corresponding directories */
int axl_flush_prepare(const fu_filemap* map, int id, scr_hash* file_list)
{
  /* build the list of files to flush, which includes meta data for each one */
  if (axl_flush_identify(map, id, file_list) != AXL_SUCCESS) {
    axl_abort(-1, "Failed to identify data for flush of dataset %d @ %s:%d",
      id, __FILE__, __LINE__
    );
  }

  /* create directories for flush */
  if (axl_flush_create_dirs(file_list) != AXL_SUCCESS) {
    /* TODO: delete the directories that we just created above? */
    if (scr_my_rank_world == 0) {
      axl_err("Failed to create flush directories for dataset %d @ %s:%d",
        id, __FILE__, __LINE__
      );
    }
    return AXL_FAILURE;
  }

  return AXL_SUCCESS;
}

/*
=========================================
Complete the flush by writing the summary file and updating the
index file
=========================================
*/

static unsigned long scr_flush_summary_map(
  const scr_path* dataset_path,
  const scr_path* file,
  unsigned long offset,
  int level,
  int valid)
{
  int rc = AXL_SUCCESS;

  /* record file name relative to prefix dir */
  scr_hash* hash = scr_hash_new();
  unsigned long pack_size = 0;
  if (valid) {
    scr_path* rel_path = scr_path_relative(scr_prefix_path, file);
    const char* rel_file = scr_path_strdup(rel_path);
    scr_hash_util_set_str(hash, SCR_SUMMARY_6_KEY_FILE, rel_file);
    scr_hash_util_set_bytecount(hash, SCR_SUMMARY_6_KEY_OFFSET, offset);
    scr_free(&rel_file);
    scr_path_delete(&rel_path);
    pack_size = (unsigned long) scr_hash_pack_size(hash);
  }

  /* pick writers so that we send roughly 1MB of data to each */
  int writer, ranks;
  scr_flush_pick_writer(level, pack_size, &writer, &ranks);

  /* create new hashes to send and receive data */
  scr_hash* send = scr_hash_new();
  scr_hash* recv = scr_hash_new();

  /* if we have valid values, prepare hash to send to writer */
  if (valid) {
    /* attach data to send hash */
    scr_hash_setf(send, hash, "%d", writer);
  } else {
    /* nothing to send, so delete data hash */
    scr_hash_delete(&hash);
  }

  /* gather hash to writers */
  scr_hash_exchange_direction(send, recv, scr_comm_world, SCR_HASH_EXCHANGE_LEFT);

  /* we record incoming data in save hash for writing */
  scr_hash* save = scr_hash_new();
  if (scr_my_rank_world == writer) {
    /* record hash containing file names indexed by rank,
     * attach received hash to save hash and set recv to NULL
     * since we no longer need to free it */
    scr_hash_set(save, SCR_SUMMARY_6_KEY_RANK, recv);
    recv = NULL;
  }

  /* free send and receive hashes */
  scr_hash_delete(&recv);
  scr_hash_delete(&send);

  /* create file name for rank2file index */
  scr_path* rank2file_path = scr_path_dup(dataset_path);
  if (ranks < scr_ranks_world) {
    /* for all lower level maps we append a level id and writer id */
    scr_path_append_strf(rank2file_path, "rank2file.%d.%d.scr", level, writer);
  } else {
    /* at the top most level, simplify the name so we can find it */
    scr_path_append_str(rank2file_path, "rank2file.scr");
  }
  char* rank2file_file = scr_path_strdup(rank2file_path);

  /* call gather recursively if there's another level */
  if (ranks < scr_ranks_world) {
     /* gather file names to higher level */
     unsigned long newoffset = 0;
     int newlevel = level + 1;
     int newvalid = 0;
     if (scr_my_rank_world == writer) {
       newvalid = 1;
     }
     if (scr_flush_summary_map(dataset_path, rank2file_path, newoffset, newlevel, newvalid)
         != AXL_SUCCESS)
     {
       rc = AXL_FAILURE;
     }
  }

  /* write hash to file */
  if (scr_my_rank_world == writer) {
    /* open the file if we need to */
    mode_t mode = scr_getmode(1, 1, 0);
    int fd = scr_open(rank2file_file, O_WRONLY | O_CREAT | O_TRUNC, mode);
    if (fd >= 0) {
      /* store level value in hash */
      scr_hash_util_set_int(save, SCR_SUMMARY_6_KEY_LEVEL, level);

      /* record total number within each level */
      scr_hash_util_set_int(save, SCR_SUMMARY_6_KEY_RANKS, ranks);

      /* persist hash */
      void* buf = NULL;
      size_t bufsize = 0;
      scr_hash_write_persist(&buf, &bufsize, save);

      /* write data to file */
      scr_lseek(rank2file_file, fd, offset, SEEK_SET);
      ssize_t write_rc = scr_write(rank2file_file, fd, buf, bufsize);
      if (write_rc < 0) {
        rc = AXL_FAILURE;
      }

      /* free the buffer holding the persistent hash */
      scr_free(&buf);

      /* close the file */
      scr_close(rank2file_file, fd);
    } else {
      scr_err("Opening file for write: %s @ %s:%d",
        rank2file_file, __FILE__, __LINE__
      );
      rc = AXL_FAILURE;
    }
  }

  /* free path and file name */
  scr_free(&rank2file_file);
  scr_path_delete(&rank2file_path);

  /* free the hash */
  scr_hash_delete(&save);

  return rc;
}

/* write summary file for flush */
static int scr_flush_summary(
  const scr_dataset* dataset,
  const scr_hash* file_list,
  scr_hash* data)
{
  int rc = AXL_SUCCESS;

  /* TODO: need to determine whether everyone flushed successfully */
  int all_complete = 1;

  /* define path to metadata directory */
  char* dataset_path_str = scr_dataset_metadir(dataset);
  scr_path* dataset_path = scr_path_from_str(dataset_path_str);
  scr_path_reduce(dataset_path);
  scr_free(&dataset_path_str);

  /* pick our writer so that we send roughly 1MB of data to each */
  int writer, ranks;
  unsigned long pack_size = (unsigned long) scr_hash_pack_size(data);
  scr_flush_pick_writer(1, pack_size, &writer, &ranks);

  /* create send and receive hashes */
  scr_hash* send = scr_hash_new();
  scr_hash* recv = scr_hash_new();

  /* copy data into send hash (note we don't have to delete temp since
   * we attach it to the send hash here */
  scr_hash* temp = scr_hash_new();
  scr_hash_merge(temp, data);
  scr_hash_setf(send, temp, "%d", writer);

  /* gather hashes to writers */
  scr_hash_exchange_direction(send, recv, scr_comm_world, SCR_HASH_EXCHANGE_LEFT);

  /* persist received hash */
  scr_hash* save = scr_hash_new();
  if (scr_my_rank_world == writer) {
    /* record hash containing file names indexed by rank,
     * attach received hash to save hash and set recv to NULL
     * since we no longer need to free it */
    scr_hash_set(save, SCR_SUMMARY_6_KEY_RANK, recv);
    recv = NULL;
  }

  /* free our hash data */
  scr_free(&recv);
  scr_free(&send);

  /* rank 0 creates summary file and writes dataset info */
  if (scr_my_rank_world == 0) {
    /* build file name to summary file */
    scr_path* summary_path = scr_path_dup(dataset_path);
    scr_path_append_str(summary_path, "summary.scr");
    char* summary_file = scr_path_strdup(summary_path);

    /* create file and write header */
    mode_t mode = scr_getmode(1, 1, 0);
    int fd = scr_open(summary_file, O_WRONLY | O_CREAT | O_TRUNC, mode);
    if (fd < 0) {
      scr_err("Opening hash file for write: %s @ %s:%d",
        summary_file, __FILE__, __LINE__
      );
      rc = AXL_FAILURE;
    }

    /* write data to file */
    if (fd >= 0) {
      /* create an empty hash to build our summary info */
      scr_hash* summary_hash = scr_hash_new();

      /* write the summary file version number */
      scr_hash_util_set_int(summary_hash, SCR_SUMMARY_KEY_VERSION, SCR_SUMMARY_FILE_VERSION_6);

      /* mark whether the flush is complete in the summary file */
      scr_hash_util_set_int(summary_hash, SCR_SUMMARY_6_KEY_COMPLETE, all_complete);

      /* write the dataset descriptor */
      scr_hash* dataset_hash = scr_hash_new();
      scr_hash_merge(dataset_hash, dataset);
      scr_hash_set(summary_hash, SCR_SUMMARY_6_KEY_DATASET, dataset_hash);

      /* write the hash to a file */
      ssize_t write_rc = scr_hash_write_fd(summary_file, fd, summary_hash);
      if (write_rc < 0) {
        rc = AXL_FAILURE;
      }

      /* free the hash object */
      scr_hash_delete(&summary_hash);

      /* close the file */
      scr_close(summary_file, fd);
    }

    /* free the path and string of the summary file */
    scr_free(&summary_file);
    scr_path_delete(&summary_path);
  }

  /* create file name for rank2file index */
  scr_path* rank2file_path = scr_path_dup(dataset_path);
  scr_path_append_strf(rank2file_path, "rank2file.0.%d.scr", writer);
  char* rank2file_file = scr_path_strdup(rank2file_path);

  /* write map to files */
  unsigned long offset = 0;
  int level = 1;
  int valid = 0;
  if (scr_my_rank_world == writer) {
    valid = 1;
  }
  if (scr_flush_summary_map(dataset_path, rank2file_path, offset, level, valid)
      != AXL_SUCCESS)
  {
    rc = AXL_FAILURE;
  }

  /* write blocks of summary data */
  if (scr_my_rank_world == writer) {
    /* open the file if we need to */
    mode_t mode = scr_getmode(1, 1, 0);
    int fd = scr_open(rank2file_file, O_WRONLY | O_CREAT | O_TRUNC, mode);
    if (fd >= 0) {
      /* store level value in hash */
      scr_hash_util_set_int(save, SCR_SUMMARY_6_KEY_LEVEL, 0);

      /* store number of ranks at this level */
      scr_hash_util_set_int(save, SCR_SUMMARY_6_KEY_RANKS, ranks);

      /* persist and compress hash */
      void* buf;
      size_t bufsize;
      scr_hash_write_persist(&buf, &bufsize, save);

      /* write data to file */
      scr_lseek(rank2file_file, fd, offset, SEEK_SET);
      ssize_t write_rc = scr_write(rank2file_file, fd, buf, bufsize);
      if (write_rc < 0) {
        rc = AXL_FAILURE;
      }

      /* free memory */
      scr_free(&buf);

      /* close the file */
      scr_close(rank2file_file, fd);
    } else {
      scr_err( "Opening file for write: %s @ %s:%d",
        rank2file_file, __FILE__, __LINE__
      );
      rc = AXL_FAILURE;
    }
  }

  /* free path and file name */
  scr_free(&rank2file_file);
  scr_path_delete(&rank2file_path);

  /* free the hash */
  scr_hash_delete(&save);

  /* free path and file name */
  scr_path_delete(&dataset_path);

  /* determine whether everyone wrote their files ok */
  if (scr_alltrue((rc == AXL_SUCCESS))) {
    return AXL_SUCCESS;
  }
  return AXL_FAILURE;
}

/* given a dataset id that has been flushed, the list provided by scr_flush_prepare,
 * and data to include in the summary file, complete the flush by writing the summary file */
int scr_flush_complete(int id, scr_hash* file_list, scr_hash* data)
{
  int flushed = AXL_SUCCESS;

  /* get the dataset of this flush */
  scr_dataset* dataset = scr_hash_get(file_list, SCR_KEY_DATASET);

  /* write summary file */
  if (scr_flush_summary(dataset, file_list, data) != AXL_SUCCESS) {
    flushed = AXL_FAILURE;
  }

  /* update index file */
  if (scr_my_rank_world == 0) {
    /* assume the flush failed */
    int complete = 0;
    if (flushed == AXL_SUCCESS) {
      /* remember that the flush was successful */
      complete = 1;

      /* read the index file */
      scr_hash* index_hash = scr_hash_new();
      scr_index_read(scr_prefix_path, index_hash);

      /* get name of dataset */
      char* name;
      if (scr_dataset_get_name(dataset, &name) != AXL_SUCCESS) {
        scr_abort(-1, "Failed to read dataset name @ %s:%d",
          __FILE__, __LINE__
        );
      }

      /* update complete flag in index file */
      scr_index_set_dataset(index_hash, id, name, dataset, complete);

      /* if this is a checkpoint, update current to point to new dataset,
       * this must come after index_set_dataset above because set_current
       * checks that named dataset is a checkpoint */
      if (scr_dataset_is_ckpt(dataset)) {
        scr_index_set_current(index_hash, name);
      }

      /* write the index file and delete the hash */
      scr_index_write(scr_prefix_path, index_hash);
      scr_hash_delete(&index_hash);
    }
  }

  /* have rank 0 broadcast whether the entire flush succeeded,
   * including summary file and index update */
  MPI_Bcast(&flushed, 1, MPI_INT, 0, scr_comm_world);

  /* mark this dataset as flushed to the parallel file system */
  if (flushed == AXL_SUCCESS) {
    scr_flush_file_location_set(id, SCR_FLUSH_KEY_LOCATION_PFS);

    /* TODODSET: if this dataset is not a checkpoint, delete it from cache now */
#if 0
    if (! scr_dataset_is_ckpt(dataset)) {
      scr_cache_delete(map, id);
    }
#endif
  }

  return flushed;
}
