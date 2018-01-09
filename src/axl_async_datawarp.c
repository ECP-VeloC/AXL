/*
 * Copyright (c) 2009, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by Adam Moody <moody20@llnl.gov>.
 * LLNL-CODE-411039.
 * All rights reserved.
 * This file was originally part of The Scalable Checkpoint / Restart (SCR) library.
 * For details, see https://sourceforge.net/projects/scalablecr/
 * Please also read this file: LICENSE.TXT.
*/

#include "axl_async_datawarp.h"
#include "kvtree.h"
#include "fu_meta.h"
#include "axl_internals.h"

#ifdef HAVE_DATAWARP
#include "datawarp.h"
#endif

int axl_flush_async_start_datawarp(fu_filemap* map, int id){
#ifdef HAVE_DATAWARP
  /* For each file figure out where it goes */
  kvtree_elem* elem;
  kvtree* files = kvtree_get(axl_flush_async_file_list, AXL_KEY_FILE);
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
    if (kvtree_util_get_str(file_hash, AXL_KEY_PATH, &dest_dir) != AXL_SUCCESS) {
      continue;
    }

    /* use file name to complete destination file name */
    char* filename = strrchr(file, '/'); // includes '/'
    char* dest_file = malloc(strlen(dest_dir) + strlen(filename) + 1);
    strcpy(dest_file, dest_dir);
    strcat(dest_file, filename);

    /* get meta data for file */
    fu_meta* meta = kvtree_get(file_hash, AXL_KEY_META);

    /* get the file size */
    unsigned long filesize = 0;
    if (fu_meta_get_filesize(meta, &filesize) != AXL_SUCCESS) {
      continue;
    }

    /* Stage out file */
    int stage_out = dw_stage_file_out(file, dest_file, DW_STAGE_IMMEDIATE);
    if(stage_out != 0){
      axl_abort(-1, "Datawarp stage file out failed with error %d @ %s:%d",
                -stage_out, __FILE__, __LINE__
        );
    }

    axl_free(&dest_file);
  }
  return AXL_SUCCESS;
#endif
  return AXL_FAILURE;
}


int axl_flush_async_complete_datawarp(fu_filemap* map, int id){
  /* no datawarp specific code for async complete */
  return AXL_SUCCESS;
}


int axl_flush_async_stop_datawarp(fu_filemap* map, int id){
#ifdef HAVE_DATAWARP
  if (axl_flush_async_in_progress){
    kvtree_elem* elem;
    int complete = 0;
    int pending = 0;
    int deferred = 0;
    int failed = 0;
    int test_complete;
    int test_pending;
    int test_deferred;
    int test_failed;
    kvtree* files = kvtree_get(axl_flush_async_file_list, AXL_KEY_FILE);
    for (elem = kvtree_elem_first(files);
         elem != NULL;
         elem = kvtree_elem_next(elem))
    {
      char* file = kvtree_elem_key(elem);
      dw_terminate_file_stage(file);
    }
  }
  return AXL_SUCCESS;
#endif
  return AXL_FAILURE;
}


int axl_flush_async_test_datawarp(fu_filemap* map, int id){
#ifdef HAVE_DATAWARP
  /* assume transfer is complete */
  int transfer_complete = 1;

  kvtree_elem* elem;
  int complete = 0;
  int pending = 0;
  int deferred = 0;
  int failed = 0;
  int test_complete;
  int test_pending;
  int test_deferred;
  int test_failed;
  kvtree* files = kvtree_get(axl_flush_async_file_list, AXL_KEY_FILE);
  for (elem = kvtree_elem_first(files);
       elem != NULL;
       elem = kvtree_elem_next(elem))
  {
    char* file = kvtree_elem_key(elem);
    int test = dw_query_file_stage(file, &test_complete, &test_pending, &test_deferred,
                                   &test_failed);
    if(test == 0){
      complete += test_complete;
      pending += test_pending;
      deferred += test_deferred;
      failed += test_failed;
    }else{
      axl_abort(-1, "Datawarp failed with error %d @ %s:%d",
                -test, __FILE__, __LINE__
		    );
    }
  }
  if(failed != 0){
    axl_abort(-1, "Datawarp failed while flushing dataset %d @ %s:%d",
              id, __FILE__, __LINE__
		  );
  }else if(pending != 0 || deferred != 0){
    transfer_complete = 0;
  }

  if(transfer_complete){
    return AXL_SUCCESS
  }
#endif
  return AXL_FAILURE;
}


int axl_flush_async_wait_datawarp(fu_filemap* map, int id){
#ifdef HAVE_DATAWARP
  /* Get the list of files */
  kvtree_elem* elem = NULL;
  int dw_wait = 0;
  kvtree* files = kvtree_get(axl_flush_async_file_list, AXL_KEY_FILE);
  for (elem = kvtree_elem_first(files);
       elem != NULL;
       elem = kvtree_elem_next(elem))
  {
    /* Do the wait on each file */
    char* file = kvtree_elem_key(elem);
    dw_wait = dw_wait_file_stage(file);
    if (dw_wait != 0){
      axl_abort(-1, "Datawarp wait operation failed with error %d @ %s:%d",
                -dw_wait, __FILE__, __LINE__
		    );
    }
  }
  return axl_flush_async_complete(map, id, AXL_XFER_DATAWARP);
#else
  return AXL_FAILURE;
#endif
}


int axl_flush_async_init_datawarp(){
  return AXL_SUCCESS;
}

int axl_flush_async_finalize_datawarp(){
  return AXL_SUCCESS;
}
