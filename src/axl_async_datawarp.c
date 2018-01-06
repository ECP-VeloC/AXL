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

#ifdef HAVE_DATAWARP
#include "datawarp.h"
#endif

int axl_flush_async_test_datawarp(fu_filemap* map, int id, double bytes){
  kvtree_elem* elem;
  int complete = 0;
  int pending = 0;
  int deferred = 0;
  int failed = 0;
  int test_complete;
  int test_pending;
  int test_deferred;
  int test_failed;
  kvtree* files = kvtree_get(scr_flush_async_file_list, AXL_KEY_FILE);
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
}


int axl_flush_async_wait_datawarp(fu_filemap* map, int id){
  /* Get the list of files */
  kvtree_elem* elem = NULL;
  int dw_wait = 0;
  kvtree* files = kvtree_get(scr_flush_async_file_list, AXL_KEY_FILE);
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
  axl_flush_async_complete(map, id, AXL_XFER_DATAWARP);
}


int axl_flush_async_init_datawarp(){
  return AXL_SUCCESS;
}

int axl_flush_async_finalize_datawarp(){
  return AXL_SUCCESS;
}
