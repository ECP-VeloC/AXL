#include "axl_internals.h"
#include "kvtree.h"

/*
=========================================
Flush file functions
=========================================
*/

/* returns true if the given dataset id needs to be flushed */
int axl_flush_file_need_flush(int id)
{
  int need_flush = 0;

  // TODO: Clean up parallelism

    /* read the flush file */
    kvtree_hash* hash = kvtree_hash_new();
    kvtree_hash_read_path(axl_flush_file, hash);

    /* if we have the dataset in cache, but not on the parallel file system,
     * then it needs to be flushed */
    kvtree_hash* dset_hash = kvtree_hash_get_kv_int(hash, AXL_FLUSH_KEY_DATASET, id);
    kvtree_hash* in_cache = kvtree_hash_get_kv(dset_hash, AXL_FLUSH_KEY_LOCATION, AXL_FLUSH_KEY_LOCATION_CACHE);
    kvtree_hash* in_pfs   = kvtree_hash_get_kv(dset_hash, AXL_FLUSH_KEY_LOCATION, AXL_FLUSH_KEY_LOCATION_PFS);
    if (in_cache != NULL && in_pfs == NULL) {
      need_flush = 1;
    }

    /* free the hash object */
    kvtree_hash_delete(&hash);

  return need_flush;
}

/* checks whether the specified dataset id is currently being flushed */
int axl_flush_file_is_flushing(int id)
{
  /* assume we are not flushing this checkpoint */
  int is_flushing = 0;

  // TODO: Cleas up paralleism

    /* read flush file into hash */
    kvtree_hash* hash = kvtree_hash_new();
    kvtree_hash_read_path(axl_flush_file, hash);

    /* attempt to look up the FLUSHING state for this checkpoint */
    kvtree_hash* dset_hash = kvtree_hash_get_kv_int(hash, AXL_FLUSH_KEY_DATASET, id);
    kvtree_hash* flushing_hash = kvtree_hash_get_kv(dset_hash, AXL_FLUSH_KEY_LOCATION, AXL_FLUSH_KEY_LOCATION_FLUSHING);
    if (flushing_hash != NULL) {
      is_flushing = 1;
    }

    /* delete the hash */
    kvtree_hash_delete(&hash);

  return is_flushing;
}

/* removes entries in flush file for given dataset id */
int axl_flush_file_dataset_remove(int id)
{
  // TODO: Clean up parallelism

    /* read the flush file into hash */
    kvtree_hash* hash = kvtree_hash_new();
    kvtree_hash_read_path(axl_flush_file, hash);

    /* delete this dataset id from the flush file */
    kvtree_hash_unset_kv_int(hash, AXL_FLUSH_KEY_DATASET, id);

    /* write the hash back to the flush file */
    kvtree_hash_write_path(axl_flush_file, hash);

    /* delete the hash */
    kvtree_hash_delete(&hash);
  }
  return AXL_SUCCESS;
}

/* adds a location for the specified dataset id to the flush file */
int axl_flush_file_location_set(int id, const char* location)
{

  //TODO: Clean up parallelism

    /* read the flush file into hash */
    kvtree_hash* hash = kvtree_hash_new();
    kvtree_hash_read_path(axl_flush_file, hash);

    /* set the location for this dataset */
    kvtree_hash* dset_hash = kvtree_hash_set_kv_int(hash, AXL_FLUSH_KEY_DATASET, id);
    kvtree_hash_set_kv(dset_hash, AXL_FLUSH_KEY_LOCATION, location);

    /* write the hash back to the flush file */
    kvtree_hash_write_path(axl_flush_file, hash);

    /* delete the hash */
    kvtree_hash_delete(&hash);
  }
  return AXL_SUCCESS;
}

/* returns AXL_SUCCESS if specified dataset id is at specified location */
int axl_flush_file_location_test(int id, const char* location)
{
 // TODO: Clean up parallelism
  int at_location = 0;

    /* read the flush file into hash */
    kvtree_hash* hash = kvtree_hash_new();
    kvtree_hash_read_path(axl_flush_file, hash);

    /* check the location for this dataset */
    kvtree_hash* dset_hash = kvtree_hash_get_kv_int(hash, AXL_FLUSH_KEY_DATASET, id);
    kvtree_hash* value     = kvtree_hash_get_kv(dset_hash, AXL_FLUSH_KEY_LOCATION, location);
    if (value != NULL) {
      at_location = 1;
    }

    /* delete the hash */
    kvtree_hash_delete(&hash);

  if (! at_location) {
    return AXL_FAILURE;
  }
  return AXL_SUCCESS;
}

/* removes a location for the specified dataset id from the flush file */
int axl_flush_file_location_unset(int id, const char* location)
{

  // TODO: Clean up parallelism

    /* read the flush file into hash */
    kvtree_hash* hash = kvtree_hash_new();
    kvtree_hash_read_path(axl_flush_file, hash);

    /* unset the location for this dataset */
    kvtree_hash* dset_hash = kvtree_hash_get_kv_int(hash, AXL_FLUSH_KEY_DATASET, id);
    kvtree_hash_unset_kv(dset_hash, AXL_FLUSH_KEY_LOCATION, location);

    /* write the hash back to the flush file */
    kvtree_hash_write_path(axl_flush_file, hash);

    /* delete the hash */
    kvtree_hash_delete(&hash);

  return AXL_SUCCESS;
}

/* create an entry in the flush file for a dataset for scavenge,
 * including name, location, and flags */
int axl_flush_file_new_entry(int id, const char* name, const char* location, int ckpt, int output)
{

  // TODO: Clean up parallelism

    /* read the flush file into hash */
    kvtree_hash* hash = kvtree_hash_new();
    kvtree_hash_read_path(axl_flush_file, hash);

    /* set the name, location, and flags for this dataset */
    kvtree_hash* dset_hash = kvtree_hash_set_kv_int(hash, AXL_FLUSH_KEY_DATASET, id);
    kvtree_hash_util_set_str(dset_hash, AXL_FLUSH_KEY_NAME, name);
    kvtree_hash_util_set_str(dset_hash, AXL_FLUSH_KEY_LOCATION, location);
    if (ckpt) {
      kvtree_hash_util_set_int(dset_hash, AXL_FLUSH_KEY_CKPT, ckpt);
    }
    if (output) {
      kvtree_hash_util_set_int(dset_hash, AXL_FLUSH_KEY_OUTPUT, output);
    }

    /* write the hash back to the flush file */
    kvtree_hash_write_path(axl_flush_file, hash);

    /* delete the hash */
    kvtree_hash_delete(&hash);

  return AXL_SUCCESS;
}
