#ifndef AXL_FLUSH_FILE_MPI_H
#define AXL_FLUSH_FILE_MPI_H

/* returns true if the given dataset id needs to be flushed */
int axl_flush_file_need_flush(int id);

/* checks whether the specified dataset id is currently being flushed */
int axl_flush_file_is_flushing(int id);

/* removes entries in flush file for given dataset id */
int axl_flush_file_dataset_remove(int id);

/* adds a location for the specified dataset id to the flush file */
int axl_flush_file_location_set(int id, const char* location);

/* returns AXL_SUCCESS if specified dataset id is at specified location */
int axl_flush_file_location_test(int id, const char* location);

/* removes a location for the specified dataset id from the flush file */
int axl_flush_file_location_unset(int id, const char* location);

/* create an entry in the flush file for a dataset for scavenge,
 * including name, location, and flags */
int axl_flush_file_new_entry(int id, const char* name, const char* location, int ckpt, int output);

#endif
