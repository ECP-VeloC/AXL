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

#ifndef AXL_INTERNAL_H
#define AXL_INTERNAL_H

#define AXL_FAILURE (1)

extern char* axl_transfer_file;
extern char* axl_flush_file;
extern int axl_flush_async_in_progress; /* tracks whether an async flush is currently underway */
extern int axl_flush_async_dataset_id; /* tracks the id of the checkpoint being flushed */

// "KEYS"
#define AXL_KEY_HANDLE_UID ("handle_uid")
#define AXL_KEY_UNAME ("uname")
#define AXL_KEY_XFER_TYPE_STR ("xfer_type_string")
#define AXL_KEY_XFER_TYPE_INT ("xfer_type_enum")
#define AXL_KEY_FLUSH_STATUS ("flush_status")
#define AXL_KEY_FILES ("files")
#define AXL_KEY_FILE_DEST ("file_dest")
#define AXL_KEY_FILE_STATUS ("file_status")
#define AXL_KEY_FILE_CRC ("file_crc")

// VENDOR "KEYS"
#define AXL_BBAPI_KEY_TRANSFERHANDLE ("BB_TransferHandle")
#define AXL_BBAPI_KEY_TRANSFERDEF ("BB_TransferDef")

// FLUSH STATUSES
#define AXL_FLUSH_STATUS_SOURCE (1)
#define AXL_FLUSH_STATUS_INPROG (2)
#define AXL_FLUSH_STATUS_DEST   (3)
#define AXL_FLUSH_STATUS_ERROR  (4)

/*
=========================================
axl_err.c functions
========================================
*/

/* print message to stdout if axl_debug is set and it is >= level */
void axl_dbg(int level, const char *fmt, ...);

/* print error message to stdout */
void scr_err(const char *fmt, ...);

/*
=========================================
axl_flush.c functions
========================================
*/

/* given a filemap and a dataset id, prepare and return a list of files to be flushed,
 * also create corresponding directories and container files */
int axl_flush_prepare(const scr_filemap* map, int id, scr_hash* file_list);

/* given a dataset id that has been flushed, the list provided by axl_flush_prepare,
 * and data to include in the summary file, complete the flush by writing the summary file */
int scr_flush_complete(int id, scr_hash* file_list, scr_hash* data);

/*
=========================================
axl_io.c functions
========================================
*/

/* open file with specified flags and mode, retry open a few times on failure */
int axl_open(const char* file, int flags, ...);

/* close file with an fsync */
int axl_close(const char* file, int fd);

/* reliable read from opened file descriptor (retries, if necessary, until hard error) */
ssize_t axl_read(const char* file, int fd, void* buf, size_t size);

/* copy a file from src to dst and calculate CRC32 in the process
 * if crc == NULL, the CRC32 value is not computed */
int axl_file_copy(const char* src_file, const char* dst_file, unsigned long buf_size, uLong* crc);

/* opens, reads, and computes the crc32 value for the given filename */
int axl_crc32(const char* filename, uLong* crc);

/*
=========================================
axl_util.c functions
========================================
*/

extern size_t axl_file_buf_size;
int axl_read_config(void);

#endif // AXL_INTERNAL_H
