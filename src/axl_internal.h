#ifndef AXL_INTERNAL_H
#define AXL_INTERNAL_H

#include <zlib.h>
#include "kvtree.h"

#define AXL_SUCCESS (0)
#define AXL_FAILURE (-1)

extern char* axl_flush_file;

/* AXL internal data structure
 * this structure is accessed by the each transfer interface
 * any changes of the existing structure must be documented */
extern kvtree* axl_file_lists;

/* "KEYS" */
#define AXL_KEY_HANDLE_UID ("handle_uid")
#define AXL_KEY_UNAME ("uname")
#define AXL_KEY_XFER_TYPE_STR ("xfer_type_string")
#define AXL_KEY_XFER_TYPE_INT ("xfer_type_enum")
#define AXL_KEY_STATUS ("status")
#define AXL_KEY_FILES ("files")
#define AXL_KEY_FILE_DEST ("file_dest")
#define AXL_KEY_FILE_STATUS ("file_status")
#define AXL_KEY_FILE_CRC ("file_crc")

/* TRANSFER STATUS */
#define AXL_STATUS_SOURCE (1)
#define AXL_STATUS_INPROG (2)
#define AXL_STATUS_DEST   (3)
#define AXL_STATUS_ERROR  (4)

/*
=========================================
axl_err.c functions
Note: these functions are taken directly from SCR
========================================
*/

/* print message to stdout if axl_debug is set and it is >= level */
void axl_dbg(int level, const char *fmt, ...);

/* print error message to stdout */
void axl_err(const char *fmt, ...);

/* print abort message and kill run */
void axl_abort(int rc, const char *fmt, ...);

/*
=========================================
axl_io.c functions
Note: these functions are taken directly from SCR
========================================
*/

/* returns user's current mode as determine by their umask */
mode_t axl_getmode(int read, int write, int execute);

/* recursively create directory and subdirectories */
int axl_mkdir(const char* dir, mode_t mode);

/* delete a file */
int axl_file_unlink(const char* file);

/* open file with specified flags and mode, retry open a few times on failure */
int axl_open(const char* file, int flags, ...);

/* close file with an fsync */
int axl_close(const char* file, int fd);

/* reliable read from opened file descriptor (retries, if necessary, until hard error) */
ssize_t axl_read(const char* file, int fd, void* buf, size_t size);

/* make a good attempt to read to file (retries, if necessary, return error if fail) */
ssize_t axl_read_attempt(const char* file, int fd, void* buf, size_t size);

/* make a good attempt to write to file (retries, if necessary, return error if fail) */
ssize_t axl_write_attempt(const char* file, int fd, const void* buf, size_t size);

/* copy a file from src to dst and calculate CRC32 in the process
 * if crc == NULL, the CRC32 value is not computed */
int axl_file_copy(const char* src_file, const char* dst_file, unsigned long buf_size, uLong* crc);

/* opens, reads, and computes the crc32 value for the given filename */
int axl_crc32(const char* filename, uLong* crc);

/* given a filename, return number of bytes in file */
unsigned long axl_file_size(const char* file);

/*
=========================================
axl_util.c functions
========================================
*/

/* returns the current linux timestamp (secs + usecs since epoch) as a double */
double axl_seconds();

extern size_t axl_file_buf_size;
int axl_read_config(char **cntl_dir);

void axl_free(void* p);

#endif /* AXL_INTERNAL_H */
