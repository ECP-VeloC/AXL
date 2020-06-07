#ifndef AXL_INTERNAL_H
#define AXL_INTERNAL_H

/* pick up cmake compile macros */
#include "config.h"

#include <zlib.h>
#include "kvtree.h"
#include "kvtree_util.h"

#define AXL_SUCCESS (0)
#define AXL_FAILURE (-1)

/* AXL internal data structure
 * this structure is accessed by the each transfer interface
 * any changes of the existing structure must be documented */
extern kvtree* axl_file_lists;

/* current debug level for AXL library,
 * set in AXL_Init used in axl_dbg */
extern int axl_debug;

/* flag to track whether file metadata should also be copied,
 * which includes uid/gid, permission bits, and timestamps */
extern int axl_copy_metadata;

/* "KEYS" */
#define AXL_KEY_HANDLE_UID    ("ID")
#define AXL_KEY_UNAME         ("NAME")
#define AXL_KEY_XFER_TYPE     ("TYPE")
#define AXL_KEY_STATE         ("STATE")
#define AXL_KEY_STATUS        ("STATUS")
#define AXL_KEY_FILES         ("FILE")
#define AXL_KEY_FILE_DEST     ("DEST")
#define AXL_KEY_FILE_STATUS   ("STATUS")
#define AXL_KEY_FILE_CRC      ("CRC")

/* TRANSFER STATUS */
#define AXL_STATUS_SOURCE (1)
#define AXL_STATUS_INPROG (2)
#define AXL_STATUS_DEST   (3)
#define AXL_STATUS_ERROR  (4)

/* attaches function name, file name, and line number to error messages
 * https://gcc.gnu.org/onlinedocs/cpp/Variadic-Macros.html */
#define AXL_ERR(format, ...) axl_err(format " @ %s %s:%d", ##__VA_ARGS__, __func__, __FILE__, __LINE__)
#define AXL_DBG(level, format, ...) axl_dbg(level, format " @ %s %s:%d", ##__VA_ARGS__, __func__, __FILE__, __LINE__)

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

#ifdef HAVE_BBAPI
/*
 * Returns 1 if the filesystem for a particular file supports the fiemap ioctl
 * (the filesystem for the file is able to report extents).
 * Returns 0 if extents are not supported.
 *
 * For example, tmpfs and ext4 support extents, NFS does not.
 */
int axl_file_supports_fiemap(char *path);
#endif

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

int axl_compare_files_or_dirs(char *path1, char *path2);
void axl_free(void* p);


/*
 * This is an helper function to iterate though a file list for a given
 * AXL ID.  Usage:
 *
 *    char *src;
 *    char *dst;
 *    char *kvtree_elem *elem = NULL;
 *
 *    while ((elem = axl_get_next_path(id, elem, &src, &dst))) {
 *        printf("src %s, dst %s\n", src, dst);
 *    }
 *
 *    src or dst can be set to NULL if you don't care about the value.
 */
kvtree_elem * axl_get_next_path(int id, kvtree_elem *elem, char **src,
    char **dst);

/* given a source file, record its current uid/gid, permissions,
 * and timestamps, record them in provided kvtree */
int axl_meta_encode(const char* file, kvtree* meta);

/* copy metadata settings recorded in provided kvtree to specified file */
int axl_meta_apply(const char* file, const kvtree* meta);

#endif /* AXL_INTERNAL_H */
