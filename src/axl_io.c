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

/* CRC and uLong */
#include <zlib.h>

/* mode bits */
#include <sys/stat.h>

/* file open modes */
#include <sys/file.h>

/* exit & malloc */
#include <stdlib.h>

// dirname and basename
#include <libgen.h>

#include <errno.h>
#include <string.h>

#include "axl_internal.h"

/* Configurations */
#ifndef AXL_OPEN_TRIES
#define AXL_OPEN_TRIES (5)
#endif

#ifndef AXL_OPEN_USLEEP
#define AXL_OPEN_USLEEP (100)
#endif

/* returns user's current mode as determine by their umask */
mode_t axl_getmode(int read, int write, int execute) {
    /* lookup current mask and set it back */
    mode_t old_mask = umask(S_IWGRP | S_IWOTH);
    umask(old_mask);

    mode_t bits = 0;
    if (read) {
        bits |= (S_IRUSR | S_IRGRP | S_IROTH);
    }
    if (write) {
        bits |= (S_IWUSR | S_IWGRP | S_IWOTH);
    }
    if (execute) {
        bits |= (S_IXUSR | S_IXGRP | S_IXOTH);
    }

    /* convert mask to mode */
    mode_t mode = bits & ~old_mask & 0777;
    return mode;
}

/* recursively create directory and subdirectories */
int axl_mkdir(const char* dir, mode_t mode) {
    int rc = AXL_SUCCESS;

    /* With dirname, either the original string may be modified or the function may return a
     * pointer to static storage which will be overwritten by the next call to dirname,
     * so we need to strdup both the argument and the return string. */

    /* extract leading path from dir = full path - basename */
    char* dircopy = strdup(dir);
    char* path    = strdup(dirname(dircopy));

    /* if we can read path or path=="." or path=="/", then there's nothing to do,
     * otherwise, try to create it */
    if (access(path, R_OK) < 0 && strcmp(path,".") != 0  && strcmp(path,"/") != 0) {
        rc = axl_mkdir(path, mode);
    }

    /* if we can write to path, try to create subdir within path */
    if (access(path, W_OK) == 0 && rc == AXL_SUCCESS) {
        int tmp_rc = mkdir(dir, mode);
        if (tmp_rc < 0) {
            if (errno == EEXIST) {
                /* don't complain about mkdir for a directory that already exists */
                axl_free(&dircopy);
                axl_free(&path);
                return AXL_SUCCESS;
            } else {
                axl_err("Creating directory: mkdir(%s, %x) path=%s errno=%d %s @ %s:%d",
                        dir, mode, path, errno, strerror(errno), __FILE__, __LINE__
                        );
                rc = AXL_FAILURE;
            }
        }
    } else {
        axl_err("Cannot write to directory: %s @ %s:%d",
                path, __FILE__, __LINE__
                );
        rc = AXL_FAILURE;
    }

    /* free our dup'ed string and return error code */
    axl_free(&dircopy);
    axl_free(&path);
    return rc;
}

/* delete a file */
int axl_file_unlink(const char* file) {
    if (unlink(file) != 0) {
        axl_dbg(2, "Failed to delete file: %s errno=%d %s @ %s:%d",
                file, errno, strerror(errno), __FILE__, __LINE__
                );
        return AXL_FAILURE;
    }
    return AXL_SUCCESS;
}

/* open file with specified flags and mode, retry open a few times on failure */
int axl_open(const char* file, int flags, ...) {
    /* extract the mode (see man 2 open) */
    int mode_set = 0;
    mode_t mode = 0;
    if (flags & O_CREAT) {
        va_list ap;
        va_start(ap, flags);
        mode = va_arg(ap, mode_t);
        va_end(ap);
        mode_set = 1;
    }

    int fd = -1;
    if (mode_set) {
        fd = open(file, flags, mode);
    } else {
        fd = open(file, flags);
    }
    if (fd < 0) {
        axl_dbg(1, "Opening file: open(%s) errno=%d %s @ %s:%d",
                file, errno, strerror(errno), __FILE__, __LINE__
                );

        /* try again */
        int tries = AXL_OPEN_TRIES;
        while (tries && fd < 0) {
            usleep(AXL_OPEN_USLEEP);
            if (mode_set) {
                fd = open(file, flags, mode);
            } else {
                fd = open(file, flags);
            }
            tries--;
        }

        /* if we still don't have a valid file, consider it an error */
        if (fd < 0) {
            axl_err("Opening file: open(%s) errno=%d %s @ %s:%d",
                    file, errno, strerror(errno), __FILE__, __LINE__
                    );
        }
    }
    return fd;
}

/* reliable read from file descriptor (retries, if necessary, until hard error) */
ssize_t axl_read(const char* file, int fd, void* buf, size_t size) {
    ssize_t n = 0;
    int retries = 10;
    while (n < size) {
        int rc = read(fd, (char*) buf + n, size - n);
        if (rc  > 0) {
            n += rc;
        } else if (rc == 0) {
            /* EOF */
            return n;
        } else { /* (rc < 0) */
            /* got an error, check whether it was serious */
            if (errno == EINTR || errno == EAGAIN) {
                continue;
            }

            /* something worth printing an error about */
            retries--;
            if (retries) {
                /* print an error and try again */
                axl_err("Error reading %s: read(%d, %x, %ld) errno=%d %s @ %s:%d",
                        file, fd, (char*) buf + n, size - n, errno, strerror(errno), __FILE__, __LINE__
                        );
            } else {
                /* too many failed retries, give up */
                axl_err("Giving up read of %s: read(%d, %x, %ld) errno=%d %s @ %s:%d",
                        file, fd, (char*) buf + n, size - n, errno, strerror(errno), __FILE__, __LINE__
                        );
                exit(1);
            }
        }
    }
    return n;
}

/* make a good attempt to read from file (retries, if necessary, return error if fail) */
ssize_t axl_read_attempt(const char* file, int fd, void* buf, size_t size) {
    ssize_t n = 0;
    int retries = 10;
    while (n < size) {
        int rc = read(fd, (char*) buf + n, size - n);
        if (rc  > 0) {
            n += rc;
        } else if (rc == 0) {
            /* EOF */
            return n;
        } else { /* (rc < 0) */
            /* got an error, check whether it was serious */
            if (errno == EINTR || errno == EAGAIN) {
                continue;
            }

            /* something worth printing an error about */
            retries--;
            if (retries) {
                /* print an error and try again */
                axl_err("Error reading file %s errno=%d %s @ %s:%d",
                        file, errno, strerror(errno), __FILE__, __LINE__
                        );
            } else {
                /* too many failed retries, give up */
                axl_err("Giving up read on file %s errno=%d %s @ %s:%d",
                        file, errno, strerror(errno), __FILE__, __LINE__
                        );
                return -1;
            }
        }
    }
    return n;
}


/* make a good attempt to write to file (retries, if necessary, return error if fail) */
ssize_t axl_write_attempt(const char* file, int fd, const void* buf, size_t size) {
    ssize_t n = 0;
    int retries = 10;
    while (n < size) {
        ssize_t rc = write(fd, (char*) buf + n, size - n);
        if (rc > 0) {
            n += rc;
        } else if (rc == 0) {
            /* something bad happened, print an error and abort */
            axl_err("Error writing file %s write returned 0 @ %s:%d",
                    file, __FILE__, __LINE__
                    );
            return -1;
        } else { /* (rc < 0) */
            /* got an error, check whether it was serious */
            if (errno == EINTR || errno == EAGAIN) {
                continue;
            }

            /* something worth printing an error about */
            retries--;
            if (retries) {
                /* print an error and try again */
                axl_err("Error writing file %s errno=%d %s @ %s:%d",
                        file, errno, strerror(errno), __FILE__, __LINE__
                        );
            } else {
                /* too many failed retries, give up */
                axl_err("Giving up write of file %s errno=%d %s @ %s:%d",
                        file, errno, strerror(errno), __FILE__, __LINE__
                        );
                return -1;
            }
        }
    }
    return n;
}

/* fsync and close file */
int axl_close(const char* file, int fd) {
    /* fsync first */
    if (fsync(fd) < 0) {
        /* print warning that fsync failed */
        axl_dbg(2, "Failed to fsync file descriptor: %s errno=%d %s @ %s:%d",
                file, errno, strerror(errno), __FILE__, __LINE__
                );
    }

    /* now close the file */
    if (close(fd) != 0) {
        /* hit an error, print message */
        axl_err("Closing file descriptor %d for file %s: errno=%d %s @ %s:%d",
                fd, file, errno, strerror(errno), __FILE__, __LINE__
                );
        return AXL_FAILURE;
    }

    return AXL_SUCCESS;
}

/* TODO: could perhaps use O_DIRECT here as an optimization */
/* TODO: could apply compression/decompression here */
/* copy src_file (full path) to dest_path and return new full path in dest_file */
int axl_file_copy(const char* src_file, const char* dst_file, unsigned long buf_size, uLong* crc) {
    /* check that we got something for a source file */
    if (src_file == NULL || strcmp(src_file, "") == 0) {
        axl_err("Invalid source file @ %s:%d",
                __FILE__, __LINE__
                );
        return AXL_FAILURE;
    }

    /* check that we got something for a destination file */
    if (dst_file == NULL || strcmp(dst_file, "") == 0) {
        axl_err("Invalid destination file @ %s:%d",
                __FILE__, __LINE__
                );
        return AXL_FAILURE;
    }

    int rc = AXL_SUCCESS;

    /* open src_file for reading */
    int src_fd = axl_open(src_file, O_RDONLY);
    if (src_fd < 0) {
        axl_err("Opening file to copy: axl_open(%s) errno=%d %s @ %s:%d",
                src_file, errno, strerror(errno), __FILE__, __LINE__
                );
        return AXL_FAILURE;
    }

    /* open dest_file for writing */
    mode_t mode_file = axl_getmode(1, 1, 0);
    int dst_fd = axl_open(dst_file, O_WRONLY | O_CREAT | O_TRUNC, mode_file);
    if (dst_fd < 0) {
        axl_err("Opening file for writing: axl_open(%s) errno=%d %s @ %s:%d",
                dst_file, errno, strerror(errno), __FILE__, __LINE__
                );
        axl_close(src_file, src_fd);
        return AXL_FAILURE;
    }

#if !defined(__APPLE__)
    /* TODO:
       posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED | POSIX_FADV_SEQUENTIAL)
       that tells the kernel that you don't ever need the pages
       from the file again, and it won't bother keeping them in the page cache.
    */
    posix_fadvise(src_fd, 0, 0, POSIX_FADV_DONTNEED | POSIX_FADV_SEQUENTIAL);
    posix_fadvise(dst_fd, 0, 0, POSIX_FADV_DONTNEED | POSIX_FADV_SEQUENTIAL);
#endif

    /* allocate buffer to read in file chunks */
    char* buf = (char*) malloc(buf_size);
    if (buf == NULL) {
        axl_err("Allocating memory: malloc(%llu) errno=%d %s @ %s:%d",
                buf_size, errno, strerror(errno), __FILE__, __LINE__
                );
        axl_close(dst_file, dst_fd);
        axl_close(src_file, src_fd);
        return AXL_FAILURE;
    }

    /* initialize crc values */
    if (crc != NULL) {
        *crc = crc32(0L, Z_NULL, 0);
    }

    /* write chunks */
    int copying = 1;
    while (copying) {
        /* attempt to read buf_size bytes from file */
        int nread = axl_read_attempt(src_file, src_fd, buf, buf_size);

        /* if we read some bytes, write them out */
        if (nread > 0) {
            /* optionally compute crc value as we go */
            if (crc != NULL) {
                *crc = crc32(*crc, (const Bytef*) buf, (uInt) nread);
            }

            /* write our nread bytes out */
            int nwrite = axl_write_attempt(dst_file, dst_fd, buf, nread);

            /* check for a write error or a short write */
            if (nwrite != nread) {
                /* write had a problem, stop copying and return an error */
                copying = 0;
                rc = AXL_FAILURE;
            }
        }

        /* assume a short read means we hit the end of the file */
        if (nread < buf_size) {
            copying = 0;
        }

        /* check for a read error, stop copying and return an error */
        if (nread < 0) {
            /* read had a problem, stop copying and return an error */
            copying = 0;
            rc = AXL_FAILURE;
        }
    }

    /* free buffer */
    axl_free(&buf);

    /* close source and destination files */
    if (axl_close(dst_file, dst_fd) != AXL_SUCCESS) {
        rc = AXL_FAILURE;
    }
    if (axl_close(src_file, src_fd) != AXL_SUCCESS) {
        rc = AXL_FAILURE;
    }

    /* unlink the file if the copy failed */
    if (rc != AXL_SUCCESS) {
        unlink(dst_file);
    }

    return rc;
}

/* opens, reads, and computes the crc32 value for the given filename */
int axl_crc32(const char* filename, uLong* crc) {
    /* check that we got a variable to write our answer to */
    if (crc == NULL) {
        return AXL_FAILURE;
    }

    /* initialize our crc value */
    *crc = crc32(0L, Z_NULL, 0);

    /* open the file for reading */
    int fd = axl_open(filename, O_RDONLY);
    if (fd < 0) {
        axl_dbg(1, "Failed to open file to compute crc: %s errno=%d @ %s:%d",
                filename, errno, __FILE__, __LINE__
                );
        return AXL_FAILURE;
    }

    /* read the file data in and compute its crc32 */
    int nread = 0;
    unsigned long buffer_size = 1024*1024;
    char buf[buffer_size];
    do {
        nread = axl_read(filename, fd, buf, buffer_size);
        if (nread > 0) {
            *crc = crc32(*crc, (const Bytef*) buf, (uInt) nread);
        }
    } while (nread == buffer_size);

    /* if we got an error, don't print anything and bailout */
    if (nread < 0) {
        axl_dbg(1, "Error while reading file to compute crc: %s @ %s:%d",
                filename, __FILE__, __LINE__
                );
        close(fd);
        return AXL_FAILURE;
    }

    /* close the file */
    axl_close(filename, fd);

    return AXL_SUCCESS;
}
