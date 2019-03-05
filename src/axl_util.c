#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>

#include "axl_internal.h"

/* returns the current linux timestamp (secs + usecs since epoch) as a double */
double axl_seconds()
{
  struct timeval tv;
  gettimeofday(&tv, NULL);
  double secs = (double) tv.tv_sec + (double) tv.tv_usec / (double) 1000000.0;
  return secs;
}

size_t axl_file_buf_size;

/*
 * Diff the contents of two files.
 *
 * Return 0 if the file contents are the same, 1 if they differ.
 */
static int axl_compare_file_contents(char *file1, char *file2)
{
    FILE *fp1 = NULL, *fp2 = NULL;
    char buf1[4096], buf2[4096];
    size_t rc1, rc2;
    int rc = 1;

    fp1 = fopen(file1, "r");
    if (!fp1) {
        goto end;
    }

    fp2 = fopen(file2, "r");
    if (!fp2) {
        goto end;
    }

    while (1) {
        rc1 = fread(buf1, sizeof(*buf1), 1, fp1);
        if (rc1 < 0) {
            break;
        }

        rc2 = fread(buf2, sizeof(*buf2), 1, fp2);
        if (rc2 < 0) {
            break;
        }

        if (rc1 != rc2) {
            break;
        }

        /* Are we done reading the files? */
        if (rc1 == 0) {
            /* All done, success */
            rc = 0;
            break;
        }

        /* Did the file contents match? */
        if (memcmp(buf1, buf2, rc1) != 0) {
            break;  /* nope */
        }
    }

end:
    if (fp1) {
        fclose(fp1);
    }

    if (fp2) {
        fclose(fp2);
    }

    return rc;
}

/*
 * Compare two files or two directories.
 *
 * 1. Given two file paths, compare the file permissions and file contents to
 * make sure they're the same.
 *
 * 2. Given two directory paths, compare the directory permissions.
 *
 * Returns 0 if everything is the same, non-zero otherwise.
 */
int axl_compare_files_or_dirs(char *path1, char *path2)
{
    struct stat stat1, stat2;
    if (stat(path1, &stat1) != 0) {
        fprintf(stderr, "Couldn't stat %s: %s\n", path1, strerror(errno));
        return errno;
    }
    if (stat(path1, &stat2) != 0) {
        fprintf(stderr, "Couldn't stat %s: %s\n", path2, strerror(errno));
        return errno;
    }
    if (!((S_ISREG(stat1.st_mode) && S_ISREG(stat2.st_mode)) ||
        (S_ISDIR(stat1.st_mode) && S_ISDIR(stat2.st_mode)))) {
        fprintf(stderr, "%s and %s should be either both files, or both dirs\n",
            path1, path2);
        return EINVAL;
    }

    if (stat1.st_size != stat2.st_size) {
        fprintf(stderr, "%s and %s have different sizes (%zu and %zu)\n",
            path1, path2, stat1.st_size, stat2.st_size);
        return EINVAL;
    }

    /* We were passed two files, compare their contents. */
    if (S_ISREG(stat1.st_mode) && S_ISREG(stat2.st_mode)) {
        if (axl_compare_file_contents(path1, path2)) {
            fprintf(stderr, "%s and %s file contents differed\n",
                path1, path2);
            return EINVAL;
        }
    }

    return 0;
}

/* caller really passes in a void**, but we define it as just void* to avoid printing
 * a bunch of warnings */
void axl_free(void* p) {
    /* verify that we got a valid pointer to a pointer */
    if (p != NULL) {
        /* free memory if there is any */
        void* ptr = *(void**)p;
        if (ptr != NULL) {
            free(ptr);
        }

        /* set caller's pointer to NULL */
        *(void**)p = NULL;
    }
}
