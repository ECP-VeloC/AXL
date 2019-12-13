#include <string.h>
#include <stdlib.h>
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
kvtree_elem *
axl_get_next_path(int id, kvtree_elem *elem, char **src, char **dst)
{
    /* lookup transfer info for the given id */
    kvtree* file_list;
    kvtree* files;
    kvtree* elem_hash;

    file_list = kvtree_get_kv_int(axl_file_lists, AXL_KEY_HANDLE_UID, id);
    if (!file_list) {
        return NULL;
    }

    files = kvtree_get(file_list, AXL_KEY_FILES);

    if (!elem) {
        elem = kvtree_elem_first(files);
    } else {
        elem = kvtree_elem_next(elem);
    }

    /* get hash for this file */
    elem_hash = kvtree_elem_hash(elem);

    if (src) {
        *src = kvtree_elem_key(elem);
    }

    if (dst) {
        /* get destination for this file */
        kvtree_util_get_str(elem_hash, AXL_KEY_FILE_DEST, dst);
    }

    return (elem);
}
