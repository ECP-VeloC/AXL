#include <string.h>
#include <stdlib.h>
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

int axl_read_config (char **cntl_dir) {

    axl_file_buf_size = (size_t) 1048576;
    *cntl_dir = strdup("/tmp");

    /* set file copy buffer size (file chunk size) */
    /*
    if ((value = axl_param_get("AXL_FILE_BUF_SIZE")) != NULL) {
        if (axl_abtoull(value, &ull) == AXL_SUCCESS) {
            axl_file_buf_size = (size_t) ull;
        } else {
            scr_err("Failed to read SCR_FILE_BUF_SIZE successfully @ %s:%d",
                    __FILE__, __LINE__
                    );
        }
    }
    */
    return AXL_SUCCESS;
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
