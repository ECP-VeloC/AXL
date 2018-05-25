#ifndef AXL_YAL_H
#define AXL_YAL_H

#include "fu_filemap.h"

#define AXL_MAJOR "0"
#define AXL_MINOR "0"
#define AXL_PATCH "1"
#define AXL_VERSION "0.0.1"
#define AXL_SUCCESS (0)

/* do a synchronous flush operation */
int axl_flush_sync(fu_filemap* map, int id);

/* stop all ongoing asynchronous flush operations */
int axl_flush_async_stop(void);

/* start an asynchronous flush */
int axl_flush_async_start(fu_filemap* map, int id);

/* check whether the flush from cache to parallel file system has completed */
int axl_flush_async_test(fu_filemap* map, int id, double* bytes);

/* complete the flush from cache to parallel file system */
int axl_flush_async_complete(fu_filemap* map, int id);

/* wait until the checkpoint currently being flushed completes */
int axl_flush_async_wait(fu_filemap* map);

/* initialize the async transfer processes */
int axl_flush_async_init(void);

/* finalize the async transfer processes */
int axl_flush_async_finalize(void);

#endif // AXL_YAL_H
