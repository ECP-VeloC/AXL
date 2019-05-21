#ifndef AXL_H
#define AXL_H

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
extern "C" {
#endif

#define AXL_SUCCESS (0)

#define AXL_VERSION "0.3.0"

/* Supported AXL transfer methods
 * Note that DW, BBAPI, and CPPR must be found at compile time */
typedef enum {
    AXL_XFER_NULL = 0,      /* placeholder to represent invalid value */
    AXL_XFER_DEFAULT,       /* Autodetect and use the fastest API for this node
                             * type that supports all AXL transfers.
                             */
    AXL_XFER_SYNC,          /* synchronous copy */
    AXL_XFER_ASYNC_DAEMON,  /* async daemon process */
    AXL_XFER_ASYNC_DW,      /* Cray Datawarp */
    AXL_XFER_ASYNC_BBAPI,   /* IBM Burst Buffer API */
    AXL_XFER_ASYNC_CPPR,    /* Intel CPPR */
    AXL_XFER_NATIVE,        /* Autodetect and use the native API (BBAPI, DW,
                             * etc) for this node type.  It may or may not
                             * be compatible with all AXL transfers (like
                             * shmem).  If there is no native API, fall back
                             * to AXL_XFER_DEFAULT.
                             */
} axl_xfer_t;

/* Read configuration from non-AXL-specific file
 * Also, start up vendor specific services */
int AXL_Init (const char* state_file);

/* Shutdown any vendor services */
int AXL_Finalize (void);

/* Create a transfer handle (used for 0+ files)
 * Type specifies a particular method to use
 * Name is a user/application provided string
 * Returns an ID to the transfer handle,
 * Returns -1 on error */
int AXL_Create (axl_xfer_t type, const char* name);

/* Add a file to an existing transfer handle */
int AXL_Add (int id, const char* source, const char* destination);

/* Initiate a transfer for all files in handle ID */
int AXL_Dispatch (int id);

/* Non-blocking call to test if a transfer has completed,
 * returns AXL_SUCCESS if the transfer has completed,
 * does not indicate whether transfer was successful,
 * only whether it's done */
int AXL_Test(int id);

/* BLOCKING
 * Wait for a transfer to complete,
 * and finalize the transfer */
int AXL_Wait (int id);

/* Cancel an existing transfer, must call Wait
 * on cancelled transfers, if cancelled wait returns an error */
int AXL_Cancel (int id);

/* Perform cleanup of internal data associated with ID */
int AXL_Free (int id);

/* Stop (cancel and free) all transfers,
 * useful to clean the plate when restarting */
int AXL_Stop (void);

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
} /* extern "C" */
#endif

#endif
