#ifndef AXL_H
#define AXL_H

#define AXL_SUCCESS (0)

#define AXL_MAJOR "0"
#define AXL_MINOR "0"
#define AXL_PATCH "1"
#define AXL_VERSION "0.0.1"

/* Supported AXL transfer methods
 * Note that DW, BBAPI, and CPPR must be found at compile time */
typedef enum {
    AXL_XFER_SYNC,
    AXL_XFER_ASYNC_DAEMON,
    AXL_XFER_ASYNC_DW,
    AXL_XFER_ASYNC_BBAPI,
    AXL_XFER_ASYNC_CPPR,
} axl_xfer_t;

/* Read configuration from non-AXL-specific file
 * Also, start up vendor specific services */
int AXL_Init (const char* conf_file);

/* Shutdown any vendor services */
int AXL_Finalize (void);

/* Create a transfer handle (used for 0+ files)
 * Type specifies a particular method to use
 * Name is a user/application provided string
 * Returns an ID to the transfer handle,
 * Returns -1 on error */
int AXL_Create (const char* type, const char* name);

/* Add a file to an existing transfer handle */
int AXL_Add (int id, const char* source, const char* destination);

/* Initiate a transfer for all files in handle ID */
int AXL_Dispatch (int id);

/* Non-blocking call to test if a transfer has completed,
 * returns 1 if the transfer has completed,
 * does not indicate whether transfer was successful,
 * only whether it's done */
int AXL_Test(int id);

/* BLOCKING
 * Wait for a transfer to complete,
 * and finalize the transfer */
int AXL_Wait (int id);

/* Cancel an existing transfer */
/* TODO: Does cancel call free? */
int AXL_Cancel (int id);

/* Perform cleanup of internal data associated with ID */
int AXL_Free (int id);

#endif
