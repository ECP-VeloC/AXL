# Overview
AXL defines a common interface for transferring files between layers in a storage hierarchy.
It abstracts vendor-specific APIs and provides synchronous and asynchronous methods using POSIX.
One creates a transfer object, defining the transfer type, and then one adds files to the transfer.
Once all files have been added, one initiates the transfer and can then later test or wait for its completion.
The library optionally records the state of ongoing transfers,
so that they can be identified or terminated even if the
process that initiated the transfer has been restarted.

For an example of how to use AXL, see [test_axl_sync.c](../test/test_axl_sync.c)

For more detailed use of the API, refer to [axl.h](../src/axl.h).

# Initializing AXL
Before calling AXL funcitons, one must first initialize the library by calling AXL\_Init.
One can optionally provide the path to a file where AXL can persist its state.
If given a state file, AXL can recover its state upon restarting the process.
If no state files is needed, one may pass NULL in place of the path name.

One must call AXL\_Finalize to shut down the library.

# Transferring files
Regardless of the transfer type, the basic control flow of a transfer is always:
1. AXL\_Create - allocate a new transfer object, providing its type and a name
2. AXL\_Add - add a file to a transfer object, giving both source and destination path
3. AXL\_Dispatch - start the transfer
4. AXL\_Test - optional, non-blocking test for whether AXL\_Wait will block
5. AXL\_Cancel - optionally cancel a dispatched transfer
6. AXL\_Wait - wait for transfer to complete
7. AXL\_Free - free resources associated with transfer object allocated in AXL\_Create

AXL\_Create returns a transfer id that is the identifier
for the transfer object for most other calls.
It returns -1 if it fails to create a transfer object.
The name is meant to serve as a user-friendly string.

One may add multiple files to a transfer,
and a transfer having zero files is also valid.

One may cancel an outstanding transfer by calling AXL\_Cancel
between AXL\_Dispatch and AXL\_Wait.
One must still call AXL\_Wait on a cancelled transfer.

AXL\_Test does not indicate whether a transfer succeeded.
It indicates whether a call to AXL\_Wait will block.
One must call AXL\_Wait to identify whether a transfer succeeded or failed.

It is valid to free a transfer object if it has not been dispatched.

One can call AXL\_Stop to terminate any and all outstanding
transfers without having to know the status, identifiers, or names
of those transfers.
One can not call wait or free on transfers that were terminated
with AXL\_Stop.

If a transfer fails, partially transferred files are not removed
from the destination.

## Transfer types

* AXL\_XFER\_SYNC - this is a synchronous transfer, which does not return until the files have been fully copied.  It uses POSIX I/O to directly read/write files.

* AXL\_XFER\_PTHREAD - Like AXL\_XFER\_SYNC, but use multiple threads to do the copy.

* AXL\_XFER\_ASYNC\_BBAPI - this method uses [IBM's Burst Buffer API](https://github.com/IBM/CAST) to transfer files.  IBM's system software then takes over to move data in the background.  It's actually using NVMeoF, reading data from the local SSD from a remote node, so that the compute node is not really bothered once started.  If either the source or destination filesystems don't support the BBAPI transfers, AXL will fall back to using a AXL\_XFER\_PTHREAD transfer instead.

* AXL\_XFER\_ASYNC\_DW - this method uses [Cray's Datawarp API](https://www.cray.com/products/storage/datawarp).

* AXL\_XFER\_DEFAULT - Let AXL choose the fastest transfer type that is compatible with all VeloC transfers.  This may or may not be the node's native transfer library.

* AXL\_XFER\_NATIVE -  Use the node's native transfer library (like IBM Burst Buffer or Cray DataWarp) for transfers.  These native libraries may or may not support all VeloC transfers.
