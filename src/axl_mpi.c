/* kvtree & everything else */
#include "axl.h"
#include "axl_internal.h"

#include "kvtree.h"
#include "kvtree_util.h"

#include "config.h"

#include "mpi.h"

int AXL_Create_comm (
  axl_xfer_t type,  /**< [IN]  - AXL transfer type (AXL_XFER_SYNC, AXL_XFER_PTHREAD, etc) */
  const char* name, 
  MPI_Comm comm)    /**< [IN]  - communicator used for coordination and flow control */
{
  MPI_Barrier(comm);
  int rc = AXL_Create(type, name);
  return rc;
}

int AXL_Dispatch_comm (
  int id,        /**< [IN]  - transfer hander ID returned from AXL_Create */
  MPI_Comm comm) /**< [IN]  - communicator used for coordination and flow control */
{
  MPI_Barrier(comm);
  int rc = AXL_Dispatch(id);
  return rc;
}

int AXL_Test_comm (
  int id,        /**< [IN]  - transfer hander ID returned from AXL_Create */
  MPI_Comm comm) /**< [IN]  - communicator used for coordination and flow control */
{
  MPI_Barrier(comm);
  int rc = AXL_Test(id);
  return rc;
}

int AXL_Wait_comm (
  int id,        /**< [IN]  - transfer hander ID returned from AXL_Create */
  MPI_Comm comm) /**< [IN]  - communicator used for coordination and flow control */
{
  MPI_Barrier(comm);
  int rc = AXL_Wait(id);
  return rc;
}

int AXL_Cancel_comm (
  int id,        /**< [IN]  - transfer hander ID returned from AXL_Create */
  MPI_Comm comm) /**< [IN]  - communicator used for coordination and flow control */
{
  MPI_Barrier(comm);
  int rc = AXL_Cancel(id);
  return rc;
}

int AXL_Free_comm (
  int id,        /**< [IN]  - transfer hander ID returned from AXL_Create */
  MPI_Comm comm) /**< [IN]  - communicator used for coordination and flow control */
{
  MPI_Barrier(comm);
  int rc = AXL_Free(id);
  return rc;
}

int AXL_Stop_comm (
  MPI_Comm comm) /**< [IN]  - communicator used for coordination and flow control */
{
  MPI_Barrier(comm);
  int rc = AXL_Stop();
  return rc;
}
