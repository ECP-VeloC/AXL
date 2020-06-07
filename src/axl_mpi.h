#ifndef AXL_MPI_H
#define AXL_MPI_H

#include "axl.h"
#include "mpi.h"

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
extern "C" {
#endif

/** \defgroup axl AXL
 *  \brief Asynchronous Transfer Library
 *
 * For MPI jobs in which multiple processes issue transfes simultaneously,
 * communicators can be used to optimize file I/O operations.  This extends
 * the AXL interface to work with a communicator.  One must provide the same
 * group of processes and in the same order as used in the communicator to
 * create the transfer handle. */

/** \file axl_mpi.h
 *  \ingroup axl
 *  \brief asynchronous transfer library for MPI communicators */

int AXL_Create_comm (
  axl_xfer_t type,  /**< [IN]  - AXL transfer type (AXL_XFER_SYNC, AXL_XFER_PTHREAD, etc) */
  const char* name, 
  MPI_Comm comm     /**< [IN]  - communicator used for coordination and flow control */
);

int AXL_Dispatch_comm (
  int id,       /**< [IN]  - transfer hander ID returned from AXL_Create */
  MPI_Comm comm /**< [IN]  - communicator used for coordination and flow control */
);

int AXL_Test_comm (
  int id,       /**< [IN]  - transfer hander ID returned from AXL_Create */
  MPI_Comm comm /**< [IN]  - communicator used for coordination and flow control */
);

int AXL_Wait_comm (
  int id,       /**< [IN]  - transfer hander ID returned from AXL_Create */
  MPI_Comm comm /**< [IN]  - communicator used for coordination and flow control */
);

int AXL_Cancel_comm (
  int id,       /**< [IN]  - transfer hander ID returned from AXL_Create */
  MPI_Comm comm /**< [IN]  - communicator used for coordination and flow control */
);

int AXL_Free_comm (
  int id,       /**< [IN]  - transfer hander ID returned from AXL_Create */
  MPI_Comm comm /**< [IN]  - communicator used for coordination and flow control */
);

int AXL_Stop_comm (
  MPI_Comm comm /**< [IN]  - communicator used for coordination and flow control */
);

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* AXL_MPI_H */
