#ifndef AXL_ASYNC_NNFDM_H
#define AXL_ASYNC_NNFDM_H

/** \file axl_async_nnfdm.h
 *  \ingroup axl
 *  \brief implementation of axl for Hpe datamoveer */

/** \name nnfdm */
///@{
void axl_async_init_nnfdm();
void axl_async_finalize_nnfdm();
int axl_async_start_nnfdm(int id);
int axl_async_test_nnfdm(int id);
int axl_async_wait_nnfdm(int id);
///@}
#endif //AXL_ASYNC_NNFDM_H
