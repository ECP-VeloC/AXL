/*
 * Copyright (c) 2009, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by Elsa Gonsiorowski <gonsie@llnl.gov>.
 * LLNL-CODE-411039.
 * All rights reserved.
 * This file is part of The Scalable Checkpoint / Restart (SCR) library.
 * For details, see https://sourceforge.net/projects/scalablecr/
 * Please also read this file: LICENSE.TXT.
*/

#ifndef AXL_ASYNC_BBAPI_H
#define AXL_ASYNC_BBAPI_H

#define AXL_BBAPI_KEY_TRANSFERHANDLE ("BB_TransferHandle")
#define AXL_BBAPI_KEY_TRANSFERDEF ("BB_TransferDef")

int axl_flush_async_start_bbapi(int id);
int axl_flush_async_complete_bbapi(int id);
int axl_flush_async_stop_bbapi(int id);
int axl_flush_async_test_bbapi(int id);
int axl_flush_async_wait_bbapi(int id);
int axl_flush_async_create_bbapi(int id);
int axl_flush_async_add_bbapi(int id, const char* source, const char* destination);
int axl_flush_async_init_bbapi(void);
int axl_flush_async_finalize_bbapi(void);

#endif //AXL_ASYNC_BBAPI_H
