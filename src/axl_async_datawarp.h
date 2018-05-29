/*
 * Copyright (c) 2009, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by Greg Becker <becker33@llnl.gov>.
 * LLNL-CODE-411039.
 * All rights reserved.
 * This file is part of The Scalable Checkpoint / Restart (SCR) library.
 * For details, see https://sourceforge.net/projects/scalablecr/
 * Please also read this file: LICENSE.TXT.
*/

#ifndef AXL_ASYNC_DATAWARP_H
#define AXL_ASYNC_DATAWARP_H

int axl_async_start_datawarp(int id);
int axl_async_complete_datawarp(int id);
int axl_async_stop_datawarp(int id);
int axl_async_test_datawarp(int id);
int axl_async_wait_datawarp(int id);
int axl_async_init_datawarp(void);
int axl_async_finalize_datawarp(void);

#endif //AXL_ASYNC_DATAWARP_H
