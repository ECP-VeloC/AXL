/*
 * Copyright (c) 2009, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by Adam Moody <moody20@llnl.gov>.
 * LLNL-CODE-411039.
 * All rights reserved.
 * This file is part of The Scalable Checkpoint / Restart (SCR) library.
 * For details, see https://sourceforge.net/projects/scalablecr/
 * Please also read this file: LICENSE.TXT.
*/

#ifndef AXL_ASYNC_DAEMON_H
#define AXL_ASYNC_DAEMON_H

int axl_flush_async_start_daemon(int id);
int axl_flush_async_complete_daemon(int id);
int axl_flush_async_stop_daemon(int id);
int axl_flush_async_test_daemon(int id);
int axl_flush_async_wait_daemon(int id);
int axl_flush_async_init_daemon(void);
int axl_flush_async_finalize_daemon(void);

#endif //AXL_ASYNC_DAEMON_H
