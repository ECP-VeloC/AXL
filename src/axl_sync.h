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

#ifndef AXL_SYNC_H
#define AXL_SYNC_H

int axl_flush_sync_start(int id);
int axl_flush_sync_test(int id);
int axl_flush_sync_wait(int id);

#endif //AXL_SYNC_H
