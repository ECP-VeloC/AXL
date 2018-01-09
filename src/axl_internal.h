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

#ifndef AXL_INTERNAL_H
#define AXL_INTERNAL_H

extern char* axl_transfer_file;
extern char* axl_flush_file;
extern int axl_flush_async_in_progress; /* tracks whether an async flush is currently underway */
extern int axl_flush_async_dataset_id; /* tracks the id of the checkpoint being flushed */

/*
=========================================
TODO: Cleanup who owns these keys
========================================
*/

#define AXL_KEY_FILE ("FILE")
#define AXL_KEY_PATH ("PATH")
#define AXL_KEY_META ("META")

/*
=========================================
TODO: Cleanup who owns these keys
========================================
*/

#define SCR_SUMMARY_FILE_VERSION_6 (6)
#define SCR_SUMMARY_6_KEY_DATASET   ("DSET")
#define SCR_SUMMARY_6_KEY_RANK2FILE ("RANK2FILE")
#define SCR_SUMMARY_6_KEY_LEVEL     ("LEVEL")
#define SCR_SUMMARY_6_KEY_RANK      ("RANK")
#define SCR_SUMMARY_6_KEY_RANKS     ("RANKS")
#define SCR_SUMMARY_6_KEY_COMPLETE  ("COMPLETE")
#define SCR_SUMMARY_6_KEY_FILE      ("FILE")
#define SCR_SUMMARY_6_KEY_FILES     ("FILES")
#define SCR_SUMMARY_6_KEY_SIZE      ("SIZE")
#define SCR_SUMMARY_6_KEY_CRC       ("CRC")
#define SCR_SUMMARY_6_KEY_NOFETCH   ("NOFETCH")
#define SCR_SUMMARY_6_KEY_CONTAINER ("CTR")
#define SCR_SUMMARY_6_KEY_SEGMENT   ("SEG")
#define SCR_SUMMARY_6_KEY_ID        ("ID")
#define SCR_SUMMARY_6_KEY_LENGTH    ("LENGTH")
#define SCR_SUMMARY_6_KEY_OFFSET    ("OFFSET")

/*
=========================================
AXL Owned Keys
========================================
*/

#define AXL_TRANSFER_KEY_FILES ("FILES")
#define AXL_TRANSFER_KEY_DESTINATION ("DESTINATION")
#define AXL_TRANSFER_KEY_SIZE ("SIZE")
#define AXL_TRANSFER_KEY_WRITTEN ("WRITTEN")
#define AXL_TRANSFER_KEY_BW ("BW")
#define AXL_TRANSFER_KEY_PERCENT ("PERCENT")

#define AXL_TRANSFER_KEY_COMMAND ("COMMAND")
#define AXL_TRANSFER_KEY_COMMAND_RUN ("RUN")
#define AXL_TRANSFER_KEY_COMMAND_STOP ("STOP")
#define AXL_TRANSFER_KEY_COMMAND_EXIT ("EXIT")

#define AXL_TRANSFER_KEY_STATE ("STATE")
#define AXL_TRANSFER_KEY_STATE_RUN ("RUNNING")
#define AXL_TRANSFER_KEY_STATE_STOP ("STOPPED")
#define AXL_TRANSFER_KEY_STATE_EXIT ("EXIT")

#define AXL_TRANSFER_KEY_FLAG ("FLAG")
#define AXL_TRANSFER_KEY_FLAG_DONE ("DONE")

#define AXL_FLUSH_KEY_DATASET ("DATASET")
#define AXL_FLUSH_KEY_LOCATION ("LOCATION")
#define AXL_FLUSH_KEY_LOCATION_CACHE ("CACHE")
#define AXL_FLUSH_KEY_LOCATION_PFS ("PFS")
#define AXL_FLUSH_KEY_LOCATION_FLUSHING ("FLUSHING")
#define AXL_FLUSH_KEY_LOCATION_SYNC_FLUSHING ("SYNC_FLUSHING")
#define AXL_FLUSH_KEY_DIRECTORY ("DIR")
#define AXL_FLUSH_KEY_NAME ("NAME")
#define AXL_FLUSH_KEY_CKPT ("CKPT")
#define AXL_FLUSH_KEY_OUTPUT ("OUTPUT")

/*
=========================================
axl_err.c functions
========================================
*/

/* print message to stdout if axl_debug is set and it is >= level */
void axl_dbg(int level, const char *fmt, ...);

/* print error message to stdout */
void scr_err(const char *fmt, ...);

/*
=========================================
axl_flush.c functions
========================================
*/

/* given a filemap and a dataset id, prepare and return a list of files to be flushed,
 * also create corresponding directories and container files */
int axl_flush_prepare(const scr_filemap* map, int id, scr_hash* file_list);

/* given a dataset id that has been flushed, the list provided by axl_flush_prepare,
 * and data to include in the summary file, complete the flush by writing the summary file */
int scr_flush_complete(int id, scr_hash* file_list, scr_hash* data);

#endif // AXL_INTERNAL_H
