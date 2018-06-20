#ifndef AXL_KEYS_H
#define AXL_KEYS_H

// THESE ARE OLD KEYS FROM KVTREE

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

/* defines a list of files to be transfered
 * ID
 *   id
 *     FILES
 *       /path/to/source/file
 *         DESTINATION
 *           /path/to/destination/file
 *         SIZE
 *           filesize
 *         WRITTEN
 *           bytes_written
 *         ERROR
 *           error message */
#define AXL_TRANSFER_KEY_ID          ("ID")
#define AXL_TRANSFER_KEY_FILES       ("FILES")
#define AXL_TRANSFER_KEY_DESTINATION ("DESTINATION")
#define AXL_TRANSFER_KEY_SIZE        ("SIZE")
#define AXL_TRANSFER_KEY_WRITTEN     ("WRITTEN")
#define AXL_TRANSFER_KEY_ERROR       ("ERROR")

/* defines throttling parameters */
#define AXL_TRANSFER_KEY_BW      ("BW")
#define AXL_TRANSFER_KEY_PERCENT ("PERCENT")

/* command given from library to daemon */
#define AXL_TRANSFER_KEY_COMMAND ("COMMAND")
#define AXL_TRANSFER_KEY_COMMAND_RUN  ("RUN")
#define AXL_TRANSFER_KEY_COMMAND_STOP ("STOP")
#define AXL_TRANSFER_KEY_COMMAND_EXIT ("EXIT")

/* state of daemon, written by daemon to inform library */
#define AXL_TRANSFER_KEY_STATE ("STATE")
#define AXL_TRANSFER_KEY_STATE_RUN  ("RUNNING")
#define AXL_TRANSFER_KEY_STATE_STOP ("STOPPED")
#define AXL_TRANSFER_KEY_STATE_EXIT ("EXIT")

/* how daemon indicates to library that it's done */
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

#endif
