/* keys used in AXL transfer file */
#ifndef AXL_ASYNC_DAEMON_KEYS_H
#define AXL_ASYNC_DAEMON_KEYS_H

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
 *           bytes_written */
#define AXL_TRANSFER_KEY_ID          ("ID")
#define AXL_TRANSFER_KEY_FILES       ("FILES")
#define AXL_TRANSFER_KEY_DESTINATION ("DESTINATION")
#define AXL_TRANSFER_KEY_SIZE        ("SIZE")
#define AXL_TRANSFER_KEY_WRITTEN     ("WRITTEN")

/* defines throttling parameters */
#define AXL_TRANSFER_KEY_BW          ("BW")
#define AXL_TRANSFER_KEY_PERCENT     ("PERCENT")

/* command given from library to daemon */
#define AXL_TRANSFER_KEY_COMMAND ("COMMAND")
#define AXL_TRANSFER_KEY_COMMAND_RUN  ("RUN")
#define AXL_TRANSFER_KEY_COMMAND_STOP ("STOP")
#define AXL_TRANSFER_KEY_COMMAND_EXIT ("EXIT")

/* state of daemon, written by daemon to inform library */
#define AXL_TRANSFER_KEY_STATE ("STATE")
#define AXL_TRANSFER_KEY_STATE_RUN  ("RUNNING")
#define AXL_TRANSFER_KEY_STATE_STOP ("STOPPED")
#define AXL_TRANSFER_KEY_STATE_EXIT ("EXITING")

/* how daemon indicates to library that its done */
#define AXL_TRANSFER_KEY_FLAG ("FLAG")
#define AXL_TRANSFER_KEY_FLAG_DONE ("DONE")

#endif /* AXL_ASYNC_DAEMON_KEYS_H */
