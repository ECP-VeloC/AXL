#ifndef AXL_ASYNC_DAEMON_H
#define AXL_ASYNC_DAEMON_H

int axl_flush_async_init_daemon(const char* axld_path, const char* transfer_file);
int axl_flush_async_finalize_daemon(void);

int axl_flush_async_start_daemon(int id);
int axl_flush_async_test_daemon(int id, double* bytes_total, double* bytes_written);
int axl_flush_async_wait_daemon(int id);
int axl_flush_async_cancel_daemon(int id);

int axl_flush_async_stop_daemon();

#endif /* AXL_ASYNC_DAEMON_H */
