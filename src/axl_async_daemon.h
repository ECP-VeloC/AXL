#ifndef AXL_ASYNC_DAEMON_H
#define AXL_ASYNC_DAEMON_H

int axl_async_init_daemon(const char* axld_path, const char* transfer_file);
int axl_async_finalize_daemon(void);

int axl_async_start_daemon(int id);
int axl_async_test_daemon(int id, double* bytes_total, double* bytes_written);
int axl_async_wait_daemon(int id);
int axl_async_cancel_daemon(int id);

int axl_async_stop_daemon();

#endif /* AXL_ASYNC_DAEMON_H */
