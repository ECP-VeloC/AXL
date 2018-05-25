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
