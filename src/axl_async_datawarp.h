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
