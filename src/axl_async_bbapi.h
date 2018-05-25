#ifndef AXL_ASYNC_BBAPI_H
#define AXL_ASYNC_BBAPI_H

#define AXL_BBAPI_KEY_TRANSFERHANDLE ("BB_TransferHandle")
#define AXL_BBAPI_KEY_TRANSFERDEF ("BB_TransferDef")

int axl_flush_async_start_bbapi(int id);
int axl_flush_async_complete_bbapi(int id);
int axl_flush_async_stop_bbapi(int id);
int axl_flush_async_test_bbapi(int id);
int axl_flush_async_wait_bbapi(int id);
int axl_flush_async_create_bbapi(int id);
int axl_flush_async_add_bbapi(int id, const char* source, const char* destination);
int axl_flush_async_init_bbapi(void);
int axl_flush_async_finalize_bbapi(void);

#endif //AXL_ASYNC_BBAPI_H
