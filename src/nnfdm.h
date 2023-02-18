#ifndef AXL_ASYNC_NNFDM_H
#define AXL_ASYNC_NNFDM_H

#ifdef __cplusplus
extern "C" {
#endif

void nnfdm_init();
void nnfdm_finalize();
int nnfdm_start(int id);
int nnfdm_test(int id);
int nnfdm_wait(int id);
int nnfdm_cancel(int id);

#ifdef __cplusplus
};
#endif

#endif //AXL_ASYNC_NNFDM_H
