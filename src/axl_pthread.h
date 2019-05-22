#ifndef AXL_PTHREAD_H
#define AXL_PTHREAD_H

int axl_pthread_start(int id);
int axl_pthread_test(int id);
int axl_pthread_wait(int id);
int axl_pthread_cancel(int id);
void axl_pthread_free(int id);

#endif //AXL_SYNC_H
