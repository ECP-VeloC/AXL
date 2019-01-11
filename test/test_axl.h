#ifndef TEST_AXL_H
#define TEST_AXL_H

#include "axl.h"

#define TEST_PASS (0)
#define TEST_FAIL (1)

struct test_args {
    /* Source and destination file paths */
    const char *src_path;
    const char *dst_path;
};

typedef int (*test_ptr_t)(struct test_args *);

void register_test(test_ptr_t test, char* test_name);
int axl_compare_files_or_dirs(const char *path1, const char *path2);
#endif //TEST_AXL_H
