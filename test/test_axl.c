#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>

#include "test_axl.h"
#include "test_axl_sync.h"
#include "test_axl_async_daemon.h"
#include "test_axl_async_ibmbb.h"

#define SYNC        0x1
#define ASYNC       0x2
#define IBMBB       0x4
#define DATAWARP    0x8

/* MAX_TESTS can be changed arbitrarily as long as
 * it exceeds the number of tests
 */
#define MAX_TESTS (1024)

static num_tests = 0;

struct test {
    char *name;
    test_ptr_t func;
} tests[MAX_TESTS];

void register_test(test_ptr_t test_func, char* test_name) {
    if (num_tests >= MAX_TESTS) {
            fprintf(stderr, "Ran out of test space\n");
            exit(1);
    }

    tests[num_tests].name = strdup(test_name);
    tests[num_tests].func = test_func;
    num_tests++;
}

static int
do_tests(unsigned int flags, struct test_args *test_args) {
    int i;
    int test_rc[MAX_TESTS] = {0};
    int num_failed = 0;

    /* Initialize each test type */
    if (flags & ASYNC) {
        test_axl_async_daemon_init();
    }
    if (flags & IBMBB) {
        test_axl_async_ibmbb_init();
    }
    if (flags & SYNC) {
        test_axl_sync_init();
    }

    /* Run the tests */
    for(i = 0; i < num_tests; i++) {
        test_rc[i] = tests[i].func(test_args);
        if (test_rc[i] != TEST_PASS) {
            num_failed++;
        }
    }

    /* Print results */
    printf("Ran %d tests: %d pass, %d fail.\n", num_tests, num_tests - num_failed, num_failed);
    for(i = 0; i < num_tests; i++) {
            printf("    %s %s\n", tests[i].name,
                test_rc[i] == 0 ? "PASSED" : "FAILED", test_rc[i]);
    }

    /* Free all allocated memory */
    for(i = 0; i < num_tests; i++) {
        free(tests[i].name);
    }

    /* Return number of tests failed as return code */
    return num_failed;
}


/*
 * Create a file with `size` bytes of random text data in it.
 *
 * Returns 0 on success, errno value on failure.
 * */
int create_source_file(const char *source_file, size_t size)
{
    FILE * fp;
    char buf[1024];
    int chunk_size = sizeof(buf);
    int chunks;
    int rc = 0;
    int chunk, i;

    fp = fopen(source_file, "w+");
    if (!fp) {
        return errno;
    }

    chunks = (size / chunk_size);
    if (size % chunk_size > 0)
        chunks++;    /* The last chunk isn't totally full */

    /* Write the random data out */
    for (chunk = 0; chunk < chunks; chunk++) {
        int this_chunk_size;

        for (i = 0; i < chunk_size; i++) {
            if (i % 80 == 0) {
                buf[i] = '\n';
            } else {
                buf[i] = 'a' + (rand() % 26);    /* random printable chars */
            }
        }

        if (size - (chunk * chunk_size) >= chunk_size ) {
            /* Full chunk */
            this_chunk_size = chunk_size;
        } else {
            /* Last leftover data */
            this_chunk_size = size % chunk_size;
        }

        rc = fwrite(buf, this_chunk_size, 1, fp);
        if (rc != 1) {
            printf("%d fwrite rc %d\n", chunk, rc);
            fprintf(stderr, "Error writing %s\n", strerror(errno));
            return errno;
        }
    }

    fclose(fp);
    return 0;
}

static void usage(char *argv0) {
        printf( "Usage: %s -abds [[-S size]|[src [dst]]]\n",
                argv0);
        printf("\n");
        printf("Tests to run:\n");
        printf("\t-a:\tAsync tests\n");
        printf("\t-b:\tIBM Burst Buffer tests\n");
        printf("\t-d:\tCray DataWarp tests\n");
        printf("\t-s:\tSync tests\n");
        printf("\t-S size:\tSize in bytes of test file to create");
        printf("\n");
        printf("\tsrc:\tsource file path (can be existing file)\n");
        printf("\tdst:\tdestination file path");
        printf("\n");
}


int main(int argc, char** argv){
    unsigned int flags = 0;
    int opt;
    struct test_args test_args;
    int rc;
    char dst_path[1024];
    int args_left;
    unsigned long size = 1024*1024;;

    while ((opt = getopt(argc, argv, "abdhsS:")) != -1) {
        switch (opt) {
            case 'a':
                flags |= ASYNC;
                break;
            case 'b':
                flags |= IBMBB;
                break;
            case 'd':
                flags |= DATAWARP;
                break;
            case 'h':
                usage(argv[0]);
                exit(0);
            case 's':
                flags |= SYNC;
                break;
            case 'S':
                size = atol(optarg);
                break;
            default: /* '?' */
                usage(argv[0]);
                exit(EXIT_FAILURE);
        }
    }

    args_left = argc - optind;

    /* Did they specify a source and destination paths? */
    if (args_left >= 1) {
        test_args.src_path = argv[optind];
        if (args_left >= 2) {
            /* Source and destination path */
            test_args.dst_path = argv[optind + 1];
        }
    } else {
        /* No source or destination paths, use defaults. */
        test_args.src_path = "/tmp/testfile";
        sprintf(dst_path,"%s/%s", getenv("BBPATH"), "testfile");
        test_args.dst_path = dst_path;
    }

    if (access(test_args.src_path, F_OK ) != -1 ) {
        printf("Using existing source file %s\n", test_args.src_path);
    } else {
        printf("Creating %lu byte source file %s\n", size, test_args.src_path);
        rc = create_source_file(test_args.src_path, size);
        if (rc != 0) {
            fprintf(stderr, "Error opening source test file %s for writing: %s\n", test_args.src_path,
                strerror(rc));
            return rc;
        }
    }

    return do_tests(flags, &test_args);
}
