#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/stat.h>

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

static unsigned int num_tests = 0;


/*
 * Diff the contents of two files.
 *
 * Return 0 if the file contents are the same, 1 if they differ.
 */
static int axl_compare_file_contents(const char *file1, const char *file2)
{
    FILE *fp1 = NULL, *fp2 = NULL;
    char buf1[4096], buf2[4096];
    size_t rc1, rc2;
    int rc = 1;

    fp1 = fopen(file1, "r");
    if (!fp1) {
        goto end;
    }

    fp2 = fopen(file2, "r");
    if (!fp2) {
        goto end;
    }

    while (1) {
        rc1 = fread(buf1, sizeof(*buf1), 1, fp1);
        if (rc1 < 0) {
            break;
        }

        rc2 = fread(buf2, sizeof(*buf2), 1, fp2);
        if (rc2 < 0) {
            break;
        }

        if (rc1 != rc2) {
            break;
        }

        /* Are we done reading the files? */
        if (rc1 == 0) {
            /* All done, success */
            rc = 0;
            break;
        }

        /* Did the file contents match? */
        if (memcmp(buf1, buf2, rc1) != 0) {
            break;  /* nope */
        }
    }

end:
    if (fp1) {
        fclose(fp1);
    }

    if (fp2) {
        fclose(fp2);
    }

    return rc;
}

/*
 * Compare two files or two directories.
 *
 * 1. Given two file paths, compare the file permissions and file contents to
 * make sure they're the same.
 *
 * 2. Given two directory paths, compare the directory permissions.
 *
 * Returns 0 if everything is the same, non-zero otherwise.
 */
int axl_compare_files_or_dirs(const char *path1, const char *path2)
{
    struct stat stat1, stat2;
    if (stat(path1, &stat1) != 0) {
        fprintf(stderr, "Couldn't stat %s: %s\n", path1, strerror(errno));
        return errno;
    }
    if (stat(path1, &stat2) != 0) {
        fprintf(stderr, "Couldn't stat %s: %s\n", path2, strerror(errno));
        return errno;
    }
    if (!((S_ISREG(stat1.st_mode) && S_ISREG(stat2.st_mode)) ||
        (S_ISDIR(stat1.st_mode) && S_ISDIR(stat2.st_mode)))) {
        fprintf(stderr, "%s and %s should be either both files, or both dirs\n",
            path1, path2);
        return EINVAL;
    }

    if (stat1.st_size != stat2.st_size) {
        fprintf(stderr, "%s and %s have different sizes (%zu and %zu)\n",
            path1, path2, stat1.st_size, stat2.st_size);
        return EINVAL;
    }

    /* We were passed two files, compare their contents. */
    if (S_ISREG(stat1.st_mode) && S_ISREG(stat2.st_mode)) {
        if (axl_compare_file_contents(path1, path2)) {
            fprintf(stderr, "%s and %s file contents differed\n",
                path1, path2);
            return EINVAL;
        }
    }

    return 0;
}

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
