#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <limits.h>
#include <errno.h>
#include <sys/stat.h>
#include <signal.h>
#include "axl.h"
#include "axl_mpi.h"

#include "mpi.h"

int id = -1;

MPI_Comm comm = MPI_COMM_NULL;
int comm_rank = 0;
int comm_size = 1;

/* Return 1 if path is a directory */
static int
is_dir(const char *path) {
   struct stat s;
   if (stat(path, &s) != 0)
       return 0;
   return S_ISDIR(s.st_mode);
}

/* Translate a string like "bbapi" to its corresponding axl_xfer_t */
static axl_xfer_t
axl_xfer_str_to_xfer(const char *xfer_str)
{
    struct {
        const char *xfer_str;
        axl_xfer_t xfer;
    } xfer_strs[] = {
            {"default", AXL_XFER_DEFAULT},
            {"native", AXL_XFER_NATIVE},
            {"sync", AXL_XFER_SYNC},
            {"dw", AXL_XFER_ASYNC_DW},
            {"bbapi", AXL_XFER_ASYNC_BBAPI},
            {"cppr", AXL_XFER_ASYNC_CPPR},
            {"pthread", AXL_XFER_PTHREAD},
            {NULL, AXL_XFER_NULL},  /* must always be last element in array */
    };
    int i;

    for (i = 0; xfer_strs[i].xfer_str; i++) {
        if (strcmp(xfer_strs[i].xfer_str, xfer_str) == 0) {
            /* Match */
            return xfer_strs[i].xfer;
        }
    }
    /* No match */
    return AXL_XFER_NULL;
}

static void
usage(void)
{
    if (comm_rank == 0) {
        printf("Usage: axl_cp [-r|-R] [-X xfer_type] SOURCE DEST\n");
        printf("       axl_cp [-r|-R] [-X xfer_type] SOURCE... DIRECTORY\n");
        printf("\n");
        printf("-r|-R:          Copy directories recursively\n");
        printf("-X xfer_type:   AXL transfer type:  default native pthread sync dw bbapi cppr\n");
        printf("\n");
    }
}

void sig_func(int signum)
{
    int rc;
    if (id == -1) {
        /* AXL not initialized yet */
        return;
    }

    rc = AXL_Cancel_comm(id, comm);
    if (rc != AXL_SUCCESS) {
        printf("axl_cp SIGTERM: AXL_Cancel failed (%d)", rc);
        MPI_Abort(comm, -1);
        exit(rc);
    }

    rc = AXL_Free(id);
    if (rc != AXL_SUCCESS) {
        printf("axl_cp SIGTERM: AXL_Free failed (%d)", rc);
        MPI_Abort(comm, -1);
        exit(rc);
    }

    rc = AXL_Finalize();
    if (rc != AXL_SUCCESS) {
        printf("axl_cp SIGTERM: AXL_Finalized failed (%d)", rc);
        MPI_Abort(comm, -1);
        exit(rc);
    }

    exit(AXL_SUCCESS);
}

int
main(int argc, char **argv) {
    int rc;
    int opt;
    int args_left;
    const char *xfer_str = NULL;
    char **src = NULL;
    char *dest = NULL;
    axl_xfer_t xfer;
    unsigned int src_count;
    int i;
    int recursive = 0;
    struct sigaction action;

    MPI_Init(&argc, &argv);
    comm = MPI_COMM_WORLD;
    MPI_Comm_rank(comm, &comm_rank);
    MPI_Comm_size(comm, &comm_size);

    memset(&action, 0, sizeof(action));
    action.sa_handler = sig_func;
    sigaction(SIGTERM, &action, NULL);
    char *state_file = NULL;

    while ((opt = getopt(argc, argv, "rRSX:")) != -1) {
        switch (opt) {
            case 'X':
                xfer_str = optarg;
                break;
            case 'S':
                state_file = optarg;
                break;
            case 'r':
            case 'R':
                recursive = 1;
                break;
            default: /* '?' */
                usage();
                MPI_Finalize();
                exit(1);
        }
    }
    args_left = argc - optind;

    /* Did they specify source(s) and destination? */
    if (args_left == 0) {
        if (comm_rank == 0) {
            printf("Missing source and destination\n");
        }
        usage();
        MPI_Finalize();
        exit(1);
    } else if (args_left == 1) {
        if (comm_rank == 0) {
            printf("Missing destination");
        }
        usage();
        MPI_Finalize();
        exit(1);
    } else {
        src = &argv[optind];
        src_count = args_left - 1;
        dest = argv[optind + args_left - 1];
    }

    /*
     * They're doing a copy where the final arg is a destination directory,
     * not a file.
     */
    if (args_left > 2 && !is_dir(dest)) {
        if (comm_rank == 0) {
            printf("axl_cp: target '%s' is not a directory\n", dest);
        }
        MPI_Finalize();
        exit(1);
    }

    if (xfer_str) {
        xfer = axl_xfer_str_to_xfer(xfer_str);
        if (xfer == AXL_XFER_NULL) {
            if (comm_rank == 0) {
                printf("Error: Invalid AXL transfer type '%s'\n", xfer_str);
                printf("\n");
            }
            usage();
            MPI_Finalize();
            exit(1);
        }
    } else {
        xfer = AXL_XFER_SYNC;
    }

    rc = AXL_Init_comm(state_file, comm);
    if (rc != AXL_SUCCESS) {
        if (comm_rank == 0) {
            printf("AXL_Init() failed (error %d)\n", rc);
        }
        MPI_Finalize();
        return rc;
    }

    id = AXL_Create_comm(xfer, "axl_cp", comm);
    if (id == -1) {
        if (comm_rank == 0) {
            printf("AXL_Create() failed (error %d)\n", id);
        }
        MPI_Finalize();
        return id;
    }

    for (i = 0; i < src_count; i++) {
        if (!recursive && is_dir(src[i])) {
            if (comm_rank == 0) {
                printf("axl_cp: omitting directory '%s'\n", src[i]);
            }
            continue;
        }

        /* distribute source items round robin across ranks */
        if (i % comm_size == comm_rank) {
            rc = AXL_Add(id, src[i], dest);
            if (rc != AXL_SUCCESS) {
                /* not collective so print from any rank */
                printf("AXL_Add(..., %s, %s) failed (error %d)\n", src[i], dest, rc);
                MPI_Abort(comm, -1);
                return rc;
            }
        }
    }

    rc = AXL_Dispatch_comm(id, comm);
    if (rc != AXL_SUCCESS) {
        if (comm_rank == 0) {
            printf("AXL_Dispatch() failed (error %d)\n", rc);
        }
        MPI_Finalize();
        return rc;
    }

    /* Wait for transfer to complete and finalize axl */
    rc = AXL_Wait_comm(id, comm);
    if (rc != AXL_SUCCESS) {
        if (comm_rank == 0) {
            printf("AXL_Wait() failed (error %d)\n", rc);
        }
        MPI_Finalize();
        return rc;
    }

    rc = AXL_Free_comm(id, comm);
    if (rc != AXL_SUCCESS) {
        if (comm_rank == 0) {
            printf("AXL_Free() failed (error %d)\n", rc);
        }
        MPI_Finalize();
        return rc;
    }

    rc = AXL_Finalize_comm(comm);
    if (rc != AXL_SUCCESS) {
        if (comm_rank == 0) {
            printf("AXL_Finalize() failed (error %d)\n", rc);
        }
        MPI_Finalize();
        return rc;
    }

    MPI_Finalize();

    return AXL_SUCCESS;
}
