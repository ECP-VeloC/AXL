#include <unistd.h>           /* For: sleep */
#include "axl_internal.h"
#include "datamovement.pb-c.h"
#include "kvtree.h"
#include "nnfdm.h"

static GoInt32 nnfdm_connection_ref = 0;
static int nnfdm_initialized = 0;

/* TODO: Move to appropriate header file for key/value keys */
#define AXL_KEY_FILE_SESSION_UID "NNFDM_Session_ID"


void axl_async_init_nnfdm() {
    if (!nnfdm_initialized) {
        nnfdm_initialized++;
        char *socketAddr = "/var/run/nnf-dm.sock";

        struct OpenConnection_return conn;
        conn = OpenConnection(socketAddr);
        if (conn.r1 != 0) {
            axl_abort(-1,
                "NNFDM OpenConnection(%s) failed with error %d @ %s:%d",
                socketAddr, conn.r1, __FILE__, __LINE__
            );
        }
        nnfdm_connection_ref = conn.r0;
    }
}

void axl_async_finalize_nnfdm() {
    if (nnfdm_initialized) {
        nnfdm_initialized = 0;

        CloseConnection(nnfdm_connection_ref);
        /* TODO: No return value from here? */
    }
}

static int axl_async_stat_nnfdm(char* uid) {
    struct Status_return statusResponse = Status(uid);
    int state = statusResponse.r0;
    int status = statusResponse.r1;
    int rc = statusResponse.r2;

    if (rc != 0) {
        axl_abort(-1,
            "Data Mover Status(%s) failed with error %d @ %s:%d",
            uid, rc, __FILE__, __LINE__
        );
    }

    switch (state) {
        case DATAMOVEMENT__DATA_MOVEMENT_STATUS_RESPONSE__STATE__PENDING:
        case DATAMOVEMENT__DATA_MOVEMENT_STATUS_RESPONSE__STATE__STARTING:
        case DATAMOVEMENT__DATA_MOVEMENT_STATUS_RESPONSE__STATE__RUNNING:
            status = AXL_STATUS_INPROG;
            break;
        case DATAMOVEMENT__DATA_MOVEMENT_STATUS_RESPONSE__STATE__COMPLETED:
            switch (status) {
                case DATAMOVEMENT__DATA_MOVEMENT_STATUS_RESPONSE__STATUS__SUCCESS:
                    status = AXL_STATUS_DEST;
                    break;
                case DATAMOVEMENT__DATA_MOVEMENT_STATUS_RESPONSE__STATUS__INVALID:
                case DATAMOVEMENT__DATA_MOVEMENT_STATUS_RESPONSE__STATUS__NOT_FOUND:
                case DATAMOVEMENT__DATA_MOVEMENT_STATUS_RESPONSE__STATUS__FAILED:
                case DATAMOVEMENT__DATA_MOVEMENT_STATUS_RESPONSE__STATUS__UNKNOWN_STATUS:
                    status = AXL_STATUS_ERROR;
                    break;
                default:
                    axl_abort(-1,
                        "Data Mover Status(%s) Unknown status: %d @ %s:%d",
                        uid, status, __FILE__, __LINE__
                    );
                    /*NOTREACHED*/
                    break;
            }
            break;
        default:
            axl_abort(-1,
                "Data Mover Status(%s) Unknown state: %d @ %s:%d",
                uid, state, __FILE__, __LINE__
            );
            /*NOTREACHED*/
            break;
    }
    return status;
}

int axl_async_start_nnfdm(int id) {
    /* Record that we started transfer of this file list */
    kvtree* file_list = axl_kvtrees[id];
    kvtree_util_set_int(file_list, AXL_KEY_STATUS, AXL_STATUS_INPROG);
 
    /* iterate over files */
    kvtree_elem* elem;
    kvtree* files = kvtree_get(file_list, AXL_KEY_FILES);
    for (kvtree_elem* elem = kvtree_elem_first(files); elem != NULL; elem = kvtree_elem_next(elem)) {
        /* get the filename */
        char* file = kvtree_elem_key(elem);
 
        /* get the hash for this file */
        kvtree* elem_hash = kvtree_elem_hash(elem);
 
        /* get the destination for this file */
        char* dest_file;
        kvtree_util_get_str(elem_hash, AXL_KEY_FILE_DEST, &dest_file);

        struct Create_return createResponse;
        createResponse = Create(file, dest_file);
        if (createResponse.r1 != 0) {
            axl_abort(-1,
                "Data Mover Create(%s, %s) failed with error %d @ %s:%d",
                file, dest_file, createResponse.r1, __FILE__, __LINE__
            );
        }

        // Get status of the data movement task.
        struct Status_return statusResponse;
        statusResponse = Status(createResponse.r0);
        if (statusResponse.r2 != 0) {
            axl_abort(-1,
                "Data Mover Status(%s) failed with error %d @ %s:%d",
                createResponse.r0, statusResponse.r2, __FILE__, __LINE__
            );
        }
 
        /* record that the file is in progress */
        kvtree_util_set_int(elem_hash, AXL_KEY_FILE_STATUS, axl_async_stat_nnfdm(createResponse.r0));

        /* record the session Id for this file */
        kvtree_util_set_str(elem_hash, AXL_KEY_FILE_SESSION_UID, createResponse.r0);
    }
 
    return AXL_SUCCESS;
}

int axl_async_test_nnfdm(int id) {
    kvtree* file_list = axl_kvtrees[id];
    kvtree* files     = kvtree_get(file_list, AXL_KEY_FILES);

    /* iterate/wait over in-progress files */
    for (kvtree_elem* elem = kvtree_elem_first(files); elem != NULL; elem = kvtree_elem_next(elem)) {
        int status;
        char* uid;

        kvtree* elem_hash = kvtree_elem_hash(elem);
        kvtree_util_get_int(elem_hash, AXL_KEY_FILE_STATUS, &status);

        if (status == AXL_STATUS_DEST) {
            continue;   /* This one is done */
        }

        kvtree_util_get_str(elem_hash, AXL_KEY_FILE_SESSION_UID, &uid);
        status = axl_async_stat_nnfdm(uid);

        if (status != AXL_STATUS_DEST) {
            return AXL_FAILURE;   /* At least one file is not done */
        }

        kvtree_util_set_int(elem_hash, AXL_KEY_FILE_STATUS, status);
    }

    return AXL_SUCCESS;
}

int axl_async_wait_nnfdm(int id) {
    kvtree* file_list = axl_kvtrees[id];
    kvtree* files     = kvtree_get(file_list, AXL_KEY_FILES);

    /* iterate/wait over in-progress files */
    for (kvtree_elem* elem = kvtree_elem_first(files); elem != NULL; elem = kvtree_elem_next(elem)) {
        int status;
        char* uid;

        kvtree* elem_hash = kvtree_elem_hash(elem);
        kvtree_util_get_str(elem_hash, AXL_KEY_FILE_SESSION_UID, &uid);

        for (status = axl_async_stat_nnfdm(uid); status == AXL_STATUS_INPROG; status = axl_async_stat_nnfdm(uid)) {
            sleep(1);
        }

        /* Delete the request */
        struct Delete_return deleteResponse;
        deleteResponse = Delete(uid);
        if (deleteResponse.r1 != 0) {
            char* file = kvtree_elem_key(elem);

            axl_abort(-1,
                "Delete(uid=%s, filename=%s) for %s failed with %d @ %s:%d",
                uid, file, __FILE__, __LINE__
            );
        }
        Free(uid);


        kvtree_util_set_int(elem_hash, AXL_KEY_FILE_STATUS, status);
    }

    /* iterate over files */
    for (kvtree_elem* elem = kvtree_elem_first(files); elem != NULL; elem = kvtree_elem_next(elem)) {
        kvtree* elem_hash = kvtree_elem_hash(elem);

        int status;
        kvtree_util_get_int(elem_hash, AXL_KEY_FILE_STATUS, &status);

        switch (status) {
            case AXL_STATUS_DEST:
                break;
            case AXL_STATUS_SOURCE:
            case AXL_STATUS_ERROR:
            default:
                axl_abort(-1,
                    "Wait operation called on file with invalid status @ %s:%d",
                    __FILE__, __LINE__
                );
        }
    }

    /* record transfer complete */
    kvtree_util_set_int(file_list, AXL_KEY_STATUS, AXL_STATUS_DEST);
    return AXL_SUCCESS;
}

