#include "nnfdm/client.h"

extern "C" {
#include "nnfdm.h"
#include "axl_internal.h"
#include "kvtree.h"
}

near_node_flash::data_movement::DataMoverClient* nnfdm_client{nullptr};
near_node_flash::data_movement::Workflow* nnfdm_workflow{nullptr};

extern "C" {

// TODO: Need to move this where other keys are stored
#define AXL_KEY_FILE_SESSION_UID "NNFDM_Session_ID"

static int nnfdm_stat(const char* uid, int64_t max_seconds_to_wait)
{
    int rval = 0;
    near_node_flash::data_movement::StatusRequest request{std::string{uid}, max_seconds_to_wait};
    near_node_flash::data_movement::StatusResponse response;

    near_node_flash::data_movement::RPCStatus status = nnfdm_client->Status(*nnfdm_workflow, request, &response);
    if (!status.ok()) {
        axl_abort(-1,
            "NNFDM Status RPC FAILED %d: %s @ %s:%d",
            status.error_code(), status.error_message().c_str(), __FILE__, __LINE__
        );
        /*NOTREACHED*/
    }

    switch (response.state()) {
        case near_node_flash::data_movement::StatusResponse::STATE_PENDING:
        case near_node_flash::data_movement::StatusResponse::STATE_STARTING:
        case near_node_flash::data_movement::StatusResponse::STATE_RUNNING:
            rval = AXL_STATUS_INPROG;
            break;
        case near_node_flash::data_movement::StatusResponse::STATE_COMPLETED:
            switch (response.status()) {
                case near_node_flash::data_movement::StatusResponse::STATUS_SUCCESS:
                    rval = AXL_STATUS_DEST;
                    break;
                default:
                    rval = AXL_STATUS_ERROR;
                    axl_err("NNFDM Offload Status UNSUCCESSFUL: %d %s", response.status(), response.message().c_str());
                    break;
            }
        default:
            axl_abort(-1, "NNFDM Offload State STATE UNKNOWN: %d %s", response.status(), response.message().c_str());
            /*NOTREACHED*/
            break;
    }

    return rval;
}

void nnfdm_init(const char* sname, const char* workflow, const char* name_space)
{
    if (nnfdm_client == nullptr) {
        nnfdm_client = new near_node_flash::data_movement::DataMoverClient{std::string{sname}};
        if (nnfdm_client == nullptr) {
            axl_abort(-1,
                "NNFDM init: Failed to create data movement client instance for %s@ %s:%d",
                sname, __FILE__, __LINE__
            );
        }

        nnfdm_workflow = new near_node_flash::data_movement::Workflow{std::string{workflow}, std::string{name_space}};
        if (nnfdm_workflow == nullptr) {
            axl_abort(-1,
                "nnfdm_init: Failed to create data movement workflow instance for %s/%s@ %s:%d",
                workflow, name_space, __FILE__, __LINE__
            );
        }
    }
}

void nnfdm_finalize()
{
    if (nnfdm_client) {
        delete nnfdm_workflow;
        delete nnfdm_client;

        nnfdm_workflow = nullptr;
        nnfdm_client = nullptr;
    }
}

int nnfdm_start(int id)
{
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

        near_node_flash::data_movement::CreateRequest createRequest(std::string{file}, std::string{dest_file});
        near_node_flash::data_movement::CreateResponse createResponse;

        near_node_flash::data_movement::RPCStatus status = nnfdm_client->Create(*nnfdm_workflow, createRequest, &createResponse);
        if (!status.ok()) {
            axl_abort(-1,
                "NNFDM Create(%s, %s) failed with error %d (%s) @ %s:%d",
                file, dest_file, status.error_code(), status.error_message().c_str(), __FILE__, __LINE__
            );
            /*NOTREACHED*/
        }

        switch (createResponse.status()) {
            case near_node_flash::data_movement::CreateResponse::Status::STATUS_SUCCESS:
                /* record that the file is in progress */
                kvtree_util_set_int(elem_hash, AXL_KEY_FILE_STATUS, AXL_STATUS_INPROG);

                /* record the session Id for this file */
                kvtree_util_set_str(elem_hash, AXL_KEY_FILE_SESSION_UID, createResponse.uid().c_str());
                break;
            default:
                axl_abort(-1,
                    "NNFDM Offload Create FAILED for %s: error %d (%s) ",
                    file, createResponse.status(), createResponse.message().c_str()
                 );
                 /*NOTREACHED*/
                 break;
        } 
    }

    return AXL_SUCCESS;
}

int nnfdm_test(int id) {
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
        status = nnfdm_stat(uid, 0);

        if (status != AXL_STATUS_DEST) {
            return AXL_FAILURE;   /* At least one file is not done */
        }

        kvtree_util_set_int(elem_hash, AXL_KEY_FILE_STATUS, status);
    }

    return AXL_SUCCESS;
}

int nnfdm_wait(int id)
{
    const int64_t max_seconds_to_wait{1};
    kvtree* file_list = axl_kvtrees[id];
    kvtree* files     = kvtree_get(file_list, AXL_KEY_FILES);

    /* iterate/wait over in-progress files */
    for (kvtree_elem* elem = kvtree_elem_first(files); elem != NULL; elem = kvtree_elem_next(elem)) {
        int status;
        char* uid;

        kvtree* elem_hash = kvtree_elem_hash(elem);
        kvtree_util_get_str(elem_hash, AXL_KEY_FILE_SESSION_UID, &uid);

        do {
            status = nnfdm_stat(uid, max_seconds_to_wait);
        } while (status == AXL_STATUS_INPROG);

        near_node_flash::data_movement::DeleteRequest deleteRequest(std::string{uid});
        near_node_flash::data_movement::DeleteResponse deleteResponse;

        near_node_flash::data_movement::RPCStatus delete_status = nnfdm_client->Delete(*nnfdm_workflow, deleteRequest, &deleteResponse);
        if (!delete_status.ok()) {
            axl_abort(-1, "NNFDM Delete RPC FAILED: %d (%s)", delete_status.error_code(), delete_status.error_message().c_str());
            /*NOTEACHED*/
        }

        /* Delete the request */
        switch (deleteResponse.status()) {
            case near_node_flash::data_movement::DeleteResponse::STATUS_SUCCESS:
                break;
            default:
                char* file = kvtree_elem_key(elem);
                axl_abort(-1,
                    "NNFDM Offload Delete(%s) UNSUCCESSFUL: %d (%s)",
                    file, deleteResponse.status(), deleteResponse.message().c_str());
                return 1;
        }

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
}