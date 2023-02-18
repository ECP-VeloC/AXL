#include <stdio.h>
#include <inttypes.h>

#include "nnfdm/client.h"

extern "C" {
#include "nnfdm.h"
#include "axl_internal.h"
#include "kvtree.h"
}

namespace nnfdm = near_node_flash::data_movement;

namespace {
    nnfdm::DataMoverClient* nnfdm_client{nullptr};
    nnfdm::Workflow* nnfdm_workflow{nullptr};

    int nnfdm_stat(const char* uid, int64_t max_seconds_to_wait)
    {
        AXL_DBG(0, "Status(uid=%s, max_seconds_to_wait=%" PRId64 ")", uid, max_seconds_to_wait);
        int rval = 0;
        nnfdm::StatusRequest status_request{std::string{uid}, max_seconds_to_wait};
        nnfdm::StatusResponse status_response;
        nnfdm::RPCStatus rpc_status{ nnfdm_client->Status(*nnfdm_workflow,  status_request, &status_response) };

        if (!rpc_status.ok()) {
            axl_abort(    -1
                        , "NNFDM Status RPC FAILED %d: %s @ %s:%d"
                        , rpc_status.error_code()
                        , rpc_status.error_message().c_str()
                        , __FILE__
                        , __LINE__);
            /*NOTREACHED*/
        }

        switch (status_response.state()) {
            case nnfdm::StatusResponse::STATE_PENDING:
                AXL_DBG(0, "nnfdm::StatusResponse::STATE_PENDING");
                rval = AXL_STATUS_INPROG;
                break;
            case nnfdm::StatusResponse::STATE_STARTING:
                AXL_DBG(0, "nnfdm::StatusResponse::STATE_STARTING");
                rval = AXL_STATUS_INPROG;
                break;
            case nnfdm::StatusResponse::STATE_RUNNING:
                AXL_DBG(0, "nnfdm::StatusResponse::STATE_RUNNING");
                rval = AXL_STATUS_INPROG;
                break;
            case nnfdm::StatusResponse::STATE_COMPLETED:
                AXL_DBG(0, "nnfdm::StatusResponse::STATE_COMPLETED");
                switch (status_response.status()) {
                    case nnfdm::StatusResponse::STATUS_SUCCESS:
                        rval = AXL_STATUS_DEST;
                        break;
                    default:
                        rval = AXL_STATUS_ERROR;
                        axl_err(  "NNFDM Offload Status UNSUCCESSFUL: %d %s"
                                , status_response.status()
                                , status_response.message().c_str());
                        break;
                }
            default:
                axl_abort(   -1
                           , "NNFDM Offload State STATE UNKNOWN: %d %s"
                           , status_response.status()
                           , status_response.message().c_str());
                /*NOTREACHED*/
                break;
        }

        return rval;
    }
}   // End of empty namepace

extern "C" {

void nnfdm_init()
{
    if (nnfdm_client == nullptr) {
        const std::string socket_name{"unix:///var/run/nnf-dm.sock"};
        const std::string workflow_name{getenv("DW_WORKFLOW_NAME")};
        const std::string workflow_namespace{getenv("DW_WORKFLOW_NAMESPACE")};

        nnfdm_client = new nnfdm::DataMoverClient{std::string{socket_name}};
        if (nnfdm_client == nullptr) {
            axl_abort(-1
                      , "NNFDM init: Failed to create data movement client "
                        "instance for %s@ %s:%d"
                      , socket_name.c_str()
                      , __FILE__
                      , __LINE__);
        }

        nnfdm_workflow = new nnfdm::Workflow{workflow_name, workflow_namespace};
        if (nnfdm_workflow == nullptr) {
            axl_abort(  -1
                      , "nnfdm_init: Failed to create data movement workflow "
                        "instance for %s/%s@ %s:%d"
                      , workflow_name.c_str()
                      , workflow_namespace.c_str()
                      , __FILE__
                      , __LINE__);
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
    int rc = AXL_SUCCESS;

    /* Record that we started transfer of this file list */
    kvtree* file_list = axl_kvtrees[id];
    kvtree_util_set_int(file_list, AXL_KEY_STATUS, AXL_STATUS_INPROG);
 
    kvtree_elem* elem;
    kvtree* files = kvtree_get(file_list, AXL_KEY_FILES);
    for (  kvtree_elem* elem = kvtree_elem_first(files);
           elem != NULL;
           elem = kvtree_elem_next(elem))
    {
        char* src_filename = kvtree_elem_key(elem);
        kvtree* elem_hash = kvtree_elem_hash(elem);
 
        /* get the destination for this file */
        char* dst_filename;
        kvtree_util_get_str(elem_hash, AXL_KEY_FILE_DEST, &dst_filename);

        nnfdm::CreateRequest create_request(  std::string{src_filename}  
                                            , std::string{dst_filename});
        nnfdm::CreateResponse create_response;
        nnfdm::RPCStatus rpc_status = nnfdm_client->Create( *nnfdm_workflow
                                                           , create_request
                                                           , &create_response);
        if (!rpc_status.ok()) {
            axl_abort(  -1
                      , "NNFDM Create(%s, %s) failed with error %d (%s) @ %s:%d"
                      , src_filename
                      , dst_filename
                      , rpc_status.error_code()
                      , rpc_status.error_message().c_str()
                      , __FILE__
                      , __LINE__);
            /*NOTREACHED*/
        }

        if (create_response.status() == nnfdm::CreateResponse::Status::STATUS_SUCCESS) {
            /* record that the file is in progress */
            kvtree_util_set_int(elem_hash, AXL_KEY_FILE_STATUS, AXL_STATUS_INPROG);
            kvtree_util_set_str(elem_hash, AXL_KEY_FILE_SESSION_UID, create_response.uid().c_str());
        }
        else {
            AXL_DBG(0, "Create(%s, %s) FAILED:\n    error=%d (%s)"
                    , src_filename
                    , dst_filename
                    , create_response.status()
                    , create_response.message().c_str());
            kvtree_util_set_int(elem_hash, AXL_KEY_FILE_STATUS, AXL_STATUS_ERROR);
            rc = AXL_FAILURE;
        } 
    }

    return rc;
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
        status = nnfdm_stat(uid, 1);

        if (status != AXL_STATUS_DEST) {
            return AXL_FAILURE;   /* At least one file is not done */
        }

        kvtree_util_set_int(elem_hash, AXL_KEY_FILE_STATUS, status);
    }

    return AXL_SUCCESS;
}

int nnfdm_cancel(int id) {
    kvtree* file_list = axl_kvtrees[id];
    kvtree* files     = kvtree_get(file_list, AXL_KEY_FILES);

    /* iterate/wait over in-progress files */
    for (  kvtree_elem* elem = kvtree_elem_first(files);
           elem != NULL;
           elem = kvtree_elem_next(elem))
    {
        int status;
        char* uid;
        char* src_filename = kvtree_elem_key(elem);
        kvtree* elem_hash = kvtree_elem_hash(elem);
        kvtree_util_get_int(elem_hash, AXL_KEY_FILE_STATUS, &status);

        if (status == AXL_STATUS_DEST) {
            continue;   /* This one is done */
        }

        kvtree_util_get_str(elem_hash, AXL_KEY_FILE_SESSION_UID, &uid);

        nnfdm::CancelRequest cancel_request(uid);
        nnfdm::CancelResponse cancel_response;
        nnfdm::RPCStatus rpc_status = nnfdm_client->Cancel(  *nnfdm_workflow
                                                           ,  cancel_request
                                                           , &cancel_response);
        if (!rpc_status.ok()) {
            axl_abort(-1,
                "NNFDM Cancel(uid=%s, file=%s) failed with error %d (%s) @ %s:%d"
                , uid
                , src_filename
                , rpc_status.error_code()
                , rpc_status.error_message().c_str()
                , __FILE__
                , __LINE__
            );
            /*NOTREACHED*/
        }

        switch (cancel_response.status()) {
            case nnfdm::CancelResponse::STATUS_NOT_FOUND:
                //
                // Assume that unfound uids simply completed previously
                //
                axl_dbg(  0
                        , "NNFDM Cancel(uid=%s, file=%s) NOTFOUND - "
                          "IGNORING @ %s:%d"
                        , uid
                        , src_filename
                        , __FILE__
                        , __LINE__);
                continue;
            case nnfdm::CancelResponse::STATUS_SUCCESS:
                axl_dbg(  0
                        , "NNFDM Cancel(uid=%s, file=%s) Canceled @ %s:%d"
                        , uid
                        , src_filename
                        , __FILE__
                        , __LINE__);
                break;
            default:
                axl_abort(    -1
                            , "NNFDM Cancel(uid=%s, file=%s) "
                              "Failed with error %d - IGNORING @ %s:%d"
                            , uid
                            , src_filename
                            , cancel_response.status()
                            , __FILE__
                            , __LINE__);
                /*NOTEACHED*/
                return 1;
        }

        // Now delete the associated uid
        nnfdm::DeleteRequest delete_request(std::string{uid});
        nnfdm::DeleteResponse deleteResponse;
        rpc_status = nnfdm_client->Delete(   *nnfdm_workflow
                                           ,  delete_request
                                           , &deleteResponse);

        if (!rpc_status.ok()) {
            axl_abort(   -1
                       , "NNFDM Delete(uid=%s, file=%s) RPC FAILED: "
                         "%d (%s) @ %s:%d"
                       , uid
                       , src_filename
                       , rpc_status.error_code()
                       , rpc_status.error_message().c_str()
                       , __FILE__
                       , __LINE__);
            /*NOTEACHED*/
        }

        /* Delete the request */
        switch (deleteResponse.status()) {
            case nnfdm::DeleteResponse::STATUS_SUCCESS:
                break;
            default:
                axl_abort(-1,
                    "NNFDM Offload Delete(%s) UNSUCCESSFUL: %d (%s) @ %s:%d",
                    src_filename, deleteResponse.status(), deleteResponse.message().c_str(),
                    __FILE__, __LINE__);
                return 1;
        }

        kvtree_util_set_int(elem_hash, AXL_KEY_FILE_STATUS, AXL_STATUS_DEST);
    }

    return AXL_SUCCESS;
}

int nnfdm_wait(int id)
{
    const int64_t max_seconds_to_wait{1}; // wait 1 second
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

        nnfdm::DeleteRequest delete_request(std::string{uid});
        nnfdm::DeleteResponse deleteResponse;

        nnfdm::RPCStatus rpc_status = nnfdm_client->Delete(*nnfdm_workflow, delete_request, &deleteResponse);
        if (!rpc_status.ok()) {
            axl_abort(-1, "NNFDM Delete RPC FAILED: %d (%s)", rpc_status.error_code(), rpc_status.error_message().c_str());
            /*NOTEACHED*/
        }

        /* Delete the request */
        switch (deleteResponse.status()) {
            case nnfdm::DeleteResponse::STATUS_SUCCESS:
                break;
            default:
                char* src_filename = kvtree_elem_key(elem);
                axl_abort(-1,
                    "NNFDM Offload Delete(%s) UNSUCCESSFUL: %d (%s)",
                    src_filename, deleteResponse.status(), deleteResponse.message().c_str());
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
