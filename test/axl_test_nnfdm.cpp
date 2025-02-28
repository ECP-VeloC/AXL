#include <iostream>
#include <string>
#include <memory>
#include <stdlib.h>

#include "nnfdm/client.h"

using namespace near_node_flash::data_movement;

int main(int argc, char** argv)
{
    std::string uid;

    DataMoverClient client("unix:///var/run/nnf-dm.sock");

    {
        // Retrieve the version information

        VersionResponse versionResponse;

        RPCStatus status = client.Version(&versionResponse);
        if (!status.ok()) {
            std::cerr << "Version RPC FAILED (" << status.error_code() << "): " << status.error_message() << std::endl;
            return 1;
        }

        std::cout << "Data Movement Version: " << versionResponse.version() << std::endl;
        std::cout << "Supported API Versions:" << std::endl;
        for (auto version : versionResponse.apiversions()) {
            std::cout << "\t" << version << std::endl;
        }

        return 0;
    }

    const char* workflow_name{getenv("DW_WORKFLOW_NAME")};
    const char* workflow_namespace{getenv("DW_WORKFLOW_NAMESPACE")};

    if (workflow_name == nullptr) {
        std::cerr << "DW_WORKFLOW_NAME environment variableis not set, make sure you are running within an allocation" << std::endl;
    }

    if (workflow_namespace == nullptr) {
        std::cerr << "DW_WORKFLOW_NAMESPACE environment variableis not set, make sure you are running within an allocation" << std::endl;
    }

    Workflow workflow(workflow_name, workflow_namespace);

    {
        // Create an offload request
        CreateRequest create_request(    "/mnt/nnf/854e2467-968a-4f40-aa8c-4ac53cb86866-0/foo"
                                       , "/p/lslide/martymcf/bar"
                                       , false                     // If True, the data movement command runs `/bin/true` rather than perform actual data movement
                                       , ""                        // mpirun command line options
                                       , ""                        // Extra options to pass to `dcp` if present in the Data Movement command.
                                       , false                     // If true, enable server-side logging of stdout when the command is successful. Failures output is always logged.
                                       , true                      // If true, store stdout in DataMovementStatusResponse.Message when the command is successful. Failure output is always contained in the message.
                                       , -1                        // The number of slots specified in the MPI hostfile. A value of 0 disables the use of slots in the hostfile. -1 will defer to the server side configuration.
                                       , -1                        // The number of max_slots specified in the MPI hostfile. A value of 0 disables the use of max_slots in the hostfile. -1 will defer to the server side configuration.
                                       , ""                        // Data movement profile.  Empty will default to the default profile.
                                     );
        CreateResponse create_response;

        RPCStatus status = client.Create(workflow, create_request, &create_response);
        if (!status.ok()) {
            std::cout << "Create RPC FAILED (" << status.error_code() << "): " << status.error_message() << std::endl;
            return 1;
        }

        switch (create_response.status()) {
            case CreateResponse::Status::STATUS_SUCCESS:
                uid = create_response.uid();
                std::cout << "Offload Created: UID: " << create_response.uid() << std::endl;
                break;
            default:
                std::cout << "Offload Create FAILED: " << create_response.status() << ": " << create_response.message() << std::endl;
                return 1;
        }
    }

    {
        ListRequest listRequest;
        ListResponse listResponse;

        RPCStatus status = client.List(workflow, listRequest, &listResponse);
        if (!status.ok()) {
            std::cout << "List RPC FAILED (" << status.error_code() << "): " << status.error_message() << std::endl;
            return 1;
        }

        std::cout << "Offload List: " << listResponse.uids().size() << std::endl;
        for (auto uid : listResponse.uids()) {
            std::cout << "\t" << uid << std::endl;
        }
    }
    
    {
        StatusRequest statusRequest(uid, 0);
        StatusResponse statusResponse;

RequestStatus:
        RPCStatus status = client.Status(workflow, statusRequest, &statusResponse);
        if (!status.ok()) {
            std::cout << "Status RPC FAILED (" << status.error_code() << "): " << status.error_message() << std::endl;
            return 1;
        }

        switch (statusResponse.state()) {
            case StatusResponse::STATE_PENDING:
            case StatusResponse::STATE_STARTING:
            case StatusResponse::STATE_RUNNING:
                std::cout << "Offload State Pending/Starting/Running" << std::endl;
                goto RequestStatus;
            case StatusResponse::STATE_COMPLETED:
                std::cout << "Offload State Completed" << std::endl;
                break;
            default:
                std::cout << "Offload State STATE UNKNOWN: " << statusResponse.state() << " Status: " << statusResponse.status() << std::endl;
                return 1;
        }

        switch (statusResponse.status()) {
            case StatusResponse::STATUS_SUCCESS:
                std::cout << "Offload Status Successful" << std::endl;
                break;
            default:
                std::cout << "Offload Status UNSUCCESSFUL: " << statusResponse.status() << " " << statusResponse.message() << std::endl;
                return 1;
        }
    }

    {
        CancelRequest cancelRequest(uid);
        CancelResponse cancelResponse;

        RPCStatus status = client.Cancel(workflow, cancelRequest, &cancelResponse);
        if (!status.ok()) {
            std::cout << "Cancel RPC FAILED (" << status.error_code() << "): " << status.error_message() << std::endl;
            return 1;
        }

        switch (cancelResponse.status()) {
            case CancelResponse::STATUS_SUCCESS:
                std::cout << "Offload Cancel Successful" << std::endl;
                break;
            default:
                std::cout << "Offload Cancel UNSUCCESSFUL: " << cancelResponse.status() << " " << cancelResponse.message() << std::endl;
                return 1;
        }
    }

    {
        DeleteRequest deleteRequest(uid);
        DeleteResponse deleteResponse;

        RPCStatus status = client.Delete(workflow, deleteRequest, &deleteResponse);
        if (!status.ok()) {
            std::cout << "Delete RPC FAILED (" << status.error_code() << "): " << status.error_message() << std::endl;
            return 1;
        }

        switch (deleteResponse.status()) {
            case DeleteResponse::STATUS_SUCCESS:
                std::cout << "Offload Delete Successful" << std::endl;
                break;
            default:
                std::cout << "Offload Delete UNSUCCESSFUL: " << deleteResponse.status() << " " << deleteResponse.message() << std::endl;
                return 1;
        }
    }

    return 0;
}
