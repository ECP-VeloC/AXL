#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <errno.h>

#include "axl_internal.h"
#include "axl_service.h"

#define AXLSVC_MAX_CLIENTS 16
struct axl_connection_ctx {
  int sd;                         /* Connection to our socket */
  struct axl_transfer_array xfr;  /* Pointer to client-specific xfer array */
} axl_connection_ctx_array[AXLSVC_MAX_CLIENTS];

static ssize_t service_request_from_client(int sd)
{
  ssize_t bytecount;
  axlsvc_Request req;
  axlsvc_Response response;
  char* buffer;

  bytecount = axl_read("AXLSVC Client Reqeust", sd, &req, sizeof(req));

  if (bytecount == 0) {
    AXL_DBG(2, "Client for socket %d closed", sd);
    return bytecount;
  }

  buffer = malloc(req.payload_length);

  bytecount = axl_read("AXLSVC Reqeust Payload", sd, &buffer, req.payload_length);

  if (bytecount != req.payload_length) {
    AXL_ABORT(-1, "Unexpected Payload Length: Expected %d, Got %d", req.payload_length, bytecount);
  }

  switch (req.request) {
    case AXLSVC_AXL_CONFIG:
      AXL_DBG(1, "AXLSVC_AXL_CONFIG(kfile=%s", buffer);
      response.response = AXLSVC_SUCCESS;
      response.payload_length = 0;
      bytecount = axl_write_attempt("AXLSVC Response to Client", sd, &response, sizeof(response));
      if (bytecount != sizeof(response)) {
        AXL_ABORT(-1, "Unexpected Write Response to client: Expected %d, Got %d",
                      sizeof(response), bytecount);
      }
      break;
    default:
      AXL_ABORT(-1, "AXLSVC Unknown Request Type %d", req.request);
      break;
  }

  free(buffer);
  return bytecount;
}

int axlsvc_server_run(int port)
{
  int server_socket;
  int opt = 1;
  struct sockaddr_in address;
  int addrlen;
  int new_socket;
  fd_set readfds;
  int activity;
  int max_sd;

  if ((server_socket = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
    AXL_ABORT(-1, "socket() failed: (%s)", strerror(errno));
  }

  if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR,
                                            (char *)&opt, sizeof(opt)) < 0 ) {
    AXL_ABORT(-1, "setsockopt() failed: (%s)", strerror(errno));
  }

  address.sin_family = AF_INET;
  address.sin_addr.s_addr = INADDR_ANY;
  address.sin_port = htons(port);

  if (bind(server_socket, (struct sockaddr *)&address, sizeof(address)) < 0) {
    AXL_ABORT(-1, "bind() failed: (%s)", strerror(errno));
  }

  if (listen(server_socket, AXLSVC_MAX_CLIENTS) < 0) {
    AXL_ABORT(-1, "listen() failed: (%s)", strerror(errno));
  }

  addrlen = sizeof(address);

  while (1) {
    FD_ZERO(&readfds);
    FD_SET(server_socket, &readfds);
    max_sd = server_socket;

    for (int i = 0 ; i < AXLSVC_MAX_CLIENTS ; i++) {
      if (axl_connection_ctx_array[i].sd > 0)
        FD_SET(axl_connection_ctx_array[i].sd, &readfds);

      if (axl_connection_ctx_array[i].sd > max_sd)
        max_sd = axl_connection_ctx_array[i].sd;
    }

    activity = select(max_sd + 1 , &readfds , NULL , NULL , NULL);

    if (activity < 0 && errno != EINTR) {
      AXL_ABORT(-1, "select() error: (%s)", strerror(errno));
    }

    if (FD_ISSET(server_socket, &readfds)) {
      AXL_DBG(1, "Accepting new incomming connection");
      if ((new_socket = accept(server_socket, (struct sockaddr *)&address,
                                                (socklen_t*)&addrlen)) < 0) {
        AXL_ABORT(-1, "accept() error: (%s)", strerror(errno));
      }

      for (int i = 0; i < AXLSVC_MAX_CLIENTS; i++) {
        if(axl_connection_ctx_array[i].sd == 0 ){
          axl_connection_ctx_array[i].sd = new_socket;
          break;
        }
      }
      AXL_DBG(2, "Connection established");
    }

    for ( int i = 0; i < AXLSVC_MAX_CLIENTS; i++) {
      if (FD_ISSET(axl_connection_ctx_array[i].sd , &readfds)) {
        axl_xfer_list = &axl_connection_ctx_array[i].xfr;

        if (service_request_from_client(axl_connection_ctx_array[i].sd) == 0) {
          AXL_DBG(2, "Closing server side socket(%d) to client", axl_connection_ctx_array[i].sd);
          close(axl_connection_ctx_array[i].sd);
          axl_connection_ctx_array[i].sd = 0;
          /*TODO: Free up memory used for acx_kvtrees */
        }
      }
    }
  }

  return 0;
}

int main(int argc , char *argv[])
{
  int rval = -1;

  if (argc == 2 && atoi(argv[1]) > 0) {
    axl_service_mode = AXLSVC_SERVER;
    memset(axl_connection_ctx_array, 0, sizeof(axl_connection_ctx_array));

    if ( (rval = AXL_Init()) == AXL_SUCCESS)
      rval = axlsvc_server_run(atoi(argv[1]));
  } else {
    fprintf(stderr, "Usage: %s <port number>\n", argv[0]);
  }

  return rval;
}