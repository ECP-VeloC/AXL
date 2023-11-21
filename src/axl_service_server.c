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

static ssize_t service_request_from_client(int sd)
{
  ssize_t bytecount;
  axlsvc_Request req;
  axlsvc_Response response;
  char* buffer;

  bytecount = axl_read("AXLSVC Client Reqeust", sd, &req, sizeof(req));

  if (bytecount == 0) {
    AXL_DBG(0, "Client for socket %d closed", sd);
    return bytecount;
  }

  buffer = malloc(req.payload_length);

  bytecount = axl_read("AXLSVC Reqeust Payload", sd, &buffer, req.payload_length);

  if (bytecount != req.payload_length) {
    AXL_ABORT(-1, "Unexpected Payload Length: Expected %d, Got %d", req.payload_length, bytecount);
  }

  switch (req.request) {
    case AXLSVC_AXL_CONFIG:
      AXL_DBG(0, "AXLSVC_AXL_CONFIG(kfile=%s", buffer);
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
  static const int axlsvc_max_clients = 16;
  int client_socket[axlsvc_max_clients];
  int server_socket;
  int opt = 1;
  struct sockaddr_in address;
  int addrlen;
  int new_socket;
  fd_set readfds;
  int activity, sd;
  int max_sd;

  for (int i = 0; i < axlsvc_max_clients; i++)
    client_socket[i] = 0;

  if ((server_socket = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
    AXL_ABORT(-1, "socket() failed: (%s)", strerror(errno));
  }

  if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&opt, sizeof(opt)) < 0 ) {
    AXL_ABORT(-1, "setsockopt() failed: (%s)", strerror(errno));
  }

  address.sin_family = AF_INET;
  address.sin_addr.s_addr = INADDR_ANY;
  address.sin_port = htons(port);

  if (bind(server_socket, (struct sockaddr *)&address, sizeof(address)) < 0) {
    AXL_ABORT(-1, "bind() failed: (%s)", strerror(errno));
  }

  if (listen(server_socket, axlsvc_max_clients) < 0) {
    AXL_ABORT(-1, "listen() failed: (%s)", strerror(errno));
  }

  addrlen = sizeof(address);

  while (1) {
    FD_ZERO(&readfds);
    FD_SET(server_socket, &readfds);
    max_sd = server_socket;

    for (int i = 0 ; i < axlsvc_max_clients ; i++) {
      sd = client_socket[i];

      if (sd > 0)
        FD_SET(sd, &readfds);

      if (sd > max_sd)
        max_sd = sd;
    }

    activity = select(max_sd + 1 , &readfds , NULL , NULL , NULL);

    if (activity < 0 && errno != EINTR) {
      AXL_ABORT(-1, "select() error: (%s)", strerror(errno));
    }

    // If something happened on the service socket, then its an incoming connection
    if (FD_ISSET(server_socket, &readfds)) {
      AXL_DBG(0, "Accepting incomming connection");
      if ((new_socket = accept(server_socket, (struct sockaddr *)&address, (socklen_t*)&addrlen)) < 0) {
        AXL_ABORT(-1, "accept() error: (%s)", strerror(errno));
      }

      for (int i = 0; i < axlsvc_max_clients; i++) {
        if( client_socket[i] == 0 ) {
          client_socket[i] = new_socket;
          break;
        }
      }
      AXL_DBG(0, "Connection established");

    }

    for ( int i = 0; i < axlsvc_max_clients; i++) {
      sd = client_socket[i];

      if (FD_ISSET(sd , &readfds)) {
        if (service_request_from_client(sd) == 0) {
          AXL_DBG(0, "Closing server side socket(%d) to client", sd);
          close(sd);
          client_socket[i] = 0;
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
    rval = axlsvc_server_run(atoi(argv[1]));
  } else {
    fprintf(stderr, "Usage: %s <port number>\n", argv[0]);
  }

  return rval;
}