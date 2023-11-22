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

/*
 * Flag to state whether the AXL client/server mode of operation is enabled,
 * and if so, whether the code is running as the client or the server.
 */
alxsvc_RunMode axl_service_mode = AXLSVC_DISABLED;

static int axlsvc_socket = -1;

int axlsvc_client_init(char* host, unsigned short port)
{
  struct sockaddr_in server;
  struct hostent *hostnm = gethostbyname(host);

  if (hostnm == (struct hostent *) 0) {
    AXL_ERR("Gethostbyname failed: (%s)", strerror(errno));
    return 0;
  }

  if ( (axlsvc_socket = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    AXL_ERR("socket() failed: (%s)", strerror(errno));
    return 0;
  }

  memset(&server, 0, sizeof(server));
  server.sin_family = AF_INET;
  server.sin_port = htons(port);
  server.sin_addr.s_addr = *((unsigned long *)hostnm->h_addr);

  if ( connect(axlsvc_socket, (struct sockaddr *)&server, sizeof(server) ) < 0) {
    AXL_ERR("connect() failed: (%s)", strerror(errno));
    close(axlsvc_socket);
    return 0;
  }

  return 1;   // success
}

void axlsvc_client_finalize()
{
  if (axlsvc_socket >= 0)
    close(axlsvc_socket);
}

