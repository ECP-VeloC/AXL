/**************************************************************************/
/* Generic client example is used with connection-oriented server designs */
/**************************************************************************/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
	
#include "axl_svc.h"

int axlsvc_client()
{
  int    len, rc;
  int    sockfd;
  char   send_buf[80];
  char   recv_buf[80];
  struct sockaddr_in server;

  unsigned short port = AXLSVC_DEFAULT_PORT;
  struct hostent *hostnm;    /* server host name information        */


  hostnm = gethostbyname("rzwhippet18");
  if (hostnm == (struct hostent *) 0) {
      perror("Gethostbyname failed\n");
      return -1;
  }

  if ( (sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    perror("socket");
    return -1;
  }

  memset(&server, 0, sizeof(server));
  server.sin_family = AF_INET;
  server.sin_port = htons(port);
  server.sin_addr.s_addr = *((unsigned long *)hostnm->h_addr);

  if ( ( rc = connect(sockfd, (struct sockaddr *)&server, sizeof(server)) ) < 0) {
    perror("connect");
    close(sockfd);
    return -1;
  }
  printf("Connect completed.\n");

  printf("Enter message to be sent:\n");
  fgets(send_buf, 80, stdin);

  len = send(sockfd, send_buf, strlen(send_buf) + 1, 0);
  if (len != strlen(send_buf) + 1) {
    perror("send");
    close(sockfd);
    return -1;
  }
  printf("%d bytes sent\n", len);

  len = recv(sockfd, recv_buf, sizeof(recv_buf), 0);
  if (len != strlen(send_buf) + 1) {
    perror("recv");
    close(sockfd);
    exit(-1);
  }
  printf("%d bytes received: (%s)\n", len, recv_buf);

  close(sockfd);
  return 0;
}
