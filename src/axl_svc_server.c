#include <stdio.h> 
#include <string.h> //strlen 
#include <stdlib.h> 
#include <errno.h> 
#include <unistd.h> //close 
#include <arpa/inet.h> //close 
#include <sys/types.h> 
#include <sys/socket.h> 
#include <netinet/in.h> 
#include <sys/time.h> //FD_SET, FD_ISSET, FD_ZERO macros 
	
#include "axl_svc.h"
	
static void execute_command(const char* pname);

int axlsvc_server()
{
  const int max_bufsize = 4096;
  const int max_clients = 30;

  int opt = AXLSVC_TRUE;
  int service_socket, addrlen, new_socket, client_socket[max_clients];
  int activity, valread, sd;
  int max_sd;
  struct sockaddr_in address;

  char buffer[max_bufsize];  //data buffer of 4K
  fd_set readfds;     //set of socket descriptors

  for (int i = 0; i < max_clients; i++)
    client_socket[i] = 0;

  if ((service_socket = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
    perror("socket failed"); 
    exit(EXIT_FAILURE); 
  } 

  if (setsockopt(service_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&opt, sizeof(opt)) < 0 ) {
    perror("setsockopt"); 
    exit(EXIT_FAILURE); 
  } 

  address.sin_family = AF_INET; 
  address.sin_addr.s_addr = INADDR_ANY; 
  address.sin_port = htons(AXLSVC_DEFAULT_PORT);

  if (bind(service_socket, (struct sockaddr *)&address, sizeof(address)) < 0) {
    perror("bind failed"); 
    exit(EXIT_FAILURE); 
  } 
  printf("Listener on port %d \n", AXLSVC_DEFAULT_PORT);

  if (listen(service_socket, max_clients) < 0) {
    perror("listen"); 
    exit(EXIT_FAILURE); 
  } 

  addrlen = sizeof(address); 
  puts("Waiting for connections ..."); 

  while(AXLSVC_TRUE) {
    FD_ZERO(&readfds);
    FD_SET(service_socket, &readfds);
    max_sd = service_socket;

    for ( int i = 0 ; i < max_clients ; i++) {
      sd = client_socket[i];  // socket descriptor

      if (sd > 0)
        FD_SET(sd, &readfds);

      if (sd > max_sd)
        max_sd = sd; 
    } 

    activity = select( max_sd + 1 , &readfds , NULL , NULL , NULL); 

    if ((activity < 0) && (errno!=EINTR))
      printf("select error");

    // If something happened on the service socket, then its an incoming connection
    if (FD_ISSET(service_socket, &readfds)) {
      if ((new_socket = accept(service_socket, (struct sockaddr *)&address, (socklen_t*)&addrlen)) < 0) {
        perror("accept"); 
        exit(EXIT_FAILURE); 
      } 

      //inform user of socket number - used in send and receive commands 
      printf("New connection , socket fd is %d , ip is : %s , port : %d\n",
        new_socket , inet_ntoa(address.sin_addr) , ntohs(address.sin_port)); 

      //add new socket to array of sockets 
      for (int i = 0; i < max_clients; i++) {
        //if position is empty 
        if( client_socket[i] == 0 ) { 
          client_socket[i] = new_socket; 
          printf("Adding to list of sockets as %d\n" , i); 
          break; 
        } 
      } 
    } 

    for ( int i = 0; i < max_clients; i++) {
      sd = client_socket[i]; 

      if (FD_ISSET(sd , &readfds)) {
        if ((valread = read( sd , buffer, 1024)) == 0) {
          getpeername(sd , (struct sockaddr*)&address , (socklen_t*)&addrlen); 
          printf("Host disconnected , ip %s , port %d \n", 
              inet_ntoa(address.sin_addr) , ntohs(address.sin_port)); 

          close( sd ); 
          client_socket[i] = 0; 
        } else {
          for (int i = 0; i < valread; i++)
            if (buffer[i] == '\r' || buffer[i] == '\n')
              buffer[i] = ' ';
          buffer[valread] = '\0'; 
          // execute_command(buffer);
          send(sd , buffer , strlen(buffer) , 0 ); 
        } 
      } 
    } 
  } 

  return 0; 
} 

static void execute_command(const char* pname)
{
  FILE *p;
  int ch;

  if ((p = popen(pname, "r")) == NULL) {
    perror(pname);
    return;
  }

  printf("%s\n", pname);

  while( ( ch = fgetc(p)) != EOF ) {
    putchar(ch);
  }
  pclose(p);
}
