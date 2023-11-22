#ifndef __AXLSVC_H__
#define __AXLSVC_H__

#include <stddef.h>

#if defined(__cplusplus)
extern "C" {
#endif

typedef enum {
   AXLSVC_DISABLED = 0, /* Default - Not utilizing AXL service (lib only) */
   AXLSVC_CLIENT = 1,   /* Using AXL service and we are the client */
   AXLSVC_SERVER = 2    /* Using AXL service and we are the server */
} alxsvc_RunMode;

/*
 * Flag to state whether the AXL client/server mode of operation is enabled,
 * and if so, whether the code is running as the client or the server.
 */
extern alxsvc_RunMode axl_service_mode;

typedef enum {
   AXLSVC_AXL_CONFIG = 0,     /* payload is config ktree file path */
} axlsvc_request_t;

typedef struct {
   axlsvc_request_t request;
   ssize_t payload_length;
} axlsvc_Request;

typedef enum {
   AXLSVC_SUCCESS =  0,
   AXLSVC_FAILURE = -1,
} axlsvc_response_t;

typedef struct {
   axlsvc_response_t response;
   ssize_t payload_length;    // Optional error/status string
} axlsvc_Response;


int axlsvc_client_init(char* host, unsigned short port);
void axlsvc_client_finalize();

#if defined(__cplusplus)
extern "C" }
#endif

#endif /* __AXLSVC_H__ */
