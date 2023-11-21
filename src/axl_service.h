#ifndef __AXLSVC_H__
#define __AXLSVC_H__

#include <stddef.h>

extern int axl_use_service;   /* whether to use AXL service instead of library */

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

#if defined(__cplusplus)
extern "C" {
#endif

int axlsvc_client_init(char* host, unsigned short port);
void axlsvc_client_finalize();

#if defined(__cplusplus)
extern "C" }
#endif

#endif /* __AXLSVC_H__ */
