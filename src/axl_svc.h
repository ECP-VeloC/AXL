#ifndef __AXLSVC_H__
#define __AXLSVC_H__

#define AXLSVC_TRUE				1
#define AXLSVC_FALSE				0
#define AXLSVC_DEFAULT_PORT	8888
	
#if defined(__cplusplus)
extern "C" {
#endif

typedef struct {
} axlsvc_request_t;

typedef struct {
} axlsvc_response_t;

int axlsvc_server();
int axlsvc_client();

#if defined(__cplusplus)
extern "C" }
#endif

#endif /* __AXLSVC_H__ */
