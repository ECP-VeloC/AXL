#include <string.h>
#include "../src/axl_svc.h"

int main(int argc , char *argv[]) 
{
  int rval = -1;
  int is_server = AXLSVC_FALSE;

  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "server") == 0) {
      is_server=AXLSVC_TRUE;
    }
  }

  if (is_server) {
    rval = run_server();
  }
  else {
    rval = axlsvc_client();
  }

  return rval;
}
