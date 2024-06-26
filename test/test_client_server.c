#include <errno.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <string.h>

#include "axl.h"

int wait_for_service_to_complete()
{
	int pid;
	int wstatus = 0;

	if ((pid = wait(&wstatus)) < 0) {
		fprintf(stderr, "Wait for service process failed: [%d] - %s\n", errno, strerror(errno));
		return 1;
	}

	return WEXITSTATUS(wstatus);
}

int run_service()
{
	return 0;
}

int run_client()
{
	int client_status = 0;

	return client_status + wait_for_service_to_complete();
}

int main()
{
	int pid;

	if ((pid = fork()) < 0) {
		fprintf(stderr, "Creation of service failed: [%d] - %s\n", errno, strerror(errno));
		return 2;
	}
	else if (pid == 0) {
		return run_service();
	}
	else {
		return run_client();
	}
}