#include <errno.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <string.h>
#include <signal.h>

#include "axl.h"

#define AXLCS_SUCCESS					0
#define AXLCS_CLIENT_INVALID			1
#define AXLCS_SERVICE_CREATION_FAILURE	1000
#define AXLCS_SERVICE_KILLED			2000
#define AXLCS_SERVICE_FAIL 				3000

static int time_to_leave = 0;

void sigterm_handler(int sig, siginfo_t* info, void* ucontext)
{
	time_to_leave++;
}

int run_service()
{
	struct sigaction act = {0};

	act.sa_flags = 0;
	sigemptyset(&act.sa_mask);
	act.sa_sigaction = sigterm_handler;
	if (sigaction(SIGTERM, &act, NULL) == -1) {
		perror("sigaction");
		return AXLCS_SERVICE_FAIL;
	}

	fprintf(stdout, "Service Started!\n");
	
	for (int i = 0; !time_to_leave && i < 100000; ++i) {
		int seconds = 2+i;
		fprintf(stdout, "Sleeping %d seconds\n", seconds);
		fprintf(stderr, "Just testing stderr. %d ..\n", seconds);
		sleep(seconds);
	}

	fprintf(stdout, "Service Ending!\n");
	return AXLCS_SUCCESS;
}

int run_client()
{
	fprintf(stdout, "Client Started!\n");
	sleep(2);
	fprintf(stdout, "Client Ending!\n");
	return AXLCS_SUCCESS;
}

int main(int ac, char **av)
{
	fprintf(stderr, "Just testing stderr...\n");
	if (ac != 2) {
		fprintf(stderr, "Command count (%d) incorrect:\nUsage: test_client_server <client|server>\n", ac);
		return AXLCS_CLIENT_INVALID;
	}

	if (strcmp("server", av[1]) == 0) {
		return run_service();
	}
	else if (strcmp("client", av[1]) == 0) {
		return run_client();
	}

	fprintf(stderr, "Unknown Argument (%s) incorrect:\nUsage: test_client_server <client|server>\n", av[1]);
	return AXLCS_CLIENT_INVALID;
}