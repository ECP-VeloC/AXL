/* variable length args */
#include <stdarg.h>

/* stdout & stderr */
#include <stdio.h>

/* exit */
#include <stdlib.h>

/* gethostname */
#include <unistd.h>

#include <string.h>
#include <errno.h>
#include <sys/time.h>
#include <time.h>

/* axl version */
#include "axl.h"

/* current debug level for AXL library,
 * set in AXL_Init used in axl_dbg */
int axl_debug;

static void axl_print_date_and_hostinfo(const char* title) 
{
    char hostname[256];
    if (gethostname(hostname, sizeof(hostname)) != 0) {
        /* TODO: error! */
    }

    struct timeval curTime;
    gettimeofday(&curTime, NULL);
    int milli = curTime.tv_usec / 1000;

    time_t rawtime;
    struct tm * timeinfo;
    char buffer[80];

    time(&rawtime);
    timeinfo = localtime(&rawtime);

    strftime(buffer, 80, "%Y-%m-%d %H:%M:%S", timeinfo);

    char currentTime[84] = "";
    sprintf(currentTime, "%s:%d", buffer, milli);

    fprintf(stderr, "%s %s:%d %s:", currentTime, title, getpid(), hostname);
}

/* print message to stderr if axl_debug is set and it is >= level */
void axl_dbg(int level, const char* fmt, ...)
{
    if (level == 0 || (axl_debug > 0 && axl_debug >= level)) {
        axl_print_date_and_hostinfo("AXL");

        va_list argp;
        va_start(argp, fmt);
        vfprintf(stderr, fmt, argp);
        va_end(argp);
        fprintf(stderr, "\n");
    }
}

/* print error message to stderr */
void axl_err(const char* fmt, ...)
{
    axl_print_date_and_hostinfo("AXL ERROR");
  
    va_list argp;
    va_start(argp, fmt);
    vfprintf(stderr, fmt, argp);
    va_end(argp);
    fprintf(stderr, "\n");
}

/* print abort message and kill run */
void axl_abort(int rc, const char* fmt, ...)
{
    axl_print_date_and_hostinfo("AXL ABORT");

    va_list argp;
    va_start(argp, fmt);
    vfprintf(stderr, fmt, argp);
    va_end(argp);
    fprintf(stderr, "\n");

    exit(rc);
}
