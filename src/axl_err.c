/*
 * Copyright (c) 2009, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by Adam Moody <moody20@llnl.gov>.
 * LLNL-CODE-411039.
 * All rights reserved.
 * This file is part of The Scalable Checkpoint / Restart (SCR) library.
 * For details, see https://sourceforge.net/projects/scalablecr/
 * Please also read this file: LICENSE.TXT.
*/

/* print message to stdout if axl_debug is set and it is >= level */
void axl_dbg(int level, const char *fmt, ...)
{
  /* get my hostname */
  char hostname[256];
  if (gethostname(hostname, sizeof(hostname)) != 0) {
    /* TODO: error! */
  }

  va_list argp;
  /*
  if (level == 0 || (scr_debug > 0 && scr_debug >= level)) {
  */
    fprintf(stdout, "AXL %s: %s: ", AXL_VERSION, hostname);
    va_start(argp, fmt);
    vfprintf(stdout, fmt, argp);
    va_end(argp);
    fprintf(stdout, "\n");
  /*
  }
  */
}

/* print error message to stdout */
void axl_err(const char *fmt, ...)
{
  /* get my hostname */
  char hostname[256];
  if (gethostname(hostname, sizeof(hostname)) != 0) {
    /* TODO: error! */
  }

  va_list argp;
  fprintf(stdout, "AXL %s ERROR: %s: ", AXL_VERSION, hostname);
  va_start(argp, fmt);
  vfprintf(stdout, fmt, argp);
  va_end(argp);
  fprintf(stdout, "\n");
}
