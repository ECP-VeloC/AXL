/*
 * Copyright (c) 2009, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by Adam Moody <moody20@llnl.gov>.
 * LLNL-CODE-411039.
 * All rights reserved.
 * KVTree was originally part of The Scalable Checkpoint / Restart (SCR) library.
 * For details, see https://sourceforge.net/projects/scalablecr/
 * Please also read this file: LICENSE.TXT.
*/

#include "test_axl.h"
#include "test_axl_sync.h"

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <limits.h>

#define TEST_PASS (0)
#define TEST_FAIL (1)

#define TEST_FILE "test_file"
#define TEST_STRING "I am a file"

#define TEST_NAME "test transfer"
#define TEST_DEST "test_file_moved"

int test_axl_sync(){
  int rc = TEST_PASS;

  /* Create a file */
  FILE * fp = fopen(TEST_FILE, "w");
  fputs(TEST_STRING, fp);
  fclose(fp);

  /* Create dest path */
  char pwd[PATH_MAX];
  getcwd(pwd, sizeof(pwd));
  char* dest_path = malloc(strlen(pwd) + strlen(TEST_DEST) + 2);
  strcpy(dest_path, pwd);
  strcat(dest_path, "/");
  strcat(dest_path, TEST_DEST);

  /* Launch axl, reate a transfer, add test file, dispatch */
  if (AXL_Init(NULL) != AXL_SUCCESS) rc = TEST_FAIL;
  int id = AXL_Create("AXL_XFER_SYNC", TEST_NAME);
  if (AXL_Add(id, TEST_FILE, dest_path) != AXL_SUCCESS) rc = TEST_FAIL;
  if (AXL_Dispatch(id) != AXL_SUCCESS) rc = TEST_FAIL;

  /* Wait for transfer to complete and finalize axl */
  if (AXL_Wait(id) != AXL_SUCCESS) rc = TEST_FAIL;
  if (AXL_Finalize() != AXL_SUCCESS) rc = TEST_FAIL;

  /* Check that file arrived properly */
  FILE* dfp = fopen(dest_path, "r");
  if(!dfp) rc = TEST_FAIL;

  char* read_str = malloc(strlen(TEST_STRING) + 1);
  fgets(read_str, strlen(TEST_STRING) + 1, dfp);
  if(strcmp(read_str, TEST_STRING)) rc = TEST_FAIL;
  free(read_str);

  /* Unlink test files and return rc */
  unlink(TEST_FILE);
  unlink(dest_path);
  free(dest_path);
  return rc;
}

void test_axl_sync_init(){
  register_test(test_axl_sync, "test_axl_sync");
}
