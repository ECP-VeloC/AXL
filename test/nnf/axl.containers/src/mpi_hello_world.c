// Author: Wes Kendall
// Copyright 2011 www.mpitutorial.com
// This code is provided freely with the tutorials on mpitutorial.com. Feel
// free to modify it for your own use. Any distribution of the code must
// either provide a link to www.mpitutorial.com or keep this header intact.
//
// An intro MPI hello world program that uses MPI_Init, MPI_Comm_size,
// MPI_Comm_rank, MPI_Finalize, and MPI_Get_processor_name.
//
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <fcntl.h>

#include <mpi.h>

int main(int argc, char **argv)
{
  char nnf_storage_path[PATH_MAX];
  char global_storage_path[PATH_MAX];
  char host_name[PATH_MAX];

  // Initialize the MPI environment. The two arguments to MPI Init are not
  // currently used by MPI implementations, but are there in case future
  // implementations might need the arguments.
  MPI_Init(NULL, NULL);

  // Get the number of processes
  int world_size;
  MPI_Comm_size(MPI_COMM_WORLD, &world_size);

  // Get the rank of the process
  int world_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

  // Get the name of the processor
  char processor_name[MPI_MAX_PROCESSOR_NAME];
  int name_len;
  MPI_Get_processor_name(processor_name, &name_len);

  if (argc < 3) {
    printf("Usage: %s nnf_storage_dir global_storage_dir\n", argv[0]);
    return -1;
  }
  strncpy(nnf_storage_path, argv[1], PATH_MAX);
  strncpy(global_storage_path, argv[2], PATH_MAX);

  if (gethostname(host_name, PATH_MAX) < 0) {
    printf("%s: gethostname failed\n", argv[0]);
  }

  // Print off a hello world message
  printf("%s: processor %s, rank %d out of %d processors. NNF Storage path: %s, Global Storage path is: %s\n",
         host_name, processor_name, world_rank, world_size, nnf_storage_path, global_storage_path);

  // Finalize the MPI environment. No more MPI calls can be made after this
  MPI_Finalize();
}
