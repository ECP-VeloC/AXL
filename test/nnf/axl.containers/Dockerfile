#
# This image contains the NNF MPI File Utils Image that is also used for Data Movement.
# On top of that, we place a simple mpi_hello_world program that will take two arguments
# as input: 
#          1) The name of the rabbit-local directory ($DW_JOB_storage)
#          2) The name of the global/shared Lustre directory
#
# This image is loaded
#
# IMPORTANT IMAGE BUILD NOTES:
#
# This image must be built on an x86 LC machine that is configure with SLURM.  This image will fail
# to build on flux machines (toss4).
#
# Steps to successful image build:
#   ssh oslic
#   podman login -u <your_docker_account_name> docker.io/<your_docker_account_name>/axl
#   salloc --userns --exclusive
#   podman build -t docker.io/<your_docker_account_name/axl .
#   podman push docker.io/mcfadden8/axl
#   exit
#   logout
FROM ghcr.io/nearnodeflash/nnf-mfu:master AS build

# Install build tools
RUN apt update
RUN apt install -y make libopenmpi-dev

# Build application
WORKDIR /src
COPY mpiGraph .
RUN make

# Final stage - start fresh and don't carry over the build artifacts
FROM ghcr.io/nearnodeflash/nnf-mfu:master

# Copy application from build stage into final stage
COPY --from=build /src/mpiGraph /usr/bin/mpiGraph
