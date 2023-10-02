# Start from the NearNodeFlash MPI File Utils Image that is used for Data Movement. This is an easy
# way to get MPI File Utils/mpi-operator as a base. Use a stage for building the application.
# Alernatively, use mpi-operator's image directly:
# FROM mpioperator/openmpi:0.3.0 AS build
FROM ghcr.io/nearnodeflash/nnf-mfu:master AS build

# Install build tools
RUN apt update
RUN apt install -y make libopenmpi-dev

# Build application
WORKDIR /src
COPY src .
RUN make

# Final stage - start fresh and don't carry over the build artifacts
FROM ghcr.io/nearnodeflash/nnf-mfu:master

# Copy application from build stage into final stage
COPY --from=build /src/mpi_hello_world /usr/bin/mpi_hello_world
