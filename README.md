# AXL: Asynchronous Transfer Library

[![axl build](https://github.com/ECP-VeloC/AXL/actions/workflows/build-and-test.yml/badge.svg)](https://github.com/ECP-VeloC/AXL/actions/workflows/build-and-test.yml)

The Asynchronous Transfer Library provides a common C interface to transfer files
between layers in an HPC storage hierarchy.

For details on its usage, see [doc/README.md](doc/README.md).
For API details, see [AXL User API docs](https://ecp-veloc.github.io/component-user-docs/group__axl.html).

## Quickstart

AXL uses the CMake build system and we recommend out-of-source builds.

```shell
git clone https://github.com/ECP-VeloC/AXL.git
mkdir build
mkdir install

cd build
cmake -DCMAKE_INSTALL_PREFIX=../install ../AXL
make
make install
make test
```

Some useful CMake command line options:

- `-DCMAKE_INSTALL_PREFIX=[path]`: Place to install the AXL library
- `-DCMAKE_BUILD_TYPE=[Debug/Release]`: Build with debugging or optimizations
- `-DMPI`: Build support for MPI collective interface

For building with IBM BB API (optional):

- `-DAXL_ASYNC_API=IBM_BBAPI`: Enable IBM BB API
- `-DWITH_BBAPI_PREFIX=/opt/ibm/bb`: Install path to IBM BB library

### Dependencies

- C
- CMake, Version 3.14+
- [KVTree](https://github.com/LLNL/KVTree)
- MPI (optional)

## Authors

AXL was originally part of the [SCR Library](https://github.com/llnl/scr).

AXL is part of the SCR project (LLNL-CODE-411039)

Numerous people have [contributed](https://github.com/llnl/scr/graphs/contributors) to the SCR project.

To reference SCR in a publication, please cite the following paper:

* Adam Moody, Greg Bronevetsky, Kathryn Mohror, Bronis R. de Supinski, [Design, Modeling, and Evaluation of a Scalable Multi-level Checkpointing System](http://dl.acm.org/citation.cfm?id=1884666), LLNL-CONF-427742, Supercomputing 2010, New Orleans, LA, November 2010.

Additional information and research publications can be found here:

[http://computation.llnl.gov/projects/scalable-checkpoint-restart-for-mpi](http://computation.llnl.gov/projects/scalable-checkpoint-restart-for-mpi)

## Release

Copyright (c) 2018, Lawrence Livermore National Security, LLC.
Produced at the Lawrence Livermore National Laboratory.
<br>
Copyright (c) 2018, UChicago Argonne LLC, operator of Argonne National Laboratory.


For release details and restrictions, please read the [LICENSE](https://github.com/LLNL/AXL/blob/main/LICENSE) and [NOTICE](https://github.com/LLNL/AXL/blob/main/NOTICE) files.

`LLNL-CODE-751725` `OCEC-18-060`
