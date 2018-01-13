# AXL: Asynchronous Transfer Library

The Asynchronous Transfer Library provides a common C interface to many vendor-specific NVMeoF technologies.
Despite the name, AXL provides a synchronous transfer method as well.

AXL was originally part of the SCR Library.

## Quickstart

AXL uses the CMake build system and we recommend out-of-source builds.

```shell
git clone git@github.com:llnl/axl.git
mkdir build
mkdir install

cd build
cmake -DCMAKE_INSTALL_PREFIX=../install ../kvtree
make
make install
make test
```

Some useful CMake command line options:

- `-DCMAKE_INSTALL_PREFIX=[path]`: Place to install the KVTree library
- `-DCMAKE_BUILD_TYPE=[Debug/Release]`: Build with debugging or optimizations

### Dependencies

- C
- CMake, Version 2.8+

## Authors

AXL is part of the SCR project

Numerous people have [contributed](https://github.com/llnl/scr/graphs/contributors) to the SCR project.

To reference SCR in a publication, please cite the following paper:

* Adam Moody, Greg Bronevetsky, Kathryn Mohror, Bronis R. de Supinski, [Design, Modeling, and Evaluation of a Scalable Multi-level Checkpointing System](http://dl.acm.org/citation.cfm?id=1884666), LLNL-CONF-427742, Supercomputing 2010, New Orleans, LA, November 2010.

Additional information and research publications can be found here:

[http://computation.llnl.gov/projects/scalable-checkpoint-restart-for-mpi](http://computation.llnl.gov/projects/scalable-checkpoint-restart-for-mpi)
