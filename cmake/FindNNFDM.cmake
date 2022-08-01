# - Try to find libnnfdm
#
# Once done this will define
#  NNFDM_FOUND - System has libdatawarp
#  NNFDM_INCLUDE_DIRS - The libdatawarp include directories
#  NNFDM_LIBRARIES - The libraries needed to use libdatawarp
#
# This is early days for this library.  For now, we assume that there is
# a the following directory and contents pointed to by WITH_NNFDM_PREFIX:
#
# path/nnfdm/
#   lib64/
#    libnnfdm.a
#   include/
#    datamovement.pb-c.h
#    nnfdm.h
#
# So, the following cmake line will cause this to be found assuming the prefix exists:
# cmake -DWITH_NNFDM_PREFIX="/usr/WS2/martymcf/scr/dm/nnfdm" -DMPI=ON ..

FIND_LIBRARY(NNFDM_LIBRARIES
    NAMES nnfdm
    HINTS ${WITH_NNFDM_PREFIX}/lib64
)

FIND_PATH(NNFDM_INCLUDE_DIRS
    NAMES nnfdm.h
    HINTS ${WITH_NNFDM_PREFIX}/include
)

INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(NNFDM DEFAULT_MSG
  NNFDM_LIBRARIES
  NNFDM_INCLUDE_DIRS
)

# Hide these vars from ccmake GUI
MARK_AS_ADVANCED(
  NNFDM_LIBRARIES
  NNFDM_INCLUDE_DIRS
)
