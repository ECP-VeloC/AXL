CMAKE_MINIMUM_REQUIRED(VERSION 3.14)
PROJECT(AXL VERSION 0.9.0)

# Mac rpath Policy
IF(POLICY CMP0042)
    CMAKE_POLICY(SET CMP0042 NEW)
ENDIF(POLICY CMP0042)
SET(CMAKE_MACOSX_RPATH ON)

# Linux rpath settings
SET(CMAKE_SKIP_BUILD_RPATH FALSE)
SET(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)
SET(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

SET(CMAKE_POSITION_INDEPENDENT_CODE ON)

INCLUDE(GNUInstallDirs)

# Configuration Options
OPTION(MPI "Enable MPI operations for AXL" OFF)
MESSAGE(STATUS "MPI: ${MPI}")

OPTION(BUILD_SHARED_LIBS "Whether to build shared libraries" ON)
MESSAGE(STATUS "BUILD_SHARED_LIBS: ${BUILD_SHARED_LIBS}")

if(BUILD_SHARED_LIBS)
   OPTION(AXL_LINK_STATIC "Default to static linking? (Needed for Cray)" OFF)
else()
   OPTION(AXL_LINK_STATIC "Default to static linking? (Needed for Cray)" ON)
endif()
MESSAGE(STATUS "AXL_LINK_STATIC: ${AXL_LINK_STATIC}")

OPTION(ENABLE_PTHREADS "Whether to enable pthreads support" ON)
MESSAGE(STATUS "ENABLE_PTHREADS: ${ENABLE_PTHREADS}")

OPTION(ENABLE_IBM_BBAPI "Whether to enable IBM Burst Buffer support" ON)
MESSAGE(STATUS "ENABLE_IBM_BBAPI: ${ENABLE_IBM_BBAPI}")

OPTION(ENABLE_CRAY_DW "Whether to enable Cray Datawarp support" OFF)
MESSAGE(STATUS "ENABLE_CRAY_DW: ${ENABLE_CRAY_DW}")

OPTION(ENABLE_HPE_NNFDM "Whether to enable HPE Data Mover support" ON)
MESSAGE(STATUS "ENABLE_HPE_NNFDM: ${ENABLE_HPE_NNFDM}")

OPTION(ENABLE_TESTS "Whether to build tests" ON)
MESSAGE(STATUS "ENABLE_TESTS: ${ENABLE_TESTS}")

SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall")

# Find Packages & Files

LIST(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")

## MPI
IF(MPI)
    INCLUDE(SetupMPI)
    IF(NOT MPI_C_FOUND)
        MESSAGE(FATAL_ERROR
            "Could not find MPI! "
            "Either add an MPI compiler to your path "
            "or force CMake to build using the correct compiler (`export CC=mpicc`). "
            "To disable MPI, set -DMPI=OFF")
    ENDIF(NOT MPI_C_FOUND)
ENDIF(MPI)

## KVTREE
FIND_PACKAGE(kvtree REQUIRED)
LIST(APPEND AXL_EXTERNAL_LIBS kvtree::kvtree_base)
LIST(APPEND AXL_EXTERNAL_STATIC_LIBS kvtree::kvtree_base-static)


## DataWarp
IF(ENABLE_CRAY_DW)
    FIND_PACKAGE(DataWarp REQUIRED)
    SET(HAVE_DATAWARP TRUE)
    INCLUDE_DIRECTORIES(${DATAWARP_INCLUDE_DIRS})
    LIST(APPEND AXL_EXTERNAL_LIBS ${DATAWARP_LIBRARIES})
    LIST(APPEND AXL_EXTERNAL_STATIC_LIBS ${DATAWARP_LIBRARIES})
ENDIF(ENABLE_CRAY_DW)

## DataMover
IF(ENABLE_HPE_NNFDM)
  FIND_PACKAGE(NNFDM REQUIRED)
  IF(NNFDM_FOUND)
    SET(HAVE_NNFDM TRUE)
    INCLUDE_DIRECTORIES(${NNFDM_INCLUDE_DIR})
    MESSAGE(STATUS "NNFDM_INCLUDE_DIR: ${NNFDM_INCLUDE_DIR}")
    MESSAGE(STATUS "NNFDM_LIBRARIES: ${NNFDM_LIBRARIES}")
    LIST(APPEND AXL_EXTERNAL_LIBS ${NNFDM_LIBRARIES})
    LIST(APPEND AXL_EXTERNAL_STATIC_LIBS ${NNFDM_LIBRARIES})
  ENDIF(NNFDM_FOUND)
ENDIF(ENABLE_HPE_NNFDM)

## IBM Burst Buffer API
IF(ENABLE_IBM_BBAPI)
    FIND_PACKAGE(BBAPI)
    IF(BBAPI_FOUND)
        SET(HAVE_BBAPI TRUE)

        SET(ENABLE_BBAPI_FALLBACK OFF CACHE BOOL "Fallback to a different transfer type if BBAPI not supported")
        IF(${ENABLE_BBAPI_FALLBACK})
            SET(HAVE_BBAPI_FALLBACK TRUE)
        ENDIF(${ENABLE_BBAPI_FALLBACK})

        INCLUDE_DIRECTORIES(${BBAPI_INCLUDE_DIRS})
        LIST(APPEND AXL_EXTERNAL_LIBS ${BBAPI_LIBRARIES})
        LIST(APPEND AXL_EXTERNAL_STATIC_LIBS ${BBAPI_LIBRARIES})
    ENDIF(BBAPI_FOUND)
ENDIF(ENABLE_IBM_BBAPI)

## ZLIB
FIND_PACKAGE(ZLIB REQUIRED)
LIST(APPEND AXL_EXTERNAL_LIBS ZLIB::ZLIB)
LIST(APPEND AXL_EXTERNAL_STATIC_LIBS ZLIB::ZLIB)

# PTHREADS
IF(ENABLE_PTHREADS)
  FIND_PACKAGE(Threads REQUIRED)
  IF(CMAKE_USE_PTHREADS_INIT)
      SET(HAVE_PTHREADS TRUE)
      LIST(APPEND AXL_EXTERNAL_LIBS Threads::Threads)
      LIST(APPEND AXL_EXTERNAL_STATIC_LIBS Threads::Threads)
  ENDIF()
ENDIF(ENABLE_PTHREADS)

############
# This sets an rpath to buildtime libraries in build directory
# and rewrites the rpath to the install location during install
# these lines must come before add_library and add_executable macros
############

# https://gitlab.kitware.com/cmake/community/wikis/doc/cmake/RPATH-handling
# use, i.e. don't skip the full RPATH for the build tree
SET(CMAKE_SKIP_BUILD_RPATH  FALSE)

# when building, don't use the install RPATH already
# (but later on when installing)
SET(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)

SET(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}")

# add the automatically determined parts of the RPATH
# which point to directories outside the build tree to the install RPATH
SET(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

# the RPATH to be used when installing, but only if it's not a system directory
LIST(FIND CMAKE_PLATFORM_IMPLICIT_LINK_DIRECTORIES "${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}" isSystemDir)
IF("${isSystemDir}" STREQUAL "-1")
    SET(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}")
ENDIF("${isSystemDir}" STREQUAL "-1")

############
# End rpath stuff
############

# Subdirectories
ADD_SUBDIRECTORY(src)

IF(ENABLE_TESTS)
    INCLUDE(CTest)
    ADD_SUBDIRECTORY(test)
ENDIF(ENABLE_TESTS)

# Generate config.h with all our build #defs
CONFIGURE_FILE(${PROJECT_SOURCE_DIR}/cmake/config.h.in ${PROJECT_BINARY_DIR}/config.h)
SET_SOURCE_FILES_PROPERTIES(${PROJECT_BINARY_DIR}/config.h PROPERTIES GENERATED TRUE)

# Special Install Files
INSTALL(FILES README.md DESTINATION ${CMAKE_INSTALL_DATADIR}/axl)

# CMake configs

# Install the exports file
INSTALL(EXPORT axlTargets FILE axlTargets.cmake NAMESPACE axl:: DESTINATION share/axl/cmake)

# Export so we can use in-tree
EXPORT(EXPORT axlTargets NAMESPACE axl:: FILE axlTargets.cmake)

# Setup the package config
INCLUDE(CMakePackageConfigHelpers)
CONFIGURE_PACKAGE_CONFIG_FILE(cmake/axlConfig.cmake.in ${CMAKE_CURRENT_BINARY_DIR}/axlConfig.cmake INSTALL_DESTINATION share/axl/cmake)
WRITE_BASIC_PACKAGE_VERSION_FILE(${CMAKE_CURRENT_BINARY_DIR}/axlConfigVersion.cmake COMPATIBILITY SameMajorVersion)

# Install package config
INSTALL(FILES ${CMAKE_CURRENT_BINARY_DIR}/axlConfig.cmake ${CMAKE_CURRENT_BINARY_DIR}/axlConfigVersion.cmake DESTINATION share/axl/cmake)
INSTALL(FILES cmake/FindBBAPI.cmake    DESTINATION share/axl/cmake)
INSTALL(FILES cmake/FindDataWarp.cmake DESTINATION share/axl/cmake)

# Package
SET(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Asynchronous Transfer Library")
SET(CPACK_PACKAGE_VENDOR "Lawrence Livermore National Laboratory")
SET(CPACK_PACKAGE_DESCRIPTION_FILE "${PROJECT_SOURCE_DIR}/README.md")
SET(CPACK_RESOURCE_FILE_LICENSE "${PROJECT_SOURCE_DIR}/LICENSE.TXT")
