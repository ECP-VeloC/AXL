###############
# Build tests
###############

LIST(APPEND axl_test_srcs
    axl_cp.c
)

# Build with debug symbols (-g)
# SET(CMAKE_BUILD_TYPE Debug)

ADD_EXECUTABLE(axl_cp ${axl_test_srcs})

TARGET_LINK_LIBRARIES(axl_cp axl)

IF(MPI_FOUND)
    ADD_EXECUTABLE(axl_mpi_cp axl_mpi_cp.c)
    TARGET_LINK_LIBRARIES(axl_mpi_cp axl)
ENDIF(MPI_FOUND)

################
# Add tests to ctest
################

CONFIGURE_FILE(test_axl.sh ${CMAKE_CURRENT_BINARY_DIR} COPYONLY)

ADD_TEST(sync_test test_axl.sh sync)
ADD_TEST(pthreads_test test_axl.sh pthread)
IF(BBAPI_FOUND)
    ADD_TEST(bbapi_test test_axl.sh bbapi)
    ADD_TEST(bbapi_cancel_test test_axl.sh -n 1500 -c 0.5 bbapi)
ENDIF(BBAPI_FOUND)

# Create 1500 files (~871MB) and cancel the test 200ms into the transfer
ADD_TEST(pthreads_cancel_test test_axl.sh -n 1500 -c 0.2 pthread)

####################
# make a verbose "test" target named "check"
####################

ADD_CUSTOM_TARGET(check COMMAND ${CMAKE_CTEST_COMMAND} --verbose)
