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

################
# Add tests to ctest
################

CONFIGURE_FILE(test_axl.sh ${CMAKE_CURRENT_BINARY_DIR} COPYONLY)

ADD_TEST(sync_test test_axl.sh sync)
ADD_TEST(pthreads_test test_axl.sh pthread)
IF(BBAPI_FOUND)
    ADD_TEST(bbapi_test test_axl.sh bbapi)

    # Create 300 files, cancel transfer after 3 seconds.
    ADD_TEST(bbapi_cancel_test test_axl.sh -n 300 -c 3 bbapi)
ENDIF(BBAPI_FOUND)

# Create 100 files, pause the transfer after 1000 bytes or more
# have been written, and cancel the transfer after 1 second.
ADD_TEST(pthreads_cancel_test test_axl.sh -n 100 -p 1000 -c 1 pthread)

# Create 100 files, pause the transfer after 1000 bytes or more
# have been written, cancel the transfer after 1 second, and resume
# the transfer.
ADD_TEST(sync_resume_test test_axl.sh -n 100 -p 1000 -c 1 -U sync)
ADD_TEST(pthread_resume_test test_axl.sh -n 100 -p 1000 -c 1 -U pthread)
IF(BBAPI_FOUND)
    # Create 300 files, cancel transfer after 3 seconds, resume transfer.
    # Values found through experimentation.
    ADD_TEST(bbapi_resume_test test_axl.sh -n 300 -c 3 -U  bbapi)
ENDIF(BBAPI_FOUND)

####################
# make a verbose "test" target named "check"
####################

ADD_CUSTOM_TARGET(check COMMAND ${CMAKE_CTEST_COMMAND} --verbose)
