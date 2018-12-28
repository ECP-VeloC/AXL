###############
# Build tests
###############

LIST(APPEND axl_test_srcs
    test_axl.c
    test_axl_sync.c
    test_axl_async_daemon.c
    test_axl_async_ibmbb.c
)

ADD_EXECUTABLE(test_axl ${axl_test_srcs})
TARGET_LINK_LIBRARIES(test_axl axl)

################
# Add tests to ctest
################

# rountdrip tests
ADD_TEST(transfer_tests test_axl)

####################
# make a verbose "test" target named "check"
####################

ADD_CUSTOM_TARGET(check COMMAND ${CMAKE_CTEST_COMMAND} --verbose)
