###############
# Build tests
###############

LIST(APPEND axl_test_srcs
    axl_cp.c
)

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
ENDIF(BBAPI_FOUND)

####################
# make a verbose "test" target named "check"
####################

ADD_CUSTOM_TARGET(check COMMAND ${CMAKE_CTEST_COMMAND} --verbose)
