#include <stdio.h>
#include <stdlib.h>
#include "axl.h"
#include "axl_internal.h"

#include "kvtree.h"
#include "kvtree_util.h"

int
main(void) {
    int rc;
    char *state_file = NULL;
    kvtree* axl_config_values = kvtree_new();

    size_t old_axl_file_buf_size = axl_file_buf_size;
    int old_axl_debug = axl_debug;
    int old_axl_make_directories = axl_make_directories;
    int old_axl_copy_metadata = axl_copy_metadata;

    rc = AXL_Init(state_file);
    if (rc != AXL_SUCCESS) {
        printf("AXL_Init() failed (error %d)\n", rc);
        return EXIT_FAILURE;
    }

    /* check AXL configuration settings */
    rc = kvtree_util_set_bytecount(axl_config_values,
                                   AXL_KEY_CONFIG_FILE_BUF_SIZE,
                                   old_axl_file_buf_size + 1);
    if (rc != KVTREE_SUCCESS) {
        printf("kvtree_util_set_bytecount failed (error %d)\n", rc);
        return EXIT_FAILURE;
    }
    rc = kvtree_util_set_int(axl_config_values, AXL_KEY_CONFIG_DEBUG,
                             !old_axl_debug);
    if (rc != KVTREE_SUCCESS) {
        printf("kvtree_util_set_int failed (error %d)\n", rc);
        return EXIT_FAILURE;
    }
    rc = kvtree_util_set_int(axl_config_values, AXL_KEY_CONFIG_MKDIR,
                             !old_axl_make_directories);
    if (rc != KVTREE_SUCCESS) {
        printf("kvtree_util_set_int failed (error %d)\n", rc);
        return EXIT_FAILURE;
    }
    rc = kvtree_util_set_int(axl_config_values, AXL_KEY_CONFIG_COPY_METADATA,
                             !old_axl_copy_metadata);
    if (rc != KVTREE_SUCCESS) {
        printf("kvtree_util_set_int failed (error %d)\n", rc);
        return EXIT_FAILURE;
    }

    printf("Configuring AXL...\n");
    if (AXL_Config(axl_config_values) == NULL) {
        printf("AXL_Config() failed\n");
        return EXIT_FAILURE;
    }

    printf("Configuring AXL a second time (this should fail)...\n");
    if (AXL_Config(axl_config_values) != NULL) {
        printf("AXL_Config() succeeded unexpectedly\n");
        return EXIT_FAILURE;
    }

    if (axl_file_buf_size != old_axl_file_buf_size + 1) {
        printf("AXL_Config() failed to set %s: %lu != %lu\n",
               AXL_KEY_CONFIG_FILE_BUF_SIZE, (long unsigned)axl_file_buf_size,
               (long unsigned)(old_axl_file_buf_size + 1));
        return EXIT_FAILURE;
    }

    if (axl_debug != !old_axl_debug) {
        printf("AXL_Config() failed to set %s: %d != %d\n",
               AXL_KEY_CONFIG_DEBUG, axl_debug, !old_axl_debug);
        return EXIT_FAILURE;
    }

    if (axl_make_directories != !old_axl_make_directories) {
        printf("AXL_Config() failed to set %s: %d != %d\n",
               AXL_KEY_CONFIG_MKDIR, axl_make_directories,
               !old_axl_make_directories);
        return EXIT_FAILURE;
    }

    if (axl_copy_metadata != !old_axl_copy_metadata) {
        printf("AXL_Config() failed to set %s: %d != %d\n",
               AXL_KEY_CONFIG_COPY_METADATA, axl_copy_metadata,
               !old_axl_copy_metadata);
        return EXIT_FAILURE;
    }

    rc = AXL_Finalize();
    if (rc != AXL_SUCCESS) {
        printf("AXL_Finalize() failed (error %d)\n", rc);
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
