#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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

    /* check that all expected global variables were set */
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

    /* check that querying works (very similar to setter in library) */
    static const char* known_options[] = {
        AXL_KEY_CONFIG_FILE_BUF_SIZE,
        AXL_KEY_CONFIG_DEBUG,
        AXL_KEY_CONFIG_MKDIR,
        AXL_KEY_CONFIG_COPY_METADATA,
        NULL
    };

    size_t new_axl_file_buf_size;
    int new_axl_debug, new_axl_make_directories, new_axl_copy_metadata;

    kvtree *axl_configured_values = AXL_Config(NULL);
    if (axl_configured_values == NULL) {
        printf("AXL_Config() failed to get config\n");
        return EXIT_FAILURE;
    }

    unsigned long ul;
    if (kvtree_util_get_bytecount(axl_configured_values,
      AXL_KEY_CONFIG_FILE_BUF_SIZE, &ul) != KVTREE_SUCCESS)
    {
        printf("Could not get %s from AXL_Config\n",
               AXL_KEY_CONFIG_FILE_BUF_SIZE);
        return EXIT_FAILURE;
    }
    new_axl_file_buf_size = (size_t) ul;
    if (new_axl_file_buf_size != ul) {
        char* value;
        kvtree_util_get_str(axl_configured_values, AXL_KEY_CONFIG_FILE_BUF_SIZE,
                            &value);
        printf("Value '%s' passed for %s exceeds int range\n",
          value, AXL_KEY_CONFIG_FILE_BUF_SIZE
        );
        return EXIT_FAILURE;
    }
    if (new_axl_file_buf_size != old_axl_file_buf_size+1) {
        printf("AXL_Config returned unexpected value %llu for %s. Expected %llu.\n",
               (unsigned long long)new_axl_file_buf_size,
               AXL_KEY_CONFIG_FILE_BUF_SIZE,
               (unsigned long long)old_axl_file_buf_size+1);
        return EXIT_FAILURE;
    }
    
    if (kvtree_util_get_int(axl_configured_values, AXL_KEY_CONFIG_DEBUG,
                            &new_axl_debug) != KVTREE_SUCCESS)
    {
        printf("Could not get %s from AXL_Config\n",
               AXL_KEY_CONFIG_DEBUG);
        return EXIT_FAILURE;
    }
    if (new_axl_debug != !old_axl_file_buf_size) {
        printf("AXL_Config returned unexpected value %d for %s. Expected %d.\n",
               new_axl_debug, AXL_KEY_CONFIG_DEBUG, !old_axl_debug);
        return EXIT_FAILURE;
    }

    if (kvtree_util_get_int(axl_configured_values, AXL_KEY_CONFIG_MKDIR,
                            &new_axl_make_directories) != KVTREE_SUCCESS)
    {
        printf("Could not get %s from AXL_Config\n",
               AXL_KEY_CONFIG_MKDIR);
        return EXIT_FAILURE;
    }
    if (new_axl_make_directories != !old_axl_make_directories) {
        printf("AXL_Config returned unexpected value %d for %s. Expected %d.\n",
               new_axl_make_directories, AXL_KEY_CONFIG_MKDIR,
               !old_axl_make_directories);
        return EXIT_FAILURE;
    }

    if (kvtree_util_get_int(axl_configured_values, AXL_KEY_CONFIG_COPY_METADATA,
                            &new_axl_copy_metadata) != KVTREE_SUCCESS)
    {
        printf("Could not get %s from AXL_Config\n",
               AXL_KEY_CONFIG_COPY_METADATA);
        return EXIT_FAILURE;
    }
    if (new_axl_copy_metadata != !old_axl_copy_metadata) {
        printf("AXL_Config returned unexpected value %d for %s. Expected %d.\n",
               new_axl_copy_metadata, AXL_KEY_CONFIG_COPY_METADATA,
               !old_axl_copy_metadata);
        return EXIT_FAILURE;
    }

    /* report all unknown options (typos?) */
    const kvtree_elem* elem;
    for (elem = kvtree_elem_first(axl_configured_values);
         elem != NULL;
         elem = kvtree_elem_next(elem))
    {
        /* must be only one level deep, ie plain kev = value */
        const kvtree* elem_hash = kvtree_elem_hash(elem);
        assert(kvtree_size(elem_hash) == 1);

        const kvtree* kvtree_first_elem_hash =
          kvtree_elem_hash(kvtree_elem_first(elem_hash));
        assert(kvtree_size(kvtree_first_elem_hash) == 0);

        /* check against known options */
        const char** opt;
        int found = 0;
        for (opt = known_options; *opt != NULL; opt++) {
            if (strcmp(*opt, kvtree_elem_key(elem)) == 0) {
                found = 1;
                break;
            }
        }
        if (! found) {
            printf("Unknown configuration parameter '%s' with value '%s'\n",
              kvtree_elem_key(elem),
              kvtree_elem_key(kvtree_elem_first(kvtree_elem_hash(elem)))
            );
            return EXIT_FAILURE;
        }
    }

    kvtree_delete(&axl_configured_values);

    rc = AXL_Finalize();
    if (rc != AXL_SUCCESS) {
        printf("AXL_Finalize() failed (error %d)\n", rc);
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
