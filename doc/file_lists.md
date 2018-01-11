# AXL's Internal Data Structure

AXL tracks sets of files for a transfer in a [KVTree](https://github.com/llnl/kvtree).
This magical data structure is like many key-value structures, but each value MUST itself be a KVTree.
Thus, in AXL (and many other codes which use KVTree) each "layer" is standardized as either keys or values.

In AXL, the monolithic internal data structure is called `axl_async_file_lists`.
If it were to be expressed as YAML, it might look like:

``` yaml
- AXL_KEY_HANDLE_UID:
  - 0:
    - AXL_KEY_UNAME: "User Defined String"
    - AXL_KEY_XFER_TYPE_STR: "Cray Datawarp"
    - AXL_KEY_XFER_TYPE_INT: AXL_XFER_ASYNC_DW
    - AXL_KEY_FLUSH_STATUS: AXL_FLUSH_STATUS_INPROG
    - AXL_KEY_FLUSH_FILES:
      - /path/to/file1:
        - AXL_KEY_FILE_DEST: /p/gpfs/file1
        - AXL_KEY_FILE_STATUS: AXL_FLUSH_STATUS_INPROG
        - AXL_KEY_FILE_CRC32: abcdef640927393
      - /path/to/file2:
        - AXL_KEY_FILE_DEST: /p/gpfs/file2
        - AXL_KEY_FILE_STATUS: AXL_FLUSH_STATUS_INPROG
        - AXL_KEY_FILE_CRC32: abcdef640927392
  - 1:
    - AXL_KEY_UNAME: "Checkpoint 1"
    - AXL_KEY_XFER_TYPE_STR: "Synchronous"
    - AXL_KEY_XFER_TYPE_INT: AXL_XFER_SYNC
    - AXL_KEY_FLUSH_STATUS: AXL_FLUSH_STATUS_SOURCE
    - AXL_KEY_FLUSH_FILES:
      - /path/to/file1:
        - AXL_KEY_FILE_DEST: /p/gpfs/file1
        - AXL_KEY_FILE_STATUS: AXL_FLUSH_STATUS_SOURCE
        - AXL_KEY_FILE_CRC32: abcdef640927394
      - /path/to/file3:
        - AXL_KEY_FILE_DEST: /p/gpfs/file3
        - AXL_KEY_FILE_STATUS: AXL_FLUSH_STATUS_SOURCE
        - AXL_KEY_FILE_CRC32: abcdef640927395
```

Here is the structure as a Python dict (using abbreviated key names and values):

```
{ KEY_UID : {
     0 : { KEY_XTYPE:  { DW : {}}
           KEY_NAME:   { "app name" : {}}
           KEY_STATUS: { INPROG : {}}
           KEY_FILES:  {
              "path/to/file" : { KEY_DEST :   { "dest/file" : {}}
                                 KEY_STATUS : { INPROG : {}}
                                 KEY_CRC32 :  { 0x12abc : {}}}}}}}
```
