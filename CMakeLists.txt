GoModBuild(
  TARGET vbmap
  PACKAGE github.com/couchbase/vbmap
  INSTALL_PATH bin
  GOVERSION SUPPORTED_NEWER)

GET_TARGET_PROPERTY (vbmap_exe vbmap GO_BINARY)
CMAKE_PATH(GET vbmap_exe PARENT_PATH vbmap_bin_dir)
SET(vbmap_BINARY_DIR ${vbmap_bin_dir} CACHE STRING
    "Path to vbmap binary directory")
