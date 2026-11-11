# 0.2.0
- update liblsl to v1.17.7
- vendor pugixml v1.15 as a git submodule (liblsl no longer bundles it)
- regenerate bindings with bindgen 0.73 (`regen-bindings.sh`); edition 2021
- add bindings for `lsl_set_config_filename` and `lsl_set_config_content`
- build: disable liblsl LTO (GCC emits bitcode-only objects the Rust linker can't use), link the
  separately-built pugixml archive, force a Release build of liblsl (no debug log spam), pin
  `CMAKE_INSTALL_LIBDIR=lib`, link all required Windows system libs
- build: use the vendored pugixml (or `LSL_PUGIXML_SOURCE_DIR`) so builds work offline
- build: use the NDK CMake toolchain for Android (any ABI), accept `ANDROID_NDK`,
  `ANDROID_NDK_HOME` or `ANDROID_NDK_ROOT`

# 0.1.1
- transferred repo from https://github.com/intheon to https://github.com/labstreaminglayer organization and updated links

# 0.1.0
- Initial Release