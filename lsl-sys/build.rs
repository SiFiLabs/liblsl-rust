use std::env;
use std::path::{Path, PathBuf};

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    for p in [
        "liblsl/CMakeLists.txt",
        "liblsl/cmake",
        "liblsl/include",
        "liblsl/src",
    ] {
        println!("cargo:rerun-if-changed={}", p);
    }
    for v in [
        "LSL_PUGIXML_SOURCE_DIR",
        "ANDROID_NDK",
        "ANDROID_NDK_HOME",
        "ANDROID_NDK_ROOT",
    ] {
        println!("cargo:rerun-if-env-changed={}", v);
    }
    build_liblsl();
}

/// Location of a local pugixml checkout to build against, so that the build does not need network
/// access. liblsl >= 1.17 no longer bundles pugixml and would otherwise fetch it from GitHub.
fn pugixml_source_dir() -> Option<PathBuf> {
    if let Some(dir) = env::var_os("LSL_PUGIXML_SOURCE_DIR") {
        return Some(PathBuf::from(dir));
    }
    let vendored = Path::new(&env::var("CARGO_MANIFEST_DIR").unwrap()).join("pugixml");
    if vendored.join("CMakeLists.txt").exists() {
        return Some(vendored);
    }
    None
}

fn android_ndk() -> Option<String> {
    ["ANDROID_NDK", "ANDROID_NDK_HOME", "ANDROID_NDK_ROOT"]
        .iter()
        .find_map(|v| env::var(v).ok().filter(|s| !s.is_empty()))
}

// Build the liblsl library from source using cmake
fn build_liblsl() {
    let target = env::var("TARGET").unwrap();

    // build with cmake
    let mut cfg = cmake::Config::new("liblsl");
    cfg.define("LSL_BUILD_STATIC", "ON")
        // LSL_OPTIMIZATIONS turns on LTO, which with GCC produces bitcode-only objects that
        // the Rust linker cannot consume.
        .define("LSL_OPTIMIZATIONS", "OFF")
        .define("LSL_UNITTESTS", "OFF")
        .define("LSL_TOOLS", "OFF")
        .define("LSL_DEBUGLOG", "OFF")
        .define("LSL_FRAMEWORK", "OFF")
        .define("LSL_UNIXFOLDERS", "ON")
        // GNUInstallDirs would pick lib64 on some distros
        .define("CMAKE_INSTALL_LIBDIR", "lib")
        // liblsl's debug build turns on verbose logging; always build an optimized library
        .profile("Release");

    if let Some(dir) = pugixml_source_dir() {
        cfg.define("FETCHCONTENT_SOURCE_DIR_PUGIXML", &dir)
            .define("FETCHCONTENT_FULLY_DISCONNECTED", "ON");
    }

    if target.contains("msvc") {
        // override some C/CXX flags that the cmake crate splices in on Windows
        // (these cause the build to fail)...
        // * /EHsc sets the correct exception handling mode
        // * /GR enables RTTI
        // * /MD links in the msvcrt as a DLL instead of statically
        let cxx_args = " /nologo /EHsc /MD /GR";
        cfg.define("WIN32", "1")
            .define("_WINDOWS", "1")
            .define("CMAKE_C_FLAGS", cxx_args)
            .define("CMAKE_CXX_FLAGS", cxx_args)
            .define("CMAKE_C_FLAGS_DEBUG", cxx_args)
            .define("CMAKE_CXX_FLAGS_DEBUG", cxx_args)
            .define("CMAKE_C_FLAGS_RELEASE", cxx_args)
            .define("CMAKE_CXX_FLAGS_RELEASE", cxx_args);
    } else if target.contains("android") {
        let ndk = android_ndk().expect(
            "set ANDROID_NDK (or ANDROID_NDK_HOME / ANDROID_NDK_ROOT) to build for Android",
        );
        let abi = match target.split('-').next().unwrap() {
            "aarch64" => "arm64-v8a",
            "armv7" | "thumbv7neon" | "arm" => "armeabi-v7a",
            "i686" => "x86",
            "x86_64" => "x86_64",
            other => panic!("unsupported Android architecture: {}", other),
        };
        cfg.define(
            "CMAKE_TOOLCHAIN_FILE",
            Path::new(&ndk).join("build/cmake/android.toolchain.cmake"),
        )
        .define("ANDROID_ABI", abi)
        .define("ANDROID_STL", "c++_shared")
        .define("ANDROID_NDK", &ndk);
        if let Ok(level) = env::var("ANDROID_PLATFORM") {
            cfg.define("ANDROID_PLATFORM", level);
        }
    }
    let install_dir = cfg.build();

    // emit link directives
    println!(
        "cargo:rustc-link-search=native={}",
        install_dir.join("lib").display()
    );
    println!("cargo:rustc-link-lib=static=lsl");

    // pugixml is built as a separate static library and not merged into liblsl.a
    let pugi_build = install_dir.join("build/_deps/pugixml-build");
    println!("cargo:rustc-link-search=native={}", pugi_build.display());
    // multi-config generators (Visual Studio, Xcode) put the archive in a per-config subdir
    println!(
        "cargo:rustc-link-search=native={}",
        pugi_build.join("Release").display()
    );
    println!("cargo:rustc-link-lib=static=pugixml");

    // make sure we also link some additional libs
    if target.contains("android") {
        // links libc++_shared.so, which the app must ship alongside the Rust library
        println!("cargo:rustc-link-lib=dylib=c++_shared");
    } else if target.contains("linux") {
        println!("cargo:rustc-link-lib=dylib=stdc++");
    } else if target.contains("windows") {
        if target.contains("gnu") {
            println!("cargo:rustc-link-lib=dylib=stdc++");
        }
        // system libraries liblsl depends on; a static archive does not carry these along
        for lib in ["iphlpapi", "winmm", "mswsock", "ws2_32", "bcrypt"] {
            println!("cargo:rustc-link-lib=dylib={}", lib);
        }
    } else {
        println!("cargo:rustc-link-lib=dylib=c++");
    }
}
