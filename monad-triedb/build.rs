use std::{env, path::PathBuf};

fn main() {
    println!("cargo:rerun-if-changed=triedb-driver");
    println!("cargo:rerun-if-changed=../monad-cxx/monad-execution");
    println!("cargo:rerun-if-env-changed=TRIEDB_TARGET");
    let build_execution_lib =
        env::var("TRIEDB_TARGET").is_ok_and(|target| target == "triedb_driver");
    if build_execution_lib {
        let target = "triedb_driver";

        let dst = cmake::Config::new("triedb-driver")
            .define("CMAKE_BUILD_TARGET", target)
            .build_target(target)
            .build();

        println!("cargo:rustc-link-arg=-Wl,-rpath,{}", dst.display());
        println!("cargo:rustc-link-search=native={}/build", dst.display());
        println!("cargo:rustc-link-lib=dylib={}", target);
    }

    let bindings = bindgen::Builder::default()
        .header("triedb-driver/include/triedb.h")
        // invalidate on header change
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .generate()
        .expect("Unable to generate bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("triedb.rs"))
        .expect("Couldn't write bindings!");
}
