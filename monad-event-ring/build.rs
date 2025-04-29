fn main() {
    println!("cargo:rerun-if-changed=../monad-cxx/monad-execution");

    let client_target = "monad_event";
    let client_dst = cmake::Config::new("../monad-cxx/monad-execution/libs/event")
        .define("CMAKE_BUILD_TARGET", client_target)
        .build_target(client_target)
        .build();

    println!(
        "cargo:rustc-link-search=native={}/build",
        client_dst.display()
    );
}
