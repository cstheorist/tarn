// The module calls Tarantool's module API (module.h), which the tarantool
// executable defines: the dynamic linker finds those symbols when Tarantool
// loads the module. Linux allows them undefined in a shared library; macOS
// must be told to.
fn main() {
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos") {
        println!("cargo:rustc-cdylib-link-arg=-undefined");
        println!("cargo:rustc-cdylib-link-arg=dynamic_lookup");
    }
}
