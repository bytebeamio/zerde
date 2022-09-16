use std::io::Result;
fn main() -> Result<()> {
    prost_build::compile_protos(&["src/test.proto"], &["src/"])?;

    capnpc::CompilerCommand::new()
        .file("src/test.capnp")
        .run()
        .expect("compiling schema");

    Ok(())
}
