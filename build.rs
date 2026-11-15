fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::compile_protos("proto/arrow_bench.proto")?;
    println!("cargo:rerun-if-changed=proto/arrow_bench.proto");
    Ok(())
}
