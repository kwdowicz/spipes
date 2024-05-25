use std::fs;
fn main() {
    let out_dir = "src";
    fs::create_dir_all(out_dir).unwrap();

    tonic_build::configure()
        .out_dir(out_dir)
        .compile(&["proto/broker_service.proto"], &["proto"])
        .unwrap_or_else(|e| panic!("Failed to compile protos {:?}", e));
}
