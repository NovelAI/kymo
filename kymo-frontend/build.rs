fn main() {
    println!("cargo:rerun-if-changed=../proto/kymo.proto");
    tonic_prost_build::configure()
        .build_server(false)
        .build_client(false)
        .build_transport(false)
        .compile_protos(&["../proto/kymo.proto"], &["../proto"])
        .unwrap();
}
