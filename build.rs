fn main() {
    prost_build::compile_protos(&["proto/graphd.proto"], &["proto/"]).unwrap();
}
