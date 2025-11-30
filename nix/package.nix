{ lib
, rustPlatform
}:
rustPlatform.buildRustPackage (finalAttrs: {
  pname = "mmproxy-rs";
  version = "unstable-2025-11-28";

  src = ../.;

  cargoHash = "sha256-MmLKVWRjIu/z5uB/j07oF8jpPFlC+B9SemltgCKoCgI=";

  meta = {
    mainProgram = "mmproxy";
    description = "Rust implementation of TCP + UDP Proxy Protocol (aka. MMProxy)";
    homepage = "https://github.com/saiko-tech/mmproxy-rs";
    license = lib.licenses.mit;
  };
})
