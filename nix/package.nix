{ lib
, rustPlatform
}:
rustPlatform.buildRustPackage (finalAttrs: {
  pname = "mmproxy-rs";
  version = "unstable-2025-12-14";

  src = ../.;

  cargoHash = "sha256-btreDkxlLeyufk8V8Z0uCabBcx39awDvnEy3vsJDXUA=";

  meta = {
    mainProgram = "mmproxy";
    description = "Rust implementation of TCP + UDP Proxy Protocol (aka. MMProxy)";
    homepage = "https://github.com/saiko-tech/mmproxy-rs";
    license = lib.licenses.mit;
  };
})
