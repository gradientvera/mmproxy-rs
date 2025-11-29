{

  description = "Rust implementation of TCP + UDP Proxy Protocol (aka. MMProxy)";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-compat = {
      url = "https://git.lix.systems/lix-project/flake-compat/archive/main.tar.gz";
      # Optional:
      flake = false;
    };
  };

  outputs = { nixpkgs, ... }:
    let
      forAllSystems = function:
        nixpkgs.lib.genAttrs [ "x86_64-linux" "aarch64-linux" ]
          (system: function (import nixpkgs { inherit system; }));
    in
  {

    packages = forAllSystems (pkgs: {
      default = pkgs.callPackage ./nix/package.nix { };
    });

    devShells = forAllSystems (pkgs: {
      default = pkgs.mkShell {
        buildInputs = with pkgs; [ cargo rustc rustfmt pre-commit rustPackages.clippy ];
        RUST_SRC_PATH = pkgs.rustPlatform.rustLibSrc;
      };
    });

    overlays = forAllSystems (pkgs: {
      default = final: prev: import ./nix/module.nix final prev;
    });

    nixosModules.default = import ./nix/module.nix;
  };

}