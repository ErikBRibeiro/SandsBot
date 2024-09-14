 {
  inputs = {
    nixpkgs.url = "nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    poetry2nix.url = "github:nix-community/poetry2nix";
  };

  outputs = { self, nixpkgs, flake-utils, poetry2nix, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};

        inherit (poetry2nix.lib.mkPoetry2Nix { inherit pkgs; })
          mkPoetryEnv
          defaultPoetryOverrides
        ;

        poetryDev = mkPoetryEnv {
          projectDir = self;
          overrides = defaultPoetryOverrides.extend
            (self: super: {
              backtesting = super.backtesting.overridePythonAttrs
                (
                  old: {
                    buildInputs = (old.buildInputs or []) ++ [super.setuptools super.urllib3];
                  }
                );
            });
        };
      in
      {
        devShells.default = pkgs.mkShell {
          packages = [
            pkgs.poetry
            pkgs.python3Packages.tkinter
            poetryDev
          ];
        };
      });
}
