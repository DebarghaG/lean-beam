/-
Copyright (c) 2026 Lean FRO LLC. All rights reserved.
Released under Apache 2.0 license as described in the file LICENSE.
Author: Emilio J. Gallego Arias
-/

namespace Beam.Cli

def bundleRootFiles : List String :=
  ["LICENSE", "Beam.lean", "lakefile.lean", "lake-manifest.json", "lean-toolchain",
    "validated-lean-toolchains", "compatible-lean-release-lines", "custom-lean-toolchains"]

def bundleSourceDirs : List String :=
  ["Beam"]

end Beam.Cli
