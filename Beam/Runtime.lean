/-
Copyright (c) 2026 Lean FRO LLC. All rights reserved.
Released under Apache 2.0 license as described in the file LICENSE.
Author: Emilio J. Gallego Arias
-/

import Beam.Hash

namespace Beam.Runtime

/-- Read-only local admission observes cancellation before and after lookup. -/
structure Admission where
  isCancelled : IO Bool

/-- A published Beam runtime generation, retained by its workspace/session owner. -/
structure Identity where
  /-- Canonical owning root supplied by the application adapter. -/
  installation : System.FilePath
  generation : String
  deriving BEq, Repr

/-- Complete execution paths and identity, independent of installation and workspace layout. -/
structure Description where
  command : String
  plugin : System.FilePath
  /-- The matching daemon also supplies the in-process Lake helper. -/
  daemonBin : System.FilePath
  toolchain : String
  identity : Identity
  deriving BEq, Repr

/-- Execution identity excludes selector labels: aliases of the same runtime remain equivalent. -/
def Description.sameRuntime (left right : Description) : Bool :=
  left.command == right.command && left.plugin == right.plugin &&
    left.daemonBin == right.daemonBin && left.identity == right.identity

/-- Bind a project root to the complete recorded runtime; shared by application adapters. -/
def Description.configHash (runtime : Description) (root : System.FilePath) : String :=
  let fields := [root.toString, runtime.command, runtime.plugin.toString, runtime.daemonBin.toString,
    runtime.identity.installation.toString, runtime.identity.generation]
  s!"{(fields.foldl Beam.mixField 14695981039346656037).toNat}"

end Beam.Runtime
