/-
Copyright (c) 2026 Lean FRO LLC. All rights reserved.
Released under Apache 2.0 license as described in the file LICENSE.
Author: Emilio J. Gallego Arias
-/

import Beam.Installation.Components

open Lean

namespace Beam.Installation

def installManifestSchemaVersion : Nat :=
  4

structure InstallManifest where
  schemaVersion : Nat
  payloadHash : String
  sourceCommit : Option String
  components : Beam.Installation.Components
  artifacts : Array Beam.Installation.Artifact
  deriving BEq, FromJson, ToJson

def parseInstallManifest (json : Json) : Except String InstallManifest := do
  let schema ← json.getObjValAs? Nat "schemaVersion"
  unless schema == installManifestSchemaVersion do
    throw s!"unsupported install manifest schemaVersion {schema}"
  let manifest : InstallManifest ← fromJson? json
  unless Beam.Installation.hexDigest 64 manifest.payloadHash do
    throw "install manifest payloadHash must be a SHA-256 digest"
  manifest.components.validate manifest.artifacts
  pure manifest

def readInstallManifest (path : System.FilePath) : IO InstallManifest := do
  let json ← IO.ofExcept <| Json.parse (← IO.FS.readFile path)
  IO.ofExcept <| parseInstallManifest json

end Beam.Installation
