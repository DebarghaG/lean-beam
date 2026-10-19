/-
Copyright (c) 2026 Lean FRO LLC. All rights reserved.
Released under Apache 2.0 license as described in the file LICENSE.
Author: Emilio J. Gallego Arias
-/

import Beam.Installation.Components

open Lean

private structure ContractCase where
  name : String
  valid : Bool
  record : Json
  deriving FromJson

def main (args : List String) : IO Unit := do
  let [path] := args | throw <| IO.userError "expected materialized manifest corpus"
  let cases ← IO.ofExcept <| Json.parse (← IO.FS.readFile path) >>= fromJson? (α := Array ContractCase)
  for item in cases do
    let accepted : Except String Unit := do
      let components ← item.record.getObjValAs? Beam.Installation.Components "components"
      let artifacts ← item.record.getObjValAs? (Array Beam.Installation.Artifact) "artifacts"
      components.validate artifacts
    unless accepted.isOk == item.valid do
      throw <| IO.userError s!"manifest contract disagreement: {item.name}"
  IO.println s!"Lean reader agreed on all {cases.size} production-writer manifest cases"
