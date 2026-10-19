/-
Copyright (c) 2026 Lean FRO LLC. All rights reserved.
Released under Apache 2.0 license as described in the file LICENSE.
Author: Emilio J. Gallego Arias
-/

import Beam.Installation.Components

open Lean

/- Construction-only byte hashing. Runtime selection never depends on checksum subprocesses. -/
namespace Beam.Install.Checksum

private def require (condition : Bool) (message : String) : IO Unit :=
  unless condition do throw <| IO.userError message

private def digestOutput (output : IO.Process.Output) : IO String := do
  require (output.exitCode == 0) s!"SHA-256 command failed ({output.exitCode}): {output.stderr}"
  let token := (output.stdout.splitOn " ").headD ""
  -- Both checksum tools prefix the line with one backslash when escaping the filename.
  let hash := if token.startsWith "\\" then (token.drop 1).toString else token
  require (Beam.Installation.hexDigest 64 hash) s!"invalid SHA-256 digest: {repr token}"
  pure hash

/-- One checksum tool choice for a construction operation and its verified staging results. -/
structure Context where
  private mk ::
  private command : String
  private arguments : Array String

def select : IO Context := do
  let commands : Array (String × Array String) := #[
    ("sha256sum", #["--binary"]), ("shasum", #["-a", "256", "--binary"])
  ]
  for (cmd, args) in commands do
    let usable ← try
      let output ← IO.Process.output { cmd, args := #["--version"] }
      pure (output.exitCode == 0)
    catch _ => pure false
    if usable then return ⟨cmd, args⟩
  throw <| IO.userError "installation construction requires sha256sum or shasum"

def Context.fileHash (context : Context) (path : System.FilePath) : IO String := do
  digestOutput (← IO.Process.output {
    cmd := context.command, args := context.arguments.push path.toString
  })

def Context.textHash (context : Context) (text : String) : IO String := do
  digestOutput (← IO.Process.output { cmd := context.command, args := context.arguments } (some text))

end Beam.Install.Checksum
