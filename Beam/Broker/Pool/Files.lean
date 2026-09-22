/- Copyright (c) 2026 Lean FRO LLC. Released under Apache 2.0 license. -/
import Lean
import Beam.Path

namespace Beam.Broker.Pool.Files

structure Stamp where
  path : String
  size : Nat
  sec : Int
  nsec : Nat
  digest : String := ""
  deriving Inhabited, BEq

def Stamp.sameMetadata (a b : Stamp) : Bool :=
  a.size == b.size && a.sec == b.sec && a.nsec == b.nsec

private opaque WatchImpl : NonemptyType
def Watch := WatchImpl.type

@[extern "lean_beam_pool_watch_new"]
opaque watchNew : IO Watch

@[extern "lean_beam_pool_watch_add"]
private opaque watchAdd (watch : @& Watch) (path relative : @& String) : IO Unit

@[extern "lean_beam_pool_watch_read"]
private opaque watchRead (watch : @& Watch) : IO (Array String)

def excluded (name : String) : Bool :=
  [".git", ".beam", ".codex-worktrees", "__pycache__", "node_modules", ".venv"].contains name

def inputFile (path : String) : Bool :=
  let name := (System.FilePath.mk path).fileName.getD ""
  !(path.splitOn "/").any excluded &&
    (["lean-toolchain", "lakefile.toml", "lake-manifest.json"].contains name ||
     [".lean", ".olean", ".server", ".private", ".ir", ".sig", ".so", ".dylib", ".dll",
      ".ilean", ".trace", ".c", ".bc"].any (fun suffix => path.endsWith suffix))

def configurationFile (path : String) : Bool :=
  ["lean-toolchain", "lakefile.lean", "lakefile.toml", "lake-manifest.json"].contains
    ((System.FilePath.mk path).fileName.getD "")

def changed (watch : Watch) : IO (Array String) := do
  return (← watchRead watch).filter fun path =>
    path == "*" || (!(path.splitOn "/").any excluded &&
      (path.endsWith "/" || inputFile path))

def stamp (root : System.FilePath) (name : String) : IO Stamp := do
  let path := root / name
  let resolved ← Beam.resolveExistingPath path
  unless (Beam.pathRelativeToRoot? root resolved).isSome do
    throw <| IO.userError s!"pool input escapes the project: {name}"
  let metadata ← path.metadata
  return { path := name, size := metadata.byteSize.toNat,
           sec := metadata.modified.sec, nsec := metadata.modified.nsec.toNat }

partial def scan (root : System.FilePath) (watch : Watch) (relative := "") :
    IO (Array Stamp) := do
  let directory := if relative.isEmpty then root else root / relative
  watchAdd watch directory.toString relative
  let mut inputs := #[]
  for entry in ← directory.readDir do
    if excluded entry.fileName then continue
    let name := if relative.isEmpty then entry.fileName else relative ++ "/" ++ entry.fileName
    if ← entry.path.isDir then
      -- Match attachment's directory walk: never follow directory symlinks.
      if (← entry.path.symlinkMetadata).type != .symlink then
        inputs := inputs ++ (← scan root watch name)
    else if inputFile name then
      inputs := inputs.push (← stamp root name)
  return inputs

def stampMap (inputs : Array Stamp) : Std.TreeMap String Stamp :=
  inputs.foldl (fun result input => result.insert input.path input) {}

def hex (bytes : ByteArray) : String :=
  String.ofList <| bytes.data.toList.flatMap fun byte =>
    let digit := fun n => Char.ofNat (if n < 10 then 48 + n else 87 + n)
    [digit (byte.toNat / 16), digit (byte.toNat % 16)]

end Beam.Broker.Pool.Files
