/-
Copyright (c) 2026 Lean FRO LLC. All rights reserved.
Released under Apache 2.0 license as described in the file LICENSE.
Author: Emilio J. Gallego Arias
-/

import Lean
import Beam.Install.Sources
import Beam.Install.Toolchain
import Beam.LSP.Lib.NativeLib
import Beam.Path

open Lean

namespace Beam.Install

/-- The Lake workspace inside one target's temporary build directory. -/
def buildWorkspace (buildDir : System.FilePath) : System.FilePath :=
  buildDir / "workspace"

/-- The SDK libraries a build declares, relative to its `.lake/build`; never inferred from files. -/
def validSharedRuntimePaths (paths : Array String) : Bool := Id.run do
  if paths.isEmpty then return false
  let directory := if System.Platform.isWindows then "bin" else "lib/lean"
  let mut seen : Std.HashSet String := {}
  for path in paths do
    if !Beam.Installation.relativePath path ||
        (System.FilePath.mk path).parent != some (System.FilePath.mk directory) || seen.contains path then
      return false
    seen := seen.insert path
  return true

private def copyFileInto (srcRoot dstRoot srcPath : System.FilePath) : IO Unit := do
  let rel := Beam.pathRelativeToRootOrSelf srcRoot srcPath
  let dst := dstRoot / rel
  if let some parent := dst.parent then
    IO.FS.createDirAll parent
  IO.FS.writeBinFile dst (← IO.FS.readBinFile srcPath)

/-- Copy the Beam sources into a fresh workspace; construction never reuses a previous build. -/
private def prepareWorkspace (source workspace : System.FilePath) : IO Unit := do
  if ← workspace.pathExists then
    throw <| IO.userError s!"construction workspace already exists: {workspace}"
  IO.FS.createDirAll workspace
  for path in ← collectSourceFiles source do
    copyFileInto source workspace path
  let packagesDir := source / ".lake" / "packages"
  if ← packagesDir.pathExists then
    for path in ← collectTreeFiles packagesDir do
      copyFileInto packagesDir (workspace / ".lake" / "packages") path

private def samePath (left right : System.FilePath) : IO Bool := do
  try
    let left ← IO.FS.realPath left
    let right ← IO.FS.realPath right
    pure (left.normalize == right.normalize)
  catch _ =>
    pure (left.normalize == right.normalize)

private def symlinkForce (src dst : System.FilePath) : IO Unit := do
  try
    IO.FS.removeFile dst
  catch _ =>
    pure ()
  let out ← IO.Process.output {
    cmd := "ln"
    args := #["-s", src.toString, dst.toString]
  }
  if out.exitCode != 0 then
    throw <| IO.userError s!"failed to create symlink {dst} -> {src}\n{out.stderr}"

private def copyExecutable (src dst : System.FilePath) : IO Unit := do
  IO.FS.writeBinFile dst (← IO.FS.readBinFile src)
  let x : IO.AccessRight := {read := true, write := true, execution := true}
  let rx : IO.AccessRight := {read := true, write := false, execution := true}
  IO.setAccessRights dst ⟨x, rx, rx⟩

/-- Development SDKs keep their libraries outside `lib/lean`; give Lake a private home that links
to them. The links point into the toolchain, so discarding the build must not follow them. -/
private def prepareNonstandardLakeHome
    (buildDir leanPrefix libDir : System.FilePath) : IO System.FilePath := do
  if System.Platform.isWindows then
    throw <| IO.userError
      "nonstandard Lean libdir toolchains are not supported by the standalone runtime construction on Windows"
  let lakeHome := buildDir / "lake-home"
  let lakeBuildDir := lakeHome / ".lake" / "build"
  let binDir := lakeBuildDir / "bin"
  let sharedLibDir := lakeBuildDir / "lib"
  let lakeLibDir := sharedLibDir / "lean"
  IO.FS.createDirAll binDir
  IO.FS.createDirAll sharedLibDir
  IO.FS.createDirAll lakeLibDir
  copyExecutable (leanPrefix / "bin" / "lake") (binDir / "lake")
  for entry in ← libDir.readDir do
    let name := entry.fileName
    if name.startsWith "lib" then
      pure ()
    else
      symlinkForce entry.path (lakeLibDir / name)
  let standardLibDir := leanPrefix / "lib" / "lean"
  for entry in ← standardLibDir.readDir do
    let name := entry.fileName
    if name.startsWith "lib" then
      symlinkForce entry.path (sharedLibDir / name)
      symlinkForce entry.path (lakeLibDir / name)
  pure lakeHome

structure LakeInvocation where
  cmd : String
  args : Array String
  env : Array (String × Option String) := #[]

private def lakeInvocationFor (buildDir : System.FilePath)
    (fingerprint : ToolchainFingerprint) :
    IO LakeInvocation := do
  let leanPrefix := System.FilePath.mk fingerprint.leanPrefix
  let exe := if System.Platform.isWindows then ".exe" else ""
  let cleanEnv : Array (String × Option String) := #[
    ("LEAN_PATH", none), ("LEAN_SRC_PATH", none),
    ("LEAN_CC", none), ("LEAN_AR", none), ("LEAN_ABORT_ON_PANIC", none)
  ]
  let default : LakeInvocation := {
    cmd := (leanPrefix / "bin" / ("lake" ++ exe)).toString
    args := #[]
    env := #[("LEAN", some (leanPrefix / "bin" / ("lean" ++ exe)).toString),
      ("LEAN_SYSROOT", some leanPrefix.toString), ("LAKE_HOME", none)] ++ cleanEnv
  }
  let libDir := System.FilePath.mk fingerprint.leanLibDir
  let standardLibDir := leanPrefix / "lib" / "lean"
  if ← samePath libDir standardLibDir then
    pure default
  else
    let lakeHome ← prepareNonstandardLakeHome buildDir leanPrefix libDir
    pure {
      cmd := (lakeHome / ".lake" / "build" / "bin" / "lake").toString
      args := #[]
      env := #[
        ("LAKE_HOME", some lakeHome.toString),
        ("LEAN", some (leanPrefix / "bin" / "lean").toString),
        ("LEAN_SYSROOT", none)
      ] ++ cleanEnv
    }

/-- Ask the selected Lake SDK for its shared-runtime inputs, without scanning its library directory. -/
def buildSharedRuntimePaths (buildDir workspace : System.FilePath)
    (fingerprint : ToolchainFingerprint) : IO (Array String) := do
  let lake ← lakeInvocationFor buildDir fingerprint
  let output ← IO.Process.output {
    cmd := lake.cmd
    args := lake.args ++ #["script", "run", "beamSharedRuntimeFiles"]
    env := lake.env
    cwd := workspace.toString
  }
  unless output.exitCode == 0 do
    throw <| IO.userError s!"selected Lake could not report its shared-runtime inputs: {output.stderr}"
  let paths ← IO.ofExcept <| (Json.parse output.stdout >>= fromJson? (α := Array String)).mapError IO.userError
  unless validSharedRuntimePaths paths do
    throw <| IO.userError "selected Lake reported invalid shared-runtime inputs"
  pure paths

private def processOutputLines (label : String) (stdout stderr : String) : List String := [
  s!"{label} stdout:",
  if stdout.trimAscii.isEmpty then "(empty)" else stdout,
  "",
  s!"{label} stderr:",
  if stderr.trimAscii.isEmpty then "(empty)" else stderr
]

/-- One completed target build: its workspace and the outputs it declared. -/
structure BuiltTarget where
  private mk ::
  toolchain : String
  workspace : System.FilePath
  daemon : System.FilePath
  plugin : System.FilePath
  /-- The SDK libraries the daemon and plugin load, as declared by the selected Lake. -/
  sharedLibraries : Array System.FilePath
  private buildDir : System.FilePath
  private lake : LakeInvocation

/-- Build one target's daemon and plugin in a fresh directory owned by the caller. -/
def buildTarget (source : System.FilePath) (toolchain : String) (fingerprint : ToolchainFingerprint)
    (buildDir : System.FilePath) : IO BuiltTarget := do
  let workspace := buildWorkspace buildDir
  prepareWorkspace source workspace
  IO.eprintln s!"building beam runtime for {toolchain}"
  let lake ← lakeInvocationFor buildDir fingerprint
  let out ← IO.Process.output {
    cmd := lake.cmd
    args := lake.args ++ #["build", "Beam.LSP:shared", "beam-daemon"]
    env := lake.env
    cwd := workspace.toString
  }
  if out.exitCode != 0 then
    throw <| IO.userError <| String.intercalate "\n" <| [
      s!"failed to build the Beam runtime for target {toolchain}",
      s!"build workspace: {workspace}",
      ""
    ] ++ processOutputLines "lake" out.stdout out.stderr
  let libraries ← buildSharedRuntimePaths buildDir workspace fingerprint
  let outputs := workspace / ".lake" / "build"
  pure {
    toolchain, workspace, buildDir, lake
    daemon := outputs / "bin" / (if System.Platform.isWindows then "beam-daemon.exe" else "beam-daemon")
    plugin := Beam.LSP.Lib.pluginSharedLibPath (outputs / "lib")
    sharedLibraries := libraries.map (fun path => outputs / System.FilePath.mk path)
  }

/-- Load `plugin` into the target's Lean and elaborate a probe. Construction passes the staged copy,
so the bytes that pass are the bytes that ship. -/
def BuiltTarget.qualify (target : BuiltTarget) (plugin : System.FilePath) : IO Unit := do
  let qualificationDir := target.buildDir / "qualification"
  IO.FS.createDirAll qualificationDir
  IO.FS.writeFile (qualificationDir / "Probe.lean") "example : True := by\n  trivial\n"
  let plugin ← Beam.resolveExistingPath plugin
  let probe ← Beam.resolveExistingPath (qualificationDir / "Probe.lean")
  let out ← IO.Process.output {
    cmd := target.lake.cmd
    args := target.lake.args ++ #[
      "env",
      "lean",
      s!"--plugin={plugin}",
      "-Dexperimental.module=true",
      probe.toString
    ]
    env := target.lake.env
    cwd := target.workspace.toString
  }
  if out.exitCode != 0 then
    throw <| IO.userError <| String.intercalate "\n" <| [
      s!"failed to qualify the Beam runtime for target {target.toolchain}",
      "the plugin did not pass the local load/elaboration probe",
      s!"plugin: {plugin}",
      s!"probe: {probe}",
      ""
    ] ++ processOutputLines "qualification" out.stdout out.stderr
  IO.FS.removeDirAll qualificationDir

end Beam.Install
