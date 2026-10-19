/-
Copyright (c) 2026 Lean FRO LLC. All rights reserved.
Released under Apache 2.0 license as described in the file LICENSE.
Author: Emilio J. Gallego Arias
-/

import Lean
import Beam.Cli.RuntimeBundle.Fingerprint
import Beam.Cli.RuntimeBundle.Paths
import Beam.Path
import Beam.System
import Beam.Cli.ConstructionHash

open Lean

namespace Beam.Cli

def bundleMetadataSchemaVersion : Nat := 6

/-- The build reports SDK dependencies explicitly; cache admission never infers them from files. -/
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

private structure BundleMetadata where
  schemaVersion : Nat
  toolchain : String
  toolchainFingerprint : ToolchainFingerprint
  sourceHash : String
  workspace : String
  builtAt : String
  sharedLibraries : Array String
  artifacts : Array Beam.Installation.Artifact
  deriving FromJson, ToJson

private def checkBundleMetadataShape (metadata : BundleMetadata) : Except String Unit := do
  if metadata.schemaVersion != bundleMetadataSchemaVersion then
    throw s!"unsupported bundle metadata schemaVersion {metadata.schemaVersion}"
  if metadata.toolchain.isEmpty then
    throw "bundle metadata toolchain must not be empty"
  if metadata.toolchainFingerprint.leanVersion.isEmpty ||
      metadata.toolchainFingerprint.leanPrefix.isEmpty ||
      metadata.toolchainFingerprint.leanLibDir.isEmpty ||
      metadata.toolchainFingerprint.lakeVersion.isEmpty ||
      !Beam.Installation.hexDigest 40 metadata.toolchainFingerprint.compilerRevision then
    throw "bundle metadata toolchain fingerprint fields must not be empty"
  if metadata.sourceHash.isEmpty then
    throw "bundle metadata sourceHash must not be empty"
  if metadata.workspace.isEmpty then
    throw "bundle metadata workspace must not be empty"
  if metadata.builtAt.isEmpty then
    throw "bundle metadata builtAt must not be empty"
  unless validSharedRuntimePaths metadata.sharedLibraries do
    throw "bundle metadata must name complete relative shared-library inputs"

private def checkBundleMetadataMatches
    (toolchain srcHash : String)
    (fingerprint : ToolchainFingerprint)
    (metadata : BundleMetadata) : Except String Unit := do
  if metadata.toolchain != toolchain then
    throw s!"bundle metadata toolchain mismatch: expected {toolchain}, got {metadata.toolchain}"
  if metadata.toolchainFingerprint != fingerprint then
    throw "bundle metadata toolchain fingerprint mismatch"
  if metadata.sourceHash != srcHash then
    throw s!"bundle metadata sourceHash mismatch: expected {srcHash}, got {metadata.sourceHash}"

private def bundleMetadataWorkspaceMatches
    (metadata : BundleMetadata) (workspace : System.FilePath) : IO Bool :=
  Beam.sameFilePath (System.FilePath.mk metadata.workspace) workspace

private def outputPaths (workspace : System.FilePath) (sharedLibraries : Array String) :
    Array System.FilePath :=
  let paths := leanBundlePathsFor workspace
  #[paths.daemon, paths.plugin] ++ sharedLibraries.map (fun path => workspace / ".lake/build" / System.FilePath.mk path)

private def relativeOutput (workspace path : System.FilePath) : String :=
  String.intercalate "/" (System.FilePath.mk (Beam.pathRelativeToRootOrSelf workspace path)).components

/-- Capture before qualification; the same bytes must survive qualification and publication. -/
def snapshotBundleArtifacts (hashes : ConstructionHash.Context) (workspace : System.FilePath) (sharedLibraries : Array String) :
    IO (Array Beam.Installation.Artifact) := do
  (outputPaths workspace sharedLibraries).mapM fun path => do
    if (← Beam.pathIsLink path) || !(← Beam.regularNonSymlinkFile path) then
      throw <| IO.userError s!"missing regular construction output: {path}"
    let relative := relativeOutput workspace path
    pure { source := relative, destination := relative, hash := ← hashes.fileHash path }

private def verifyBundleArtifacts (hashes : ConstructionHash.Context) (workspace : System.FilePath) (sharedLibraries : Array String)
    (artifacts : Array Beam.Installation.Artifact) : IO Bool := do
  let expected := (outputPaths workspace sharedLibraries).map (relativeOutput workspace)
  unless artifacts.map (·.source) == expected && artifacts.map (·.destination) == expected &&
      artifacts.all (fun artifact => Beam.Installation.relativePath artifact.source &&
        Beam.Installation.hexDigest 64 artifact.hash) do return false
  pure ((← snapshotBundleArtifacts hashes workspace sharedLibraries) == artifacts)

/-- A cache admission result retains the successful build's complete byte inventory. -/
structure VerifiedBundle where
  private mk ::
  private hashes : ConstructionHash.Context
  private workspace : System.FilePath
  private sharedLibraries : Array String
  private artifacts : Array Beam.Installation.Artifact

def VerifiedBundle.paths (bundle : VerifiedBundle) : LeanBundlePaths :=
  { leanBundlePathsFor bundle.workspace with
    sharedLibraries := bundle.sharedLibraries.map (fun path => bundle.workspace / ".lake/build" / System.FilePath.mk path) }

/-- Copy only recorded outputs, then verify the staged bytes against the qualified record. -/
def VerifiedBundle.copyFile (bundle : VerifiedBundle) (source destination : System.FilePath) : IO Unit := do
  let relative := relativeOutput bundle.workspace source
  let some artifact := bundle.artifacts.find? (·.source == relative)
    | throw <| IO.userError s!"unrecorded construction output: {source}"
  if (← Beam.pathIsLink source) || !(← Beam.regularNonSymlinkFile source) then
    throw <| IO.userError s!"missing regular construction output: {source}"
  IO.FS.createDirAll (destination.parent.getD ".")
  IO.FS.writeBinFile destination (← IO.FS.readBinFile source)
  unless (← bundle.hashes.fileHash destination) == artifact.hash do
    throw <| IO.userError s!"construction output changed before staging: {source}"

def bundleMetadataPath (bundleDir : System.FilePath) : System.FilePath :=
  bundleDir / "metadata.json"

inductive CacheFailure where
  | invalid (detail : String)
  | io (error : IO.Error)

def CacheFailure.message : CacheFailure → String
  | .invalid detail => detail
  | .io error => error.toString

/-- Only actual absence permits a cache miss, including when a parent has not been created.
Windows metadata follows links, so inspect directory entries before declaring a missing target absent. -/
private partial def cacheEntryPresent (path : System.FilePath) : IO Bool := do
  try
    discard <| path.symlinkMetadata
    return true
  catch
  | error@(.noFileOrDirectory ..) =>
      let some parent := path.parent | throw error
      if parent == path then throw error
      unless ← cacheEntryPresent parent do return false
      -- Windows directory enumeration may return no entries for an unusable junction.
      -- Establish that the present parent is traversable before interpreting an empty listing.
      let resolvedParent ← Beam.resolveExistingPath parent
      unless (← resolvedParent.metadata).type == .dir do
        throw <| IO.userError s!"cache ancestor must be a directory: {parent}"
      return (← resolvedParent.readDir).any fun entry =>
        let name := path.fileName.getD ""
        if System.Platform.isWindows then entry.fileName.toLower == name.toLower
        else entry.fileName == name
  | error => throw error

private def readBundleMetadata (bundleDir : System.FilePath) :
    IO (Except CacheFailure (Option BundleMetadata)) := do
  let path := bundleMetadataPath bundleDir
  try
    unless ← cacheEntryPresent path do return .ok none
    if (← Beam.pathIsLink path) || (← path.symlinkMetadata).type != .file then
      return .error (.invalid s!"metadata must be a regular non-symlinked file: {path}")
    return (Json.parse (← IO.FS.readFile path) >>= fromJson?).map some |>.mapError .invalid
  catch error => return .error (.io error)

def bundleMetadataJson
    (toolchain srcHash : String)
    (fingerprint : ToolchainFingerprint)
    (workspace : System.FilePath)
    (builtAt : String) (sharedLibraries : Array String) (artifacts : Array Beam.Installation.Artifact) : Json :=
  toJson ({
    schemaVersion := bundleMetadataSchemaVersion
    toolchain
    toolchainFingerprint := fingerprint
    sourceHash := srcHash
    workspace := workspace.toString
    builtAt
    sharedLibraries
    artifacts
  } : BundleMetadata)

/-- Shared publication/cache checks establish the same structural and byte invariants. -/
private def verifyBundleMetadata (hashes : ConstructionHash.Context) (bundleDir : System.FilePath)
    (toolchain srcHash : String) (fingerprint : ToolchainFingerprint) (metadata : BundleMetadata) :
    IO (Except CacheFailure VerifiedBundle) := do
  match checkBundleMetadataShape metadata *> checkBundleMetadataMatches toolchain srcHash fingerprint metadata with
  | .error detail => return .error (.invalid detail)
  | .ok () => pure ()
  try
    let workspace := bundleWorkspaceFor bundleDir
    unless ← bundleMetadataWorkspaceMatches metadata workspace do
      return .error (.invalid "bundle metadata workspace mismatch")
    unless ← verifyBundleArtifacts hashes workspace metadata.sharedLibraries metadata.artifacts do
      return .error (.invalid "recorded construction outputs differ from qualified bytes")
    return .ok ⟨hashes, workspace, metadata.sharedLibraries, metadata.artifacts⟩
  catch error => return .error (.io error)

/-- Preserve absence separately from invalid records and failed byte verification. -/
def readVerifiedTarget (hashes : ConstructionHash.Context) (bundleDir : System.FilePath)
    (toolchain srcHash : String) (fingerprint : ToolchainFingerprint) :
    IO (Except CacheFailure (Option VerifiedBundle)) := do
  match ← readBundleMetadata bundleDir with
  | .error failure => pure (.error failure)
  | .ok none => pure (.ok none)
  | .ok (some metadata) => return (← verifyBundleMetadata hashes bundleDir toolchain srcHash fingerprint metadata).map some

/-- Publish the qualified record and return the verified values, without rereading mutable inputs. -/
def writeBundleMetadata (hashes : ConstructionHash.Context) (bundleDir : System.FilePath) (toolchain srcHash : String)
    (fingerprint : ToolchainFingerprint) (workspace : System.FilePath)
    (sharedLibraries : Array String) (artifacts : Array Beam.Installation.Artifact) : IO VerifiedBundle := do
  let metadata : BundleMetadata := {
    schemaVersion := bundleMetadataSchemaVersion, toolchain, toolchainFingerprint := fingerprint
    sourceHash := srcHash, workspace := workspace.toString, builtAt := ← Beam.utcTimestamp
    sharedLibraries, artifacts
  }
  let verified ← IO.ofExcept <| (← verifyBundleMetadata hashes bundleDir toolchain srcHash fingerprint metadata).mapError
    (IO.userError ∘ CacheFailure.message)
  let path := bundleMetadataPath bundleDir
  if let some parent := path.parent then IO.FS.createDirAll parent
  let temporary := path.addExtension "tmp"
  IO.FS.writeFile temporary ((toJson metadata).pretty ++ "\n")
  IO.FS.rename temporary path
  pure verified

end Beam.Cli
