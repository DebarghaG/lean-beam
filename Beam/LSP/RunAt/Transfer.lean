/- Copyright (c) 2026. Released under Apache 2.0 license as described in LICENSE. -/
import Beam.LSP.RunAt
import Lean.CompactedRegion

open Lean Lean.Server Beam.LSP.Lib

namespace Beam.LSP.RunAt.Transfer

def exportMethod : String := "$/lean/beam/exportSnapshot"
def importMethod : String := "$/lean/beam/importSnapshot"
def contextMethod : String := "$/lean/beam/snapshotContext"

/-- The worker verifies the binary and dependency hashes before invoking this trusted local API. -/
structure Identity where
  format : Nat := 1
  compiler : String
  runtime : String
  source : String
  mainModule : Name
  imports : Array Name
  regionSizes : Array Nat
  deriving FromJson, ToJson, BEq

private def identity (env : Environment) : RequestM Identity := do
  let some runtime ← IO.getEnv "BEAM_SNAPSHOT_RUNTIME"
    | throw <| RequestError.invalidParams "snapshot transfer requires a verified worker runtime"
  unless runtime.length == 64 && runtime.toList.all Char.isHexDigit do
    throw <| RequestError.invalidParams "invalid snapshot runtime identity"
  let doc ← RequestM.readDoc
  return {
    compiler := Lean.githash, runtime, source := doc.meta.text.source
    mainModule := env.header.mainModule, imports := env.header.moduleNames
    regionSizes := env.header.regions.map (·.size.toNat) }

structure Context where
  identity : Identity
  regions : Array String
  residentBytes : Nat
  processId : Nat
  deriving FromJson, ToJson

structure ExportResult extends Context where
  preparationNanos : Nat
  saveNanos : Nat
  deriving FromJson, ToJson

private def context (env : Environment) : RequestM Context := do
  return {
    identity := ← identity env
    regions := env.header.regions.map (·.filePath.toString)
    residentBytes := (← ImportedSnapshot.liveBytes).toNat
    processId := (← IO.Process.getPID).toNat }

structure ContextParams where
  textDocument : Lean.Lsp.VersionedTextDocumentIdentifier
  deriving FromJson, ToJson

instance : Lean.Lsp.FileSource ContextParams where
  fileSource p := p.textDocument.uri

def describe (p : ContextParams) : RequestM (RequestTask Context) := do
  requireDocumentVersion p.textDocument
  withRunAtSnapAtPos ⟨0, 0⟩ fun snap => context snap.env

structure ExportParams where
  textDocument : Lean.Lsp.VersionedTextDocumentIdentifier
  handle : Handle
  path : String
  deriving FromJson, ToJson

instance : Lean.Lsp.FileSource ExportParams where
  fileSource p := p.textDocument.uri

structure ImportParams where
  textDocument : Lean.Lsp.VersionedTextDocumentIdentifier
  path : String
  identity : Identity
  deriving FromJson, ToJson

instance : Lean.Lsp.FileSource ImportParams where
  fileSource p := p.textDocument.uri

private unsafe def saveUnsafe (path : System.FilePath) (snapshot : ProofSnapshot) : IO Unit := do
  -- Requests install fresh cancellation on resume. Editor reporting tasks are not
  -- proof state and may contain unresolved cancellation promises. Keep all eight
  -- semantic states/contexts, including the full elaboration environment.
  let data := { snapshot with
    coreContext := { snapshot.coreContext with cancelTk? := none }
    coreState := { snapshot.coreState with snapshotTasks := #[], infoState := {} }
    termContext := { snapshot.termContext with tacSnap? := none } }
  let compactor ← CompactedRegion.save path `Beam.ProofSnapshot data
    snapshot.coreState.env.header.regions none (allowClosures := true)
  Runtime.forget compactor

@[implemented_by saveUnsafe]
private opaque save (path : System.FilePath) (snapshot : ProofSnapshot) : IO Unit

private unsafe def loadUnsafe (path : System.FilePath) (regions : Array CompactedRegion) :
    IO StoredHandleState := do
  let (snapshot, region) ← CompactedRegion.read (α := ProofSnapshot) path regions
  return .importedProof (← ImportedSnapshot.adopt snapshot region region.size)

@[implemented_by loadUnsafe]
private opaque load (path : System.FilePath) (regions : Array CompactedRegion) : IO StoredHandleState

-- Like Lean.Elab.Frontend's incremental-save preparation, settle cancellation promises
-- retained by diagnostic trees (including trees cached inside environment realizations).
-- First collect and check the entire tree: never cancel unfinished elaboration to export it.
private partial def completedTokens (tree : Language.SnapshotTree)
    (tokens : Array IO.CancelToken := #[]) : IO (Array IO.CancelToken) := do
  let mut tokens := tokens
  for child in tree.children do
    let some finished ← child.get?
      | throw <| IO.userError "snapshot export requires completed document and proof reporting"
    tokens ← completedTokens finished tokens
    if let some tk := child.cancelTk? then
      tokens := tokens.push tk
  return tokens

def exportHandle (p : ExportParams) : RequestM (RequestTask ExportResult) := do
  requireDocumentVersion p.textDocument
  withStoredHandle p.handle false fun stored => do
    withRunAtSnapAtPos ⟨0, 0⟩ fun localHeader => do
      try
        let started ← IO.monoNanosNow
        let snapshot ← match stored.state with
          | .proof snapshot => pure snapshot
          | .importedProof owner => pure (ImportedSnapshot.data owner)
          | .command _ => throw <| RequestError.invalidParams "only proof handles can be transferred"
        -- Imported graphs can retain the sender's dependency file names. Publish the
        -- receiving process's paths for hashing; the mapped regions themselves are
        -- already relocated by Lean and remain dependencies of subsequent exports.
        let stamp ← context localHeader.env
        unless stamp.identity == (← identity snapshot.coreState.env) do
          throw <| RequestError.invalidParams "snapshot import environment differs from the document"
        let doc ← RequestM.readDoc
        let tokens ← completedTokens (Language.toSnapshotTree doc.initSnap)
        let tokens ← completedTokens
          ⟨{ diagnostics := .empty }, snapshot.coreState.snapshotTasks⟩ tokens
        checkRequestCancelled
        for tk in tokens do tk.set
        let prepared ← IO.monoNanosNow
        save p.path snapshot
        let saved ← IO.monoNanosNow
        checkRequestCancelled
        return { toContext := stamp
                 preparationNanos := prepared - started
                 saveNanos := saved - prepared }
      finally
        stored.state.keepAlive

def importHandle (p : ImportParams) : RequestM (RequestTask Handle) := do
  requireDocumentVersion p.textDocument
  withRunAtSnapAtPos ⟨0, 0⟩ fun snap => do
    unless p.identity == (← identity snap.env) do
      throw <| RequestError.invalidParams "incompatible proof snapshot identity"
    let snapshot ← load p.path snap.env.header.regions
    checkRequestCancelled
    mintHandle snapshot

end Beam.LSP.RunAt.Transfer
