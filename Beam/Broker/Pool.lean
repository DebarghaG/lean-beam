/-
Copyright (c) 2026 Lean FRO LLC. All rights reserved.
Released under Apache 2.0 license as described in the file LICENSE.
-/

import Beam.Broker.Errors
import Beam.Broker.Pool.Files
import Beam.LSP.RunAt
import Std.Internal.UV.TCP
import Std.Internal.UV.DNS
import Std.Sync.Mutex

open Lean

namespace Beam.Broker.Pool

private def closed (j : Json) (keys : List String) : Except String Unit := do
  let fields ← j.getObj?
  unless fields.size == keys.length && keys.all (fun key => (j.getObjVal? key).isOk) do
    throw "unexpected or missing pool configuration fields"

abbrev InputStamp := Files.Stamp

instance : FromJson InputStamp where
  fromJson? j := do
    closed j ["path", "size", "sec", "nsec", "digest"]
    let path ← j.getObjValAs? String "path"
    unless !(System.FilePath.mk path).isAbsolute && !path.isEmpty &&
        !(path.splitOn "/").contains ".." do
      throw "pool input paths must be relative"
    pure { path
           size := ← j.getObjValAs? Nat "size"
           sec := ← j.getObjValAs? Int "sec"
           nsec := ← j.getObjValAs? Nat "nsec"
           digest := ← j.getObjValAs? String "digest" }

structure Binding where
  root : String
  host : String
  port : UInt16
  snapshot : String
  binding : String
  inputsFile : String
  inputs : Array InputStamp := #[]
  deriving Inhabited

instance : FromJson Binding where
  fromJson? j := do
    closed j ["root", "host", "port", "snapshot", "binding", "inputs"]
    let root ← j.getObjValAs? String "root"
    let host ← j.getObjValAs? String "host"
    let port ← j.getObjValAs? Nat "port"
    let snapshot ← j.getObjValAs? String "snapshot"
    let binding ← j.getObjValAs? String "binding"
    let inputsFile ← j.getObjValAs? String "inputs"
    unless (System.FilePath.mk root).isAbsolute && !host.isEmpty && port > 0 && port < 65536 &&
        !snapshot.isEmpty && !binding.isEmpty && !inputsFile.isEmpty &&
        !(System.FilePath.mk inputsFile).isAbsolute && !(inputsFile.splitOn "/").contains ".." do
      throw "invalid pool binding"
    pure { root, host, port := port.toUInt16, snapshot, binding, inputsFile }

private def readBindings : IO (Except String (Array Binding)) := do
  let some file ← IO.getEnv "BEAM_LEAN_POOL_CONFIG" | return .ok #[]
  try
    let metadata ← (System.FilePath.mk file).metadata
    if metadata.byteSize.toNat > 32 * 1024 * 1024 then
      return .error "pool configuration exceeds 32 MiB"
    let text ← IO.FS.readFile file
    pure <| do
      let json ← Json.parse text
      closed json ["schema", "bindings"]
      unless (← json.getObjValAs? Nat "schema") == 2 do throw "unsupported pool configuration; attach again"
      let bindings ← json.getObjValAs? (Array Binding) "bindings"
      unless (bindings.toList.map (·.root)).eraseDups.length == bindings.size do
        throw "duplicate pool workspace binding"
      pure <| bindings.map fun binding => { binding with
        inputsFile := ((System.FilePath.mk file).parent.getD "." / binding.inputsFile).toString }
  catch error => return .error s!"cannot load pool configuration: {error}"

private initialize bindingsCache : Std.Mutex (Option (Except String (Array Binding))) ←
  Std.Mutex.new none

private initialize inputsCache :
    Std.Mutex (Std.TreeMap String (Except String (Array InputStamp))) ← Std.Mutex.new {}

private def readInputs (file : String) : IO (Except String (Array InputStamp)) := do
  try
    if (← (System.FilePath.mk file).metadata).byteSize.toNat > 32 * 1024 * 1024 then
      return .error "workspace input manifest exceeds 32 MiB"
    let text ← IO.FS.readFile file
    pure <| (Json.parse text).bind fromJson?
  catch error => return .error s!"cannot load workspace inputs: {error}"

-- Configuration is an operator setting frozen for the process lifetime, not a request argument.
def bindingFor (root : System.FilePath) : IO (Except ResponseFailure (Option Binding)) := do
  let bindings ← bindingsCache.atomically do
    if let some cached ← get then return cached
    let loaded ← readBindings
    set (some loaded)
    return loaded
  match bindings with
  | .error message =>
    return .error <| responseFailureFor .invalidParams s!"invalid BEAM_LEAN_POOL_CONFIG: {message}"
  | .ok bindings => do
    let some binding := bindings.find? (fun b => b.root == root.toString) | return .ok none
    let inputs ← inputsCache.atomically do
      if let some cached := (← get).get? binding.inputsFile then return cached
      let loaded ← readInputs binding.inputsFile
      modify (·.insert binding.inputsFile loaded)
      return loaded
    return match inputs with
      | .ok inputs => .ok <| some { binding with inputs }
      | .error message => .error <| responseFailureFor .invalidParams
          s!"invalid BEAM_LEAN_POOL_CONFIG: {message}"

def checkPath (binding : Binding) (sourcePath : System.FilePath) :
    IO (Except ResponseFailure Unit) := do
  let some name := Beam.pathRelativeToRoot? (System.FilePath.mk binding.root) sourcePath
    | return .error <| responseFailureFor .invalidParams "file is outside the attached pool project"
  unless Files.inputFile name && name.endsWith ".lean" && !Files.configurationFile name do
    return .error <| responseFailureFor .invalidParams "pool request requires a Lean source file"
  return .ok ()

structure Handle where
  beamPool : Nat := 1
  binding : String
  path : String
  version : Nat
  documentGeneration : Nat
  id : String
  deriving ToJson

instance : FromJson Handle where
  fromJson? j := do
    closed j ["beamPool", "binding", "path", "version", "documentGeneration", "id"]
    unless (← j.getObjValAs? Nat "beamPool") == 1 do throw "unsupported pool handle"
    pure {
      binding := ← j.getObjValAs? String "binding"
      path := ← j.getObjValAs? String "path"
      version := ← j.getObjValAs? Nat "version"
      documentGeneration := ← j.getObjValAs? Nat "documentGeneration"
      id := ← j.getObjValAs? String "id"
    }

def isHandle (json : Json) : Bool :=
  (json.getObjValAs? Nat "beamPool").toOption == some 1

private abbrev Action := ExceptT ResponseFailure IO

private def liftIO (act : IO α) : Action α := ExceptT.mk do
  return .ok (← act)

private def decode [FromJson α] (j : Json) : Action α :=
  match fromJson? j with
  | .ok value => pure value
  | .error message => throw <| responseFailureFor .internalError s!"invalid pool response: {message}"

private def checkStop (cancelRef? : Option (IO.Ref Bool)) (deadline : Nat) : Action Unit := do
  if let some ref := cancelRef? then
    if ← liftIO ref.get then throw <| responseFailureFor .requestCancelled "pool request cancelled"
  if (← liftIO IO.monoNanosNow) >= deadline then
    throw { error := { code := "deadlineExceeded", message := "pool request deadline exceeded" } }

private partial def waitPromise (promise : IO.Promise (Except IO.Error α))
    (cancelRef? : Option (IO.Ref Bool)) (deadline : Nat) (pollMs : UInt32 := 1) : Action α := do
  checkStop cancelRef? deadline
  if ← liftIO <| IO.hasFinished promise.result? then
    let some result ← liftIO <| IO.wait promise.result?
      | throw <| responseFailureFor .workerExited "pool connection closed"
    match result with
    | .ok value => return value
    | .error error => throw <| responseFailureFor .workerExited s!"pool connection failed: {error}"
  liftIO <| IO.sleep pollMs
  waitPromise promise cancelRef? deadline (min (2 * pollMs) 10)

private def maxFrame := 4 * 1024 * 1024

private partial def receive (socket : Std.Internal.UV.TCP.Socket) (identity : String)
    (buffer : ByteArray) (cancelRef? : Option (IO.Ref Bool)) (deadline : Nat) : Action Json := do
  checkStop cancelRef? deadline
  if let some pos := buffer.data.findIdx? (· == 10) then
    if pos > maxFrame then throw <| responseFailureFor .internalError "pool frame exceeds limit"
    let line := String.fromUTF8? (buffer.extract 0 pos)
    let some line := line | throw <| responseFailureFor .internalError "pool response is not UTF-8"
    let message ← match Json.parse line with
      | .ok value => pure value
      | .error _ => throw <| responseFailureFor .internalError "malformed pool response"
    unless (← decode (← field message "id") : String) == identity do
      throw <| responseFailureFor .internalError "pool response id mismatch"
    if (message.getObjVal? "event").isOk then
      -- Remote progress is not yet part of the broker's typed stream contract.
      return ← receive socket identity (buffer.extract (pos + 1) buffer.size) cancelRef? deadline
    if (← decode (← field message "ok") : Bool) then
      return ← field message "result"
    let error ← field message "error"
    let code : String ← decode (← field error "code")
    let text : String ← decode (← field error "message")
    throw { error := { code := if code == "workerLost" then "workerExited" else code
                       message := text
                       data? := (error.getObjVal? "data").toOption } }
  if buffer.size > maxFrame then
    throw <| responseFailureFor .internalError "pool frame exceeds limit"
  let promise ← liftIO <| socket.recv? 65536
  let some bytes ← waitPromise promise cancelRef? deadline
    | throw <| responseFailureFor .workerExited "pool connection closed before a result"
  receive socket identity (buffer ++ bytes) cancelRef? deadline
where
  field (json : Json) (name : String) : Action Json :=
    match json.getObjVal? name with
    | .ok value => pure value
    | .error _ => throw <| responseFailureFor .internalError s!"pool response lacks {name}"

-- A native socket per admitted request gives existing broker cancellation an exact remote lifetime.
-- No Python helper process is launched in the query path.
def request (binding : Binding) (op : String) (args : Json)
    (cancelRef? : Option (IO.Ref Bool) := none)
    (timeoutMs : Nat := 120000) : IO (Except ResponseFailure Json) := do
  let socket ← Std.Internal.UV.TCP.Socket.new
  try
    (do
      let some token ← liftIO <| IO.getEnv "BEAM_POOL_TOKEN"
        | throw <| responseFailureFor .invalidParams "BEAM_POOL_TOKEN is required for the configured pool"
      unless token.utf8ByteSize >= 16 do
        throw <| responseFailureFor .invalidParams "invalid BEAM_POOL_TOKEN"
      let started ← liftIO IO.monoNanosNow
      let deadline := started + timeoutMs * 1000000
      let identity := s!"{← liftIO IO.Process.getPID}-{started}"
      let addresses ← liftIO <| Std.Internal.UV.DNS.getAddrInfo binding.host "" 0
      let addresses : Array Std.Net.IPAddr ← waitPromise addresses cancelRef? deadline
      let ipv4? := addresses.find? fun address => match address with
        | Std.Net.IPAddr.v4 _ => true
        | Std.Net.IPAddr.v6 _ => false
      let some address := ipv4?.orElse (fun _ => addresses[0]?)
        | throw <| responseFailureFor .workerExited "pool hostname has no address"
      let address : Std.Net.SocketAddress := match (address : Std.Net.IPAddr) with
        | Std.Net.IPAddr.v4 addr => Std.Net.SocketAddress.v4 { addr, port := binding.port }
        | Std.Net.IPAddr.v6 addr => Std.Net.SocketAddress.v6 { addr, port := binding.port }
      let connected ← liftIO <| socket.connect address
      waitPromise connected cancelRef? deadline
      liftIO socket.noDelay
      let frame := Json.mkObj [("v", toJson (1 : Nat)), ("id", toJson identity),
        ("token", toJson token), ("op", toJson op), ("args", args)]
      let data := (frame.compress ++ "\n").toUTF8
      if data.size > maxFrame then
        throw <| responseFailureFor .invalidParams "pool request exceeds frame limit"
      let sent ← liftIO <| socket.send #[data]
      waitPromise sent cancelRef? deadline
      receive socket identity {} cancelRef? deadline).run
  catch error => return .error <| responseFailureFor .workerExited s!"pool transport failed: {error}"
  finally
    try socket.cancelRecv catch _ => pure ()
    try discard <| socket.shutdown catch _ => pure ()

private structure ProjectState where
  watch? : Option Files.Watch := none
  inputs? : Option (Std.TreeMap String InputStamp) := none
  files : Std.TreeMap String (Option String) := {}
  sequence : Nat := 0
  dirty : Bool := true

private initialize projectCaches :
    Std.Mutex (Std.TreeMap String (Std.Mutex ProjectState)) ← Std.Mutex.new {}

private def projectCache (binding : Binding) : IO (Std.Mutex ProjectState) :=
  projectCaches.atomically do
    if let some cache := (← get).get? binding.binding then return cache
    let cache ← Std.Mutex.new ({} : ProjectState)
    modify (·.insert binding.binding cache)
    return cache

structure ProjectRevision where
  id : String
  sequence : Nat

private def uploadFile (binding : Binding) (group : String) (input : InputStamp)
    (cancelRef? : Option (IO.Ref Bool)) : Action String := do
  let root := System.FilePath.mk binding.root
  let file ← liftIO <| IO.FS.Handle.mk (root / input.path) .read
  let upload := s!"{← liftIO IO.Process.getPID}-{← liftIO IO.monoNanosNow}"
  let mut offset := 0
  let mut digest := ""
  repeat
    let bytes ← liftIO <| file.read 65536
    let last := offset + bytes.size == input.size
    if offset + bytes.size > input.size || (bytes.isEmpty && !last) then
      throw <| responseFailureFor .contentModified "pool input changed during upload"
    let reply ← ExceptT.mk <| request binding "put" (Json.mkObj [
      ("group", toJson group), ("upload", toJson upload), ("offset", toJson offset),
      ("data", toJson (Files.hex bytes)), ("last", toJson last)]) cancelRef?
    offset := offset + bytes.size
    if last then
      digest ← decode <| (reply.getObjVal? "digest").toOption.getD Json.null
      break
  unless input.sameMetadata (← liftIO <| Files.stamp root input.path) do
    throw <| responseFailureFor .contentModified "pool input changed during upload"
  return digest

private def prepareProjectState (binding : Binding) (group : String) (old : ProjectState)
    (cancelRef? : Option (IO.Ref Bool)) : Action (ProjectState × ProjectRevision) := do
  let changed ← match old.watch? with
    | some watch => liftIO <| Files.changed watch
    | none => pure #[]
  let root := System.FilePath.mk binding.root
  let baseline := Files.stampMap binding.inputs
  let mut state := old
  if old.dirty || old.watch?.isNone || !changed.isEmpty then
    let watch ← liftIO Files.watchNew
    let current := Files.stampMap (← liftIO <| Files.scan root watch)
    let previous := old.inputs?.getD baseline
    let mut files := old.files
    let forced := changed.foldl (fun acc path => acc.insert path) ({} : Std.TreeSet String)
    let forced := if old.dirty && old.inputs?.isSome then forced.insert "*" else forced
    for (path, input) in current.toList do
      if (previous.get? path).any (·.sameMetadata input) &&
          !forced.contains path && !forced.contains "*" then continue
      let digest ← uploadFile binding group input cancelRef?
      if let some base := baseline.get? path then
        if digest == base.digest then
          files := files.erase path
          continue
        if Files.configurationFile path then
          throw <| responseFailureFor .contentModified
            s!"project configuration changed: {path}; prepare the new environment and restart Beam"
      files := files.insert path (some digest)
    for (path, _) in previous.toList do
      if current.contains path then continue
      if Files.configurationFile path then
        throw <| responseFailureFor .contentModified s!"project configuration disappeared: {path}"
      files := if baseline.contains path then files.insert path none else files.erase path
    unless (← liftIO <| Files.changed watch).isEmpty do
      throw <| responseFailureFor .contentModified "project changed while preparing a pool revision"
    state := { watch? := some watch, inputs? := some current, files,
               sequence := if old.inputs?.isNone || files.toList != old.files.toList
                 then old.sequence + 1 else old.sequence, dirty := false }
  let args := Json.mkObj [
    ("snapshot", toJson binding.snapshot), ("group", toJson group),
    ("sequence", toJson state.sequence),
    ("files", Json.mkObj <| state.files.toList.map fun (path, digest) => (path, toJson digest))]
  let mut reply ← ExceptT.mk <| request binding "publish" args cancelRef?
  let missing : Array String ← decode <| (reply.getObjVal? "missing").toOption.getD Json.null
  if !missing.isEmpty then
    let inputs := state.inputs?.getD baseline
    for digest in missing do
      let some (path, _) := state.files.toList.find? (fun (_, d) => d == some digest)
        | throw <| responseFailureFor .internalError "pool requested an unrelated input"
      let some input := inputs.get? path
        | throw <| responseFailureFor .contentModified "pool input disappeared"
      unless (← uploadFile binding group input cancelRef?) == digest do
        throw <| responseFailureFor .contentModified "pool input changed before cache recovery"
    reply ← ExceptT.mk <| request binding "publish" args cancelRef?
  let id : String ← decode <| (reply.getObjVal? "revision").toOption.getD Json.null
  if let some watch := state.watch? then
    unless (← liftIO <| Files.changed watch).isEmpty do
      throw <| responseFailureFor .contentModified "project changed before pool execution"
  return (state, { id, sequence := state.sequence })

def prepareProject (binding : Binding) (group : String) (cancelRef? : Option (IO.Ref Bool)) :
    IO (Except ResponseFailure ProjectRevision) := do
  let cache ← projectCache binding
  cache.atomically do
    let old ← get
    -- Keep a failed/cancelled upload dirty even after its watcher events have been read.
    set { old with dirty := true }
    try
      match ← (prepareProjectState binding group old cancelRef?).run with
      | .error failure => return .error failure
      | .ok (state, revision) =>
          set state
          return .ok revision
    catch error =>
      return .error <| responseFailureFor .contentModified s!"cannot prepare pool project: {error}"

def projectUnchanged (binding : Binding) (revision : ProjectRevision) :
    IO (Except ResponseFailure Unit) := do
  let cache ← projectCache binding
  cache.atomically do
    let state ← get
    let changed ← match state.watch? with
      | some watch => Files.changed watch
      | none => pure #["*"]
    unless changed.isEmpty do set { state with dirty := true }
    if state.dirty || state.sequence != revision.sequence || !changed.isEmpty then
      return .error <| responseFailureFor .contentModified "project changed during pool execution"
    return .ok ()

-- Convert the closed MCP result to the existing broker/LSP result field names exactly once.
def result (reply : Json) : Except ResponseFailure Json := do
  let decoded : Except String Json := do
    let r ← reply.getObjVal? "result"
    let success ← r.getObjValAs? Bool "success"
    let messages ← r.getObjValAs? (Array Beam.LSP.RunAt.Message) "messages"
    let traces ← r.getObjValAs? (Array String) "traces"
    let proofState? ← r.getObjValAs? (Option Beam.LSP.Lib.ProofState) "proof_state"
    let handle? ← r.getObjValAs? (Option String) "next_handle"
    let result := toJson ({ success, messages, traces, proofState? } : Beam.LSP.RunAt.Result)
    pure <| match handle? with
      | some handle => result.setObjVal! "handle" (toJson handle)
      | none => result
  match decoded with
  | .ok value => pure value
  | .error message => throw <| responseFailureFor .internalError s!"invalid pool result: {message}"

end Beam.Broker.Pool
