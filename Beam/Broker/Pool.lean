/-
Copyright (c) 2026 Lean FRO LLC. All rights reserved.
Released under Apache 2.0 license as described in the file LICENSE.
-/

import Beam.Broker.Errors
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

structure InputStamp where
  path : String
  size : Nat
  sec : Int
  nsec : Nat
  deriving Inhabited

instance : FromJson InputStamp where
  fromJson? j := do
    closed j ["path", "size", "sec", "nsec"]
    let path ← j.getObjValAs? String "path"
    unless !(System.FilePath.mk path).isAbsolute && !path.isEmpty &&
        !(path.splitOn "/").contains ".." do
      throw "pool input paths must be relative"
    pure { path
           size := ← j.getObjValAs? Nat "size"
           sec := ← j.getObjValAs? Int "sec"
           nsec := ← j.getObjValAs? Nat "nsec" }

structure Binding where
  root : String
  host : String
  port : UInt16
  snapshot : String
  binding : String
  inputs : Array InputStamp
  deriving Inhabited

instance : FromJson Binding where
  fromJson? j := do
    closed j ["root", "host", "port", "snapshot", "binding", "inputs"]
    let root ← j.getObjValAs? String "root"
    let host ← j.getObjValAs? String "host"
    let port ← j.getObjValAs? Nat "port"
    let snapshot ← j.getObjValAs? String "snapshot"
    let binding ← j.getObjValAs? String "binding"
    unless (System.FilePath.mk root).isAbsolute && !host.isEmpty && port > 0 && port < 65536 &&
        !snapshot.isEmpty && !binding.isEmpty do
      throw "invalid pool binding"
    pure { root, host, port := port.toUInt16, snapshot, binding
           inputs := ← j.getObjValAs? (Array InputStamp) "inputs" }

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
      unless (← json.getObjValAs? Nat "schema") == 1 do throw "unsupported pool configuration"
      let bindings ← json.getObjValAs? (Array Binding) "bindings"
      unless (bindings.toList.map (·.root)).eraseDups.length == bindings.size do
        throw "duplicate pool workspace binding"
      pure bindings
  catch error => return .error s!"cannot load pool configuration: {error}"

private initialize bindingsCache : Std.Mutex (Option (Except String (Array Binding))) ←
  Std.Mutex.new none

-- Configuration is an operator setting frozen for the process lifetime, not a request argument.
def bindingFor (root : System.FilePath) : IO (Except ResponseFailure (Option Binding)) := do
  let bindings ← bindingsCache.atomically do
    if let some cached ← get then return cached
    let loaded ← readBindings
    set (some loaded)
    return loaded
  pure <| match bindings with
    | .error message => .error <| responseFailureFor .invalidParams s!"invalid BEAM_LEAN_POOL_CONFIG: {message}"
    | .ok bindings => .ok <| bindings.find? (fun b => b.root == root.toString)

-- Attachment checks SHA-256 identities once. Metadata checks detect ordinary changes to every
-- prepared input at request boundaries; the selected source is also compared byte-for-byte remotely.
def checkInputs (binding : Binding) (sourcePath : System.FilePath) :
    IO (Except ResponseFailure Unit) := do
  unless binding.inputs.any (fun input => System.FilePath.mk binding.root / input.path == sourcePath) do
    return .error <| responseFailureFor .invalidParams "file is outside the attached pool snapshot"
  for input in binding.inputs do
    let path := System.FilePath.mk binding.root / input.path
    if path == sourcePath then continue
    try
      let current ← path.metadata
      unless current.byteSize.toNat == input.size && current.modified.sec == input.sec &&
          current.modified.nsec.toNat == input.nsec do
        return .error <| responseFailureFor .contentModified
          s!"prepared pool input changed: {input.path}; attach the rebuilt snapshot and restart Beam"
    catch _ =>
      return .error <| responseFailureFor .contentModified s!"prepared pool input disappeared: {input.path}"
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
    (cancelRef? : Option (IO.Ref Bool)) (deadline : Nat) : Action α := do
  checkStop cancelRef? deadline
  if ← liftIO <| IO.hasFinished promise.result? then
    let some result ← liftIO <| IO.wait promise.result?
      | throw <| responseFailureFor .workerExited "pool connection closed"
    match result with
    | .ok value => return value
    | .error _ => throw <| responseFailureFor .workerExited "pool connection failed"
  liftIO <| IO.sleep 1
  waitPromise promise cancelRef? deadline

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
      -- The broker emits its own typed progress. Never parse human MCP progress strings.
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
  catch _ => return .error <| responseFailureFor .workerExited "pool transport failed"
  finally
    try socket.cancelRecv catch _ => pure ()
    try discard <| socket.shutdown catch _ => pure ()

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
