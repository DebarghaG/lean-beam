import Lean

open Lean Elab Tactic

deriving instance TypeName for Nat

def snapshotReportingKey : Nat := 0

-- Cached realization results retain this finished diagnostic task and its unresolved
-- cancellation promise. Serializing the environment must settle completed reporting first.
elab "snapshot_cached_reporting" : tactic => do
  unless (← getEnv).areRealizationsEnabledForConst ``snapshotReportingKey do
    throwError "fixture requires a cached realization"
  let _ ← Meta.realizeValue ``snapshotReportingKey (0 : Nat) do
    let token ← IO.CancelToken.new
    Core.logSnapshotTask {
      stx? := none, cancelTk? := some token
      task := Task.pure ⟨{ diagnostics := .empty }, #[]⟩ }
    return (7 : Nat)

elab "snapshot_pending_reporting" : tactic => do
  let promise ← IO.Promise.new
  let token ← IO.CancelToken.new
  token.onSet (promise.resolve ⟨{ diagnostics := .empty }, #[]⟩)
  Core.logSnapshotTask { stx? := none, cancelTk? := some token, task := promise.result! }

elab "snapshot_finish_reporting" : tactic => do
  let some token := (← getThe Core.State).snapshotTasks.back?.bind (·.cancelTk?)
    | throwError "missing pending reporting"
  if ← token.isSet then throwError "export cancelled unfinished work"
  token.set

example : True := by
  snapshot_cached_reporting
  sorry
