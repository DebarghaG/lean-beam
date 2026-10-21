/- Copyright (c) 2026. Released under Apache 2.0 license as described in LICENSE. -/
import Lean.CompactedRegion

namespace Beam.LSP.RunAt

/-- Owns a snapshot and its mapped region, destroying the snapshot before unmapping. -/
opaque ImportedSnapshot (α : Type) : NonemptyType

@[extern "lean_beam_imported_snapshot_new"]
unsafe opaque ImportedSnapshot.adopt {α : Type} (data : α)
    (region : Lean.CompactedRegion) (bytes : USize) : IO (ImportedSnapshot α).type

/-- Internal borrowed view. Keep the owner alive until all temporary views have been dropped. -/
@[extern "lean_beam_imported_snapshot_data"]
opaque ImportedSnapshot.data {α : Type} [Nonempty α] (snapshot : @& (ImportedSnapshot α).type) : α

@[extern "lean_beam_imported_snapshot_derive"]
opaque ImportedSnapshot.derive {α : Type} (snapshot : @& (ImportedSnapshot α).type)
    (data : α) : IO (ImportedSnapshot α).type

@[extern "lean_beam_region_live_bytes"]
opaque ImportedSnapshot.liveBytes : IO USize

/-- An effectful use after serialization prevents the compiler from releasing the mapping early. -/
@[extern "lean_beam_region_keep_alive"]
opaque ImportedSnapshot.keepAlive {α : Type} (owner : @& (ImportedSnapshot α).type) : IO Unit

end Beam.LSP.RunAt
