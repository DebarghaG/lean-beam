import Lean

universe u
namespace SnapshotTransfer

class Witness (α : Type u) where
  value : α

def seven : Nat := 7
local notation "answer" => seven

example (α : Type u) [Witness α] (x : α) : x = x := by
  sorry

end SnapshotTransfer
