import Lean

theorem bitvectorDistribution (x y : BitVec 32) : (x &&& y) ||| (x &&& ~~~y) = x := by
--v $/lean/runAt: {"text":"bv_decide"}
  bv_decide
