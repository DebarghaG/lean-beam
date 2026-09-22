import Mathlib

example (a b : Nat) : a + b = b + a := by
  omega

example : True ∧ True := by
  constructor <;> trivial

example (a b : ℝ) : (a + b) ^ 2 = a ^ 2 + 2 * a * b + b ^ 2 := by
  ring

example (x y : ℝ) (hx : 1 ≤ x) (hy : x + 2 ≤ y) : 3 ≤ y := by
  linarith

example (x : ℝ) : 0 ≤ x ^ 2 := by
  positivity

theorem bitvectorDistribution (x y : BitVec 32) : (x &&& y) ||| (x &&& ~~~y) = x := by
  bv_decide

example : (List.range 100).sum = 4950 := by
  native_decide
