import Lean

example (a b : Nat) : a + b = b + a := by
  omega

example : True ∧ True := by
  constructor <;> trivial
