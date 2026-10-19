/-
Copyright (c) 2026 Lean FRO LLC. All rights reserved.
Released under Apache 2.0 license as described in the file LICENSE.
Author: Emilio J. Gallego Arias
-/

import Beam.System

open Std.Time

private def check : IO Unit := do
  let cases : List (Timestamp × String) := [
    (.ofSecondsSinceUnixEpoch 0, "1970-01-01T00:00:00Z"),
    (.ofSecondsSinceUnixEpoch (-1), "1969-12-31T23:59:59Z"),
    (.ofNanosecondsSinceUnixEpoch 1709251199123456789, "2024-02-29T23:59:59Z"),
    (.ofSecondsSinceUnixEpoch 1709251200, "2024-03-01T00:00:00Z")
  ]
  for (timestamp, expected) in cases do
    let actual := Beam.formatUtcTimestamp timestamp
    unless actual == expected do
      throw <| IO.userError s!"expected UTC timestamp {expected}, got {actual}"
  let before ← Timestamp.now
  let text ← Beam.utcTimestamp
  let after ← Timestamp.now
  let actual ← IO.ofExcept <| Formats.iso8601.parse text
  unless text.length == 20 && text.endsWith "Z" &&
      before.val.toSeconds ≤ actual.timestamp.val.toSeconds &&
      actual.timestamp.val.toSeconds ≤ after.val.toSeconds do
    throw <| IO.userError s!"UTC timestamp does not describe the current wall clock: {text}"

#eval check
