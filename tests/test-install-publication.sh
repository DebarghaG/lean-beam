#!/usr/bin/env bash

# Copyright (c) 2026 Lean FRO LLC. All rights reserved.
# Released under Apache 2.0 license as described in the file LICENSE.

# Exercise publication without building or modifying an installed runtime.
set -euo pipefail
cd "$(dirname "$0")/.."
. scripts/install-beam.sh
. tests/lib/assertions.sh

tmp_root="$(mktemp -d /tmp/beam-install-publication-XXXXXX)"
trap 'rm -rf -- "$tmp_root"' EXIT
physical_root="$(cd -P "$tmp_root" && pwd)"
mkdir -p "$physical_root/real/install/versions/old/bin" "$physical_root/real/install/versions/new/bin" \
  "$physical_root/bin" "$physical_root/foreign"
ln -s "$physical_root/real" "$physical_root/alias"
set_install_root "$physical_root/real/install"
set_bin_home "$physical_root/bin"
dont_ask=1
old="$versions_root/old"
new="$versions_root/new"
printf 'old\n' >"$old/bin/lean-beam"
printf 'new\n' >"$new/bin/lean-beam"

case "${1:-all}" in
  aliases|all)
    ln -s "$physical_root/alias/install/versions/old" "$current_root"
    ln -s "$physical_root/alias/install/current/bin/lean-beam" "$bin_home/lean-beam"
    verify_publish_targets
    # Relative targets and missing generations remain repairable.
    rm "$current_root"
    ln -s versions/missing "$current_root"
    verify_publish_targets
    rm "$current_root"
    ln -s "$old" "$current_root"
    # A directory alias escaping the installation is never owned by it.
    ln -s "$physical_root/foreign" "$install_root/escape"
    rm "$bin_home/lean-beam"
    ln -s "$install_root/escape/lean-beam" "$bin_home/lean-beam"
    if (verify_publish_targets) >"$tmp_root/error" 2>&1; then
      echo 'accepted a foreign link target through an installation directory alias' >&2
      exit 1
    fi
    assert_contains_literal "$tmp_root/error" 'outside'
    rm "$bin_home/lean-beam"
    ;;
esac

case "${1:-all}" in
  missing-root|all)
    # After the install root is deleted, its launchers still point into it. They remain
    # Beam-owned and replaceable; a launcher into some other missing directory does not.
    (
      set_install_root "$physical_root/alias/deleted-install"
      rm -f "$bin_home/lean-beam"
      ln -s "$install_root/current/bin/lean-beam" "$bin_home/lean-beam"
      verify_publish_targets
      rm "$bin_home/lean-beam"
      ln -s "$physical_root/alias/elsewhere/current/bin/lean-beam" "$bin_home/lean-beam"
      if (verify_publish_targets) >"$tmp_root/error" 2>&1; then
        echo 'accepted a launcher outside a deleted install root' >&2
        exit 1
      fi
      assert_contains_literal "$tmp_root/error" 'outside'
      rm "$bin_home/lean-beam"
    )
    ;;
esac

case "${1:-all}" in
  failure|all)
    [ -L "$current_root" ] || ln -s "$old" "$current_root"
    # Observe the actual rename boundary, then inject a failed rename. It must
    # still name the old generation and failure must not remove that generation.
    (
      mv() {
        [ -L "$current_root" ] && [ "$(readlink "$current_root")" = "$old" ] || {
          echo 'old link disappeared before the rename' >&2
          return 74
        }
        return 73
      }
      replace_symlink_atomically "$new" "$current_root" "$install_root" 'current link'
    ) >"$tmp_root/failure" 2>&1 && {
      echo 'publication ignored a rename failure' >&2
      exit 1
    }
    if [ ! -L "$current_root" ] || [ "$(readlink "$current_root")" != "$old" ]; then
      cat "$tmp_root/failure" >&2
      exit 1
    fi
    [ -z "$(find "$install_root" -name '.link-swap-*' -print)" ]
    ;;
esac

case "${1:-all}" in
  success|all)
    [ -L "$current_root" ] || ln -s "$old" "$current_root"
    replace_symlink_atomically "$new" "$current_root" "$install_root" 'current link'
    [ "$(readlink "$current_root")" = "$new" ]
    assert_file "$current_root/bin/lean-beam"
    assert_not_exists "$old/link"
    replace_symlink_atomically "$current_root/bin/lean-beam" "$bin_home/lean-beam" "$bin_home" 'wrapper link'
    replace_symlink_atomically "$current_root/bin/lean-beam" "$bin_home/lean-beam" "$bin_home" 'wrapper link'
    [ "$(cat "$bin_home/lean-beam")" = new ]
    [ -z "$(find "$install_root" "$bin_home" -name '.link-swap-*' -print)" ]
    ;;
esac

echo 'installer publication checks passed'
