#!/usr/bin/env sh
# Builds the Tessera storage engine as a shared library on Linux/macOS.
# Output: native/bin/libtessera.so (Linux) or native/bin/libtessera.dylib (macOS).
set -eu
root="$(cd "$(dirname "$0")" && pwd)"
src="$root/tessera"
mkdir -p "$root/bin"
case "$(uname -s)" in
  Darwin) out="$root/bin/libtessera.dylib"; extra="-dynamiclib -D_DARWIN_C_SOURCE" ;;
  *)      out="$root/bin/libtessera.so";    extra="-shared" ;;
esac
CC="${CC:-cc}"
# shellcheck disable=SC2046
$CC -std=c11 -O2 -fPIC $extra -D_POSIX_C_SOURCE=200809L -D_DEFAULT_SOURCE \
    -I"$src/include" -I"$src/src" $(find "$src/src" -name '*.c') -o "$out"
echo "Built $out"
