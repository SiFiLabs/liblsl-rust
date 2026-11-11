#!/bin/sh
# Regenerate src/generated.rs from the liblsl C headers.
# Requires bindgen-cli (`cargo install bindgen-cli`) and libclang.
set -eu
cd "$(dirname "$0")"
{
	# keep the liblsl license notice at the top of the generated file
	printf '/* '; cat liblsl/LICENSE; printf ' */\n'
	bindgen liblsl/include/lsl_c.h \
		--allowlist-function 'lsl_.*' \
		--allowlist-type 'lsl_.*' \
		--allowlist-var 'lsl_.*|LIBLSL_.*' \
		--no-layout-tests \
		-- -Iliblsl/include
} > src/generated.rs.tmp
mv src/generated.rs.tmp src/generated.rs
