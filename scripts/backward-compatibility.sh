#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
release_tag="$(git describe --tags --abbrev=0 HEAD)"
work_root="$(mktemp -d)"
release_root="${work_root}/release"
store_root="${work_root}/fixture"
compat_java_opts="${JAVA_OPTS:-} --add-modules=jdk.incubator.vector --enable-native-access=ALL-UNNAMED"

cleanup() {
  rm -rf "${work_root}"
}
trap cleanup EXIT

echo "Writing compatibility fixture with Stratum ${release_tag}"
git clone --quiet --depth 1 --branch "${release_tag}" \
  https://github.com/replikativ/stratum.git "${release_root}"

clojure -T:build compile-java
(
  cd "${release_root}"
  clojure -T:build compile-java
)

fixture_path="${repo_root}/backward-compatibility-test/src"
old_deps="{:deps {org.replikativ/stratum {:local/root \"${release_root}\"}} :paths [\"${fixture_path}\"]}"
current_deps="{:deps {org.replikativ/stratum {:local/root \"${repo_root}\"}} :paths [\"${fixture_path}\"]}"

BACK_COMPAT_ROOT="${store_root}" JAVA_OPTS="${compat_java_opts}" clojure -Sdeps "${old_deps}" -X backward-test/write
echo "Reading, mutating, and reopening ${release_tag} fixture with $(git rev-parse --short HEAD)"
BACK_COMPAT_ROOT="${store_root}" JAVA_OPTS="${compat_java_opts}" clojure -Sdeps "${current_deps}" -X backward-test/verify
