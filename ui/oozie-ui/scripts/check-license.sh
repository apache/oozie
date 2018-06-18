#!/usr/bin/env bash
set -e

type lcc || "license-compatibility-check not found. Please install devDependencies with 'npm install --only=dev'";

check_license() {
  lcc | grep incompatible | grep -v `cat .known-license | awk '{print "-e " $2}' | paste -sd " " -`
}

[[ "$(check_license | wc -l)" -eq "0" ]] || {
  echo >&2 "Please remove these incompatible licenses or add them to the .known-license file."
  echo >&2 ""
  check_license
  exit -1
}
