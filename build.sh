#!/bin/bash
BDIR=$(dirname $0)/build
if [[ ! -d "$BDIR" ]]; then echo "build.sh: no build directory, run buildmaker.sh again" >&2; exit 1; fi
TARGETS=$(ls -l $BDIR | sed -n "s/^d.*[0-9][0-9]:[0-9][0-9] \(.*\)$/\1/p" | tr "\n" " ")
if [[ -z "$1" ]]; then echo "build.sh: no target specified, available: ${TARGETS}" >&2; exit 1; fi
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
  echo "build.sh: usage: build.sh <target> [build system args]"
  echo "            available targets: ${TARGETS}"
  exit 1
fi
BDIR="$BDIR/$1"
if [[ ! -d "$BDIR" ]]; then
  echo "build.sh: specified target does not exist, available targets: ${TARGETS}" >&2
  exit 1
fi
pushd $BDIR > /dev/null
shift
ninja $@ && cp compile_commands.json ../
if [ ! $? -eq 0 ]; then echo "build.sh: build failed" >&2; exit 1
else echo "build.sh: build successful, testing..."; ninja test; fi 
popd > /dev/null
