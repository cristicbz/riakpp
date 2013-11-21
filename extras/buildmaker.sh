#!/bin/bash

cleanup() {
  echo "${PROGNAME}: removing any existing build files..."
  rm -rf build build.sh
}

dotarget() {
  TARGET=$1
  echo -e "${PROGNAME}: generating build/${TARGET} target..."
  mkdir -p build/${TARGET}
  if [ ! $? -eq 0 ]; then echo "${PROGNAME}: cannot mkdir" >&2; exit 1; fi
  pushd build/${TARGET} > /dev/null
  CXX=${CXX} cmake -G"${GENERATOR}" ${CMAKE_COMMON} -DCMAKE_BUILD_TYPE=${TARGET} ../.. > ../${TARGET}.cmake.log
  if [ ! $? -eq 0 ]; then
    echo "${PROGNAME}: cmake failed, build/${TARGET}.cmake.log:" >&2
    cat ../${TARGET}.cmake.log >&2
    popd > /dev/null
    exit 1
  fi
  popd > /dev/null
}

PROGNAME=$(basename "$0")

if [[ "$1" == "distclean" ]]; then
  cleanup
  exit 0
elif [[ "$1" == "help" || "$1" == "-h" || "$1" == "--help" || ! -z "$1" ]]; then
  echo "${PROGNAME}: usage (in package folder: "
  echo "    ${PROGNAME} [distclean]"
  echo
  echo "  Runs cmake to generate build dirs and script for each of the targets defined in the CMakeLists.txt"
  echo "of the current directory. To use it, do 'cd <some-package>; ../buildmaker.sh'."
  echo
  echo "  It tries all combinations of ninja, make, g++ and clang. To clean up everything run"
  echo "'../buildmaker.sh distclean', but take into account that this is not necessary even if you change "
  echo "CMakeLists.txt, since the generated build.sh will automatically rereun cmake if that happens."
  exit 1
fi

if [[ ! -f "CMakeLists.txt" ]]; then
  echo "${PROGNAME}: error: no CMakeLists.txt file present in current directory" >&2
  exit 1
fi

if [[ -e "build" ]]; then
  cleanup
fi

if hash ninja-build 2> /dev/null; then
  echo "${PROGNAME}: found build system: ninja"
  GENERATOR="Ninja"
  GENERATOR_CMD="ninja-build"
elif hash ninja 2> /dev/null; then
  echo "${PROGNAME}: found build system: ninja"
  GENERATOR="Ninja"
  GENERATOR_CMD="ninja"
elif hash make 2> /dev/null; then
  echo "found build system: make..."
  GENERATOR="Unix Makefiles"
  GENERATOR_CMD="make"
else
  echo "${PROGNAME}: no supported build system found (tried make, ninja, ninja-build)" >&2
  exit 1
fi

if hash clang 2> /dev/null; then
  echo "${PROGNAME}: found compiler: clang"
  CXX="clang++"
elif hash g++ 2> /dev/null; then
  echo "${PROGNAME}: found compiler: g++"
  CXX="g++"
else
  echo "${PROGNAME}: no supported compiler found (tried g++, clang)" >&2
  exit 1
fi

CMAKE_COMMON="-DCMAKE_EXPORT_COMPILE_COMMANDS=ON"
dotarget debug
dotarget release

echo "${PROGNAME}: generating build.sh script..."
echo '#!/bin/bash' > build.sh
echo -e 'BDIR=$(dirname $0)/build' >> build.sh
echo -e 'if [[ ! -d "$BDIR" ]]; then echo "build.sh: no build directory, run buildmaker.sh again" >&2; exit 1; fi' >> build.sh
echo -e 'TARGETS=$(ls -l $BDIR | sed -n "s/^d.*[0-9][0-9]:[0-9][0-9] \(.*\)$/\1/p" | tr "\\n" " " | sed "s/.$//")' >> build.sh
echo -e 'if [[ "$1" == "-h" || "$1" == "--help" ]]; then' >> build.sh
echo -e '  echo "build.sh: usage: build.sh [target] [build system args]"' >> build.sh
echo -e '  echo "            available targets: ${TARGETS}"\n  exit 1\nfi' >> build.sh
echo -e 'if [[ -z "$1" ]]; then echo "build.sh: using default target \"debug\" (available targets: ${TARGETS})." >&2; TARGET="debug"; else TARGET="$1"; fi' >> build.sh
echo -e 'BDIR="$BDIR/$TARGET"' >> build.sh
echo -e 'if [[ ! -d "$BDIR" ]]; then\n  echo "build.sh: specified target does not exist, available targets: ${TARGETS}" >&2' >> build.sh
echo -e '  exit 1' >> build.sh
echo -e 'fi' >> build.sh
echo -e 'pushd $BDIR > /dev/null\nif [[ ! -z $1 ]]; then shift; fi' >> build.sh
echo ${GENERATOR_CMD}' $@ && cp compile_commands.json ../' >> build.sh
echo -e 'if [ ! $? -eq 0 ]; then echo "build.sh: build failed" >&2; exit 1; fi' >> build.sh
echo 'popd > /dev/null' >> build.sh
chmod +x build.sh

echo "${PROGNAME}: done, to build/test the project, run:"
echo "  ./build.sh <debug|release> [make/ninja additional args]"
