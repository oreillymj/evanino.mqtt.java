#!/bin/bash
# Compiles the client, its tests and the Paho issue tests in paho-issues/tests, then runs the given
# issue test classes (simple names), e.g.  paho-issues/run.sh PahoIssue1090ExceptionMessageTest
# Needs a JDK 8+, the project's dependencies in ~/.m2/repository (a Maven build fetches them) and,
# for the tests that need a broker, mosquitto (see MosquittoBroker).
set -e
here="$(cd "$(dirname "$0")" && pwd)"
root="$(dirname "$here")"
cd "$root"
R="${M2_REPO:-$HOME/.m2/repository}"
JARS="$(ls $R/io/netty/netty-{buffer,codec,codec-http,codec-mqtt,codec-socks,common,handler,handler-proxy,resolver,transport,transport-native-unix-common}/4.1.137.Final/*.Final.jar) $R/com/google/guava/guava/21.0/guava-21.0.jar $R/com/google/android/android/4.1.1.4/android-4.1.1.4.jar $R/junit/junit/4.13.1/junit-4.13.1.jar $R/org/hamcrest/hamcrest-core/1.3/hamcrest-core-1.3.jar $R/org/xerial/sqlite-jdbc/3.47.1.0/sqlite-jdbc-3.47.1.0.jar"
case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*) sep=';'; native() { cygpath -w "$1"; }; mixed() { cygpath -m "$1"; } ;;
  *) sep=':'; native() { echo "$1"; }; mixed() { echo "$1"; } ;;
esac
CP=""; for j in $JARS; do CP="$CP$sep$(native "$j")"; done; CP="${CP#"$sep"}"
OUT="$here/build"; rm -rf "$OUT"; mkdir -p "$OUT/main" "$OUT/test" "$OUT/gen"
sed "s/\${mqtt.logging}/true/" src/main/java-templates/io/github/oreillymj/evanino/mqtt/BuildConfig.java > "$OUT/gen/BuildConfig.java"
{ find src/main/java -name '*.java'; mixed "$OUT/gen/BuildConfig.java"; } > "$OUT/main.txt"
{ find src/test/java "paho-issues/tests" -name '*.java'; } > "$OUT/test.txt"
javac -nowarn -encoding UTF-8 -cp "$CP" -d "$(native "$OUT/main")" @"$(native "$OUT/main.txt")" 2>&1 | grep -v "^Note:" || true
javac -nowarn -encoding UTF-8 -cp "$CP$sep$(native "$OUT/main")" -d "$(native "$OUT/test")" @"$(native "$OUT/test.txt")" 2>&1 | grep -v "^Note:" || true
cp -r src/test/resources/* "$OUT/test/"
java -cp "$CP$sep$(native "$OUT/main")$sep$(native "$OUT/test")" org.junit.runner.JUnitCore \
  $(for c in "$@"; do echo io.github.oreillymj.evanino.mqtt.$c; done)
