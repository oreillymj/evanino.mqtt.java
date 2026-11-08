#!/usr/bin/env bash
# Runs the Paho vs Evanino MQTT benchmarks; see benchmark/README.md.
#
#   benchmark/run.sh                 # every scenario, both clients
#   benchmark/run.sh latency soak    # only these scenarios
#   QUICK=1 benchmark/run.sh         # small counts, to check the harness works
#   NETTY_VERSION=4.1.137.Final benchmark/run.sh
#                                    # against another Netty version (default 4.2.20.Final)
#   JAVA_OPTS="-Dbench.flushConsolidation=true -Dbench.sharedGroupSizing=true" benchmark/run.sh
#                                    # extra JVM options, here turning on this client's optional tuning
#
# Needs a JDK (8+), mosquitto, and the jars in ~/.m2/repository (mvn dependency:resolve, or the
# Paho jar from Maven Central). Results are appended to benchmark/results/<timestamp>.txt.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(dirname "$here")"
repo="${M2_REPO:-$HOME/.m2/repository}"
netty_version="${NETTY_VERSION:-4.2.20.Final}"
paho_version=1.2.5

netty_modules=(buffer codec codec-http codec-mqtt codec-socks common handler handler-proxy resolver transport transport-native-unix-common)
# Netty 4.2 split codec-base and codec-compression out of codec
case "$netty_version" in
  4.1.*) ;;
  *) netty_modules+=(codec-base codec-compression) ;;
esac
jars=()
for a in "${netty_modules[@]}"; do
  jars+=("$repo/io/netty/netty-$a/$netty_version/netty-$a-$netty_version.jar")
done
jars+=("$repo/com/google/guava/guava/21.0/guava-21.0.jar")
jars+=("$repo/com/google/android/android/4.1.1.4/android-4.1.1.4.jar")
paho_jar="$repo/org/eclipse/paho/org.eclipse.paho.client.mqttv3/$paho_version/org.eclipse.paho.client.mqttv3-$paho_version.jar"
jars+=("$paho_jar")
for j in "${jars[@]}"; do
  [ -f "$j" ] || { echo "missing $j" >&2; exit 1; }
done

# javac and java on Windows need Windows paths and ';'
case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*) sep=';'; native() { cygpath -w "$1"; } ;;
  *) sep=':'; native() { echo "$1"; } ;;
esac

build="$here/build"
rm -rf "$build"
mkdir -p "$build/client" "$build/bench" "$here/results"
cp=""
for j in "${jars[@]}"; do cp="$cp$sep$(native "$j")"; done
cp="${cp#"$sep"}"

# BuildConfig is generated from its template, as Maven does; MQTT_LOGGING=false builds the client
# without its debug log calls, as -Pno-logging does
gen="$build/generated/io/github/oreillymj/evanino/mqtt"
mkdir -p "$gen"
sed "s/\${mqtt.logging}/${MQTT_LOGGING:-true}/" \
  "$root/src/main/java-templates/io/github/oreillymj/evanino/mqtt/BuildConfig.java" > "$gen/BuildConfig.java"
{ find "$root/src/main/java" -name '*.java'; echo "$gen/BuildConfig.java"; } > "$build/client-sources.txt"
find "$here/src" -name '*.java' > "$build/bench-sources.txt"
if [ "$sep" = ';' ]; then
  sed -i 's#^/\([a-zA-Z]\)/#\1:/#' "$build/client-sources.txt" "$build/bench-sources.txt"
fi
echo "compiling the client and the harness..."
javac -nowarn -encoding UTF-8 -cp "$cp" -d "$(native "$build/client")" @"$(native "$build/client-sources.txt")" 2>&1 | grep -v '^Note:' || true
javac -nowarn -encoding UTF-8 -cp "$cp$sep$(native "$build/client")" -d "$(native "$build/bench")" @"$(native "$build/bench-sources.txt")" 2>&1 | grep -v '^Note:' || true
runcp="$cp$sep$(native "$build/client")$sep$(native "$build/bench")"

if [ -n "${QUICK:-}" ]; then
  conn="clients=100 hold=3 parallel=20"; tp0="qos=0 count=20000 qos0window=65535"; tp1="qos=1 count=10000"; tp2="qos=2 count=5000"
  lat="rate=500 duration=5 warmup=2"; soak="clients=5 duration=30 drop=8 rate=10 grace=15"
else
  conn="clients=1000 hold=10"; tp0="qos=0 count=500000 qos0window=65535"; tp1="qos=1 count=200000"; tp2="qos=2 count=100000"
  lat="rate=2000 duration=30 warmup=5"; soak="clients=20 duration=300 drop=20 rate=20 grace=30"
fi

# scenario name -> scenario and options; the inflight=1000 runs give both clients the same window
declare -A runs=(
  [connections]="connections $conn"
  [throughput-qos0]="throughput $tp0"
  [throughput-qos1]="throughput $tp1"
  [throughput-qos1-inflight1000]="throughput $tp1 inflight=1000"
  [throughput-qos2]="throughput $tp2"
  [latency-qos0]="latency qos=0 $lat"
  [latency-qos1]="latency qos=1 $lat"
  [soak]="soak $soak"
)
order=(connections throughput-qos0 throughput-qos1 throughput-qos1-inflight1000 throughput-qos2 latency-qos0 latency-qos1 soak)

selected=("$@")
out="$here/results/$(date +%Y%m%d-%H%M%S).txt"
echo "results: $out${JAVA_OPTS:+ (JAVA_OPTS=$JAVA_OPTS)}"
for name in "${order[@]}"; do
  if [ ${#selected[@]} -gt 0 ]; then
    match=
    for s in "${selected[@]}"; do [[ "$name" == "$s"* ]] && match=1; done
    [ -n "$match" ] || continue
  fi
  read -r scenario opts <<< "${runs[$name]}"
  for client in paho netty; do
    echo "== $name: $client"
    # shellcheck disable=SC2086
    line=$(java -Xms1g -Xmx2g -Xss512k ${JAVA_OPTS:-} -cp "$runcp" bench.Bench --client "$client" --scenario "$scenario" $opts 2>"$build/$name-$client.err" | grep '^RESULT' || true)
    if [ -z "$line" ]; then
      line="RESULT client=$client scenario=$scenario error=see_$(native "$build/$name-$client.err")"
    fi
    echo "$line" | tee -a "$out"
  done
done
echo "done: $out"
