# Evanino MQTT

A Java MQTT 3.1.1 client built on [Netty](https://netty.io/), with an API modelled on the
[Eclipse Paho](https://eclipse.dev/paho/) Java client (`org.eclipse.paho.client.mqttv3`). Code written
against Paho can usually be ported by changing imports and a few calls; see
[Differences from Paho](#differences-from-paho).

It provides a blocking client (`MqttClient`) and a non-blocking, token-based client (`MqttAsyncClient`),
and connects over plain TCP, TLS, WebSockets, secure WebSockets, and through SOCKS4/SOCKS5 or HTTP proxies.

## Contents

- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick start](#quick-start)
  - [Synchronous client](#synchronous-client)
  - [Asynchronous client](#asynchronous-client)
- [Broker URLs](#broker-urls)
- [Connection options](#connection-options)
- [Callbacks and threading](#callbacks-and-threading)
- [TLS](#tls)
- [Proxies](#proxies)
- [Automatic reconnect](#automatic-reconnect)
- [Several brokers (high availability)](#several-brokers-high-availability)
- [Manual acknowledgements](#manual-acknowledgements)
- [Differences from Paho](#differences-from-paho)
- [Building](#building): command line, IntelliJ, skipping steps, without logging, GitHub releases
- [Testing](#testing)
- [Project layout](#project-layout)
- [License](#license)

## Features

- MQTT 3.1.1 (3.1 also selectable), QoS 0, 1 and 2 in both directions, retained messages and last will.
- Blocking `MqttClient` and asynchronous `MqttAsyncClient` with Paho-style `IMqttToken`s and
  `IMqttActionListener` callbacks.
- Per-subscription message listeners (`IMqttMessageListener`) as well as a client-wide `MqttCallback`.
- Transports: TCP, TLS, WebSocket and secure WebSocket, with optional custom WebSocket upgrade headers.
- SOCKS4, SOCKS5 and HTTP `CONNECT` proxies.
- Keep-alive pings, retransmission of unacknowledged packets, and optional automatic reconnect.
- QoS 1 and 2 messages published while disconnected are held and sent once the connection is re-established.
- Optional manual acknowledgement of incoming messages.
- User callbacks run on a dedicated callback thread, never on Netty's I/O threads.

## Requirements

- Java 8 or later.
- Maven 3.6 or later to build (IntelliJ IDEA includes it); see [What you need](#what-you-need).
- Runtime dependencies: `io.netty:netty-all` 4.1.x and `com.google.guava:guava`.
- To run the integration tests: a [mosquitto](https://mosquitto.org/) broker installed locally
  (see [Testing](#testing)).

## Installation

Build and install the library into your local Maven repository (see [Building](#building)), or install a JAR from a
[GitHub release](#releases-on-github), then add it to your project:

```xml
<dependency>
    <groupId>io.github.oreillymj</groupId>
    <artifactId>evanino-mqtt-java</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

All public classes are in the package `io.github.oreillymj.evanino.mqtt`.

## Quick start

In this client the broker URL and client ID are part of `MqttConnectOptions`, rather than being passed to the
client constructor as in Paho.

### Synchronous client

`MqttClient` blocks on each operation until the broker acknowledges it, for up to the time set with
`setTimeToWait` (30 seconds by default).

The quickest way to connect is the Paho-style constructor, which uses default options:

```java
MqttClient client = new MqttClient("tcp://localhost:1883", "quick-example");
client.connect();
client.publish("greetings", "hello".getBytes(StandardCharsets.UTF_8), 1, false);
client.disconnect();
```

To set more options, build an `MqttConnectOptions` and pass it to the constructor:

```java
import io.github.oreillymj.evanino.mqtt.*;

public class SyncExample {
    public static void main(String[] args) throws Exception {
        MqttConnectOptions options = new MqttConnectOptions();
        options.setServerURI("tcp://localhost:1883");
        options.setClientId("sync-example");
        options.setCleanSession(true);
        options.setKeepAliveInterval(30);

        MqttClient client = new MqttClient(options);
        client.setTimeToWait(10_000);               // wait up to 10 s per operation

        client.connect(options);                    // blocks until CONNACK

        // Receive messages through a listener attached to the subscription.
        // subscribeWithResponse blocks until SUBACK and returns the granted QoS.
        IMqttToken subscribed = client.subscribeWithResponse(
                new String[]{"sensors/+/temperature"},
                new int[]{1},
                new IMqttMessageListener[]{
                        (topic, message) -> System.out.println(topic + " -> " + message.getPayloadString())
                });
        System.out.println("Granted QoS: " + subscribed.getGrantedQos()[0]);

        // Publish: blocks until the delivery completes for the QoS used.
        client.publish("sensors/kitchen/temperature", "21.5", 1, false);

        MqttMessage message = new MqttMessage("22.0");
        message.setQos(2);
        message.setRetained(true);
        client.publish("sensors/lounge/temperature", message);

        Thread.sleep(1000);                         // give the listener a moment to print

        client.disconnect();                        // sends DISCONNECT and closes the connection
        client.close();                             // releases threads; the client cannot be reused
    }
}
```

Instead of per-subscription listeners you can set a client-wide callback with `client.setCallback(...)` and
subscribe with `client.subscribe(topic, qos)`; see [Callbacks and threading](#callbacks-and-threading).

### Asynchronous client

`MqttAsyncClient` returns immediately with a token. Wait on the token with `waitForCompletion()`, or pass an
`IMqttActionListener` to be notified when the broker acknowledges the action.

```java
import io.github.oreillymj.evanino.mqtt.*;

public class AsyncExample {
    public static void main(String[] args) throws Exception {
        MqttAsyncClient client = new MqttAsyncClient("tcp://localhost:1883", "async-example");

        MqttConnectOptions options = client.getOptions();
        options.setAutomaticReconnect(true);

        // 1. Blocking style: wait on the token.
        client.connect(options).waitForCompletion(10_000);

        // 2. Listener style: be notified when SUBACK arrives.
        client.subscribe("alerts/#", 1, null, new IMqttActionListener() {
            @Override
            public void onSuccess(IMqttToken token) {
                System.out.println("Subscribed, granted QoS " + token.getGrantedQos()[0]);
            }

            @Override
            public void onFailure(IMqttToken token, Throwable exception) {
                System.err.println("Subscribe failed: " + exception);
            }
        }, (topic, message) -> System.out.println(topic + " -> " + message.getPayloadString()));

        // Publish and get notified when the delivery completes (PUBCOMP for QoS 2).
        MqttMessage message = new MqttMessage("disk almost full");
        message.setQos(2);
        IMqttDeliveryToken delivery = client.publish("alerts/server1", message, null, new IMqttActionListener() {
            @Override
            public void onSuccess(IMqttToken token) {
                System.out.println("Delivered");
            }

            @Override
            public void onFailure(IMqttToken token, Throwable exception) {
                System.err.println("Delivery failed: " + exception);
            }
        });
        delivery.waitForCompletion(10_000);

        client.disconnect().waitForCompletion(10_000);
        client.shutdown();                          // releases threads; the client cannot be reused
    }
}
```

## Broker URLs

The broker address is given with `MqttConnectOptions.setServerURI(...)` (or `setHost(...)`). A URL without a
scheme is treated as `mqtt://`, or `mqtts://` / `ws://` / `wss://` depending on the TLS and WebSocket settings.

| Scheme | Transport | Default port |
|---|---|---|
| `mqtt://`, `tcp://` | Plain TCP | 1883 |
| `mqtts://`, `ssl://` | TLS (the two are aliases) | 8883 |
| `ws://`, `http://` | WebSocket (the two are aliases) | 8083 |
| `wss://`, `https://` | Secure WebSocket (the two are aliases) | 8084 |

Notes:

- **The scheme only picks the transport.** Any of them can go through a SOCKS4, SOCKS5 or HTTP proxy, set
  separately; see [Proxies](#proxies).
- **WebSocket paths always end in `/mqtt`**, for `http://` and `https://` too. It is appended if missing:
  `ws://host:8083` connects to `/mqtt`, and `ws://host:8083/broker` connects to `/broker/mqtt`. A path that already
  ends in `/mqtt` is used as it is.
- **Only WebSocket URLs may have a path.** A path on any other scheme is rejected with `IllegalArgumentException`.
- **Host names may contain underscores** (for example Docker Compose service names such as `tcp://my_broker:1883`).
- **The port set last wins.** `setPort(...)` overrides the port in the URL; a later `setServerURI(...)` with a
  port replaces an earlier `setPort(...)`, and one without a port keeps it.

## Connection options

`MqttConnectOptions` holds everything about a connection. The most commonly used settings:

| Setting | Default | Description |
|---|---|---|
| `setServerURI(String)` | — | Broker URL; see [Broker URLs](#broker-urls). |
| `setClientId(String)` | random `netty-mqtt/XXXXXXXX` | Client identifier. If unset, a new random ID is generated each time it is read. |
| `setUserName(String)` / `setPassword(...)` | none | Credentials sent in CONNECT. A `null` (or, for strings, empty) value is ignored. |
| `setCleanSession(boolean)` | `true` | Start a clean session, or resume the broker-side session. With `false`, QoS 1 and 2 publishes in flight when the connection is lost are sent again after reconnecting; with `true`, they fail with `REASON_CODE_CONNECTION_LOST`. |
| `setKeepAliveInterval(int)` | 60 s | Idle time before a PINGREQ is sent; the connection is closed if no PINGRESP follows. `-1` turns keep-alive off (CONNECT carries 0). |
| `setConnectionTimeout(int)` | 5 s | Longest a connect may take, from the TCP connect through any TLS or WebSocket handshake to CONNACK; `-1` for none (the operating system's TCP limit still applies, and keep-alive gives up on a broker that never answers). |
| `setMqttVersion(MqttVersion)` | 3.1.1 | `MQTT_VERSION_3_1` or `MQTT_VERSION_3_1_1`; `null` selects the default. |
| `setWill(...)` / `setLastWill(MqttLastWill)` | none | Last will and testament. |
| `setAutomaticReconnect(boolean)` | `false` | Reconnect after the connection is lost; see [Automatic reconnect](#automatic-reconnect). |
| `setRetryInterval(long)` | 1 s | Wait before the first reconnect attempt; it doubles after each failed attempt. |
| `setMaxReconnectDelay(int)` | 128000 ms | Longest wait between reconnect attempts. |
| `setMaxInflight(int)` | 1000 | Most QoS 1/2 publishes awaiting acknowledgement at once (1 to 65535; Paho's default is 10); a publish beyond it throws `REASON_CODE_MAX_INFLIGHT`. For QoS 2 against Mosquitto, keep it to about 1000: Mosquitto slows down sharply with tens of thousands of QoS 2 messages in progress (about 500 messages a second with 65535, against 14,000 or more with 1000). |
| `setMaxUnwrittenQos0(int)` | 65535 | Most QoS 0 publishes waiting to be written to the socket; a publish beyond it throws `REASON_CODE_MAX_INFLIGHT`, so a publisher faster than the network gets back-pressure rather than an ever-growing queue. Paho does not limit QoS 0. |
| `enableFlushConsolidation(boolean)` | `false` | Combine flushes to the socket with Netty's `FlushConsolidationHandler`: acknowledgements written during a read share one flush, as do publishes queued on the same pass of the event loop. Saves a system call per packet under load. |
| `setMaxRetries(int)` | 2 | Maximum reconnect attempts; `-1` for no limit. |
| `setRetransmissionInterval(int)` | `0` (off) | Seconds before an unacknowledged packet is sent again on the same connection, each later delay 5 s longer. Off by default, as in Paho: unacknowledged publishes are sent again after reconnecting with `cleanSession=false`. |
| `setMaxInflightRetries(int)` | `-1` (no limit) | Retransmissions of an unacknowledged publish before its token fails, when `setRetransmissionInterval` is on. |
| `setMaxBytesInMessage(int)` | 268435455 (MQTT's maximum) | Largest incoming packet the client accepts, over every transport; a larger one closes the connection with `REASON_CODE_INVALID_MESSAGE`. Lower it to bound the memory one message can take. |
| `setManualAcks(boolean)` | `false` | See [Manual acknowledgements](#manual-acknowledgements). |
| `setMessageArrivedErrorsTriggerDisconnect(boolean)` | `false` | Drop the connection when a message callback throws, as Paho does. |
| `setSslContext(...)` | none | TLS context; without one, TLS URLs use the JVM's default context. See [TLS](#tls). |
| `setHttpsHostnameVerificationEnabled(boolean)` | `false` | Check that the broker's certificate names the host connected to, as Paho does (Paho's default is `true`). |
| `setSSLHostnameVerifier(HostnameVerifier)` | none | Custom check of the broker after the TLS handshake, as in Paho; used only while HTTPS hostname verification is off. |
| `setSslEngineConsumer(Consumer<SSLEngine>)` | no-op | Customize each TLS engine (protocols, ciphers); runs after hostname verification is applied. |
| `setProxy(...)` / `setHTTPProxy(...)` | none | See [Proxies](#proxies). |
| `setCustomWebSocketHeaders(Map)`, `setCustomWebSocketHeader(...)` | none | Extra headers for the WebSocket upgrade request: all at once from a map (replacing earlier ones), or one at a time. |
| `setChannelClass(...)` | `NioSocketChannel` | Netty channel class; must match the event loop set with `setEventLoop`. |
| `enablePahoMode(boolean)` | `false` | Switch hostname verification, the connection timeout, the in-flight window, reconnect attempts, message callback errors and the disconnect timeouts to Paho's values; see [PahoMigration.md](PahoMigration.md#paho-mode). |

Options are read when a connection attempt starts. Setters on the client (and `setCallback`) are ignored while a
connection attempt is in progress.

## Callbacks and threading

There are two ways to receive messages:

- **Per-subscription listener:** an `IMqttMessageListener` passed at subscribe time receives only that
  subscription's messages.
- **Client-wide callback:** an `MqttCallback` set with `setCallback(...)` receives messages for subscriptions
  without a listener, and every connection event.

Every event has an `on<Event>` method, so the handlers sort together alphabetically. The shortest overload of
each one is abstract, so it is what your IDE inserts when you implement the interface. Where Paho has the same
event, the abstract handler takes exactly Paho's parameters, so migrating a Paho callback means adding `on` to the
front of each method name. Override a longer overload when you want more detail:

| Event | Abstract handler | Extra overloads (empty defaults) | Paho name (empty default) |
|---|---|---|---|
| Connected | `onConnectComplete(reconnect, serverURI)` | `(reconnect, isCleanSession, serverURI)`, `(reconnect, isCleanSession, serverURI, uuid)` | `connectComplete(reconnect, serverURI)` |
| Connect attempt failed | `onConnectFailed(serverURI, reason)` | `+ uuid` | |
| Connect attempt started / ended | `onConnecting(connecting, serverURI)` | `+ uuid` | |
| Connection lost | `onConnectionLost(cause)` | `(cause, uuid)`, `(cause, serverURI, uuid)` | `connectionLost(cause)` |
| Outgoing message delivered | `onDeliveryComplete(token)` | | `deliveryComplete(token)` |
| Message received | `onMessageArrived(topic, MqttMessage)` | `onMessageArrived(topic, String payload)` (UTF-8) | `messageArrived(topic, message)` |
| Automatic reconnect gave up | `onReconnectFailed(serverURI, reason)` | `+ uuid` | |
| Automatic reconnect succeeded | `onSuccessfulReconnect(isCleanSession, serverURI)` | `+ uuid` | |

`uuid` identifies the client instance that raised the event. When an event occurs the client calls **every**
overload of it: the Paho name first, then the `on<Event>` overloads from shortest to longest. A callback written
for Paho also keeps working unrenamed once the abstract handlers are added (they can be empty), and you can
override whichever overloads you like. If you override more than one overload of an event, each one runs, so when
renaming a Paho method to its `on` form, remove the original rather than keeping both. An exception
thrown by one overload does not stop the others.

Network I/O runs on one Netty event loop group shared by every client in the JVM (twice as many daemon threads,
named `netty-mqtt-*`, as there are processors, or one per processor after `MqttAsyncClient.enableSharedGroupSizing(true)`
is called before the first client is created), so thousands of clients need only a few I/O threads. The group is
shut down when the last client using it is shut down. To use your own group, call `setEventLoop(...)` before
connecting; the client never shuts down a group you give it.

All callbacks and action listeners run on a single dedicated callback thread per client, in order, never on Netty's
I/O threads. Keep them short: a slow callback delays the ones after it. Call
`MqttAsyncClient.preserveMessageOrdering(false)` to use a thread pool instead, at the cost of ordering.

Exceptions thrown from a message callback are logged and the client stays connected. Set
`setMessageArrivedErrorsTriggerDisconnect(true)` for Paho's behaviour, where such an exception closes the
connection and `connectionLost` is called with it as the cause.

## TLS

Use a `mqtts://` (or `ssl://`) or `wss://` (or `https://`) URL. Without `setSslContext(...)` the client uses the
JVM's default context (`SSLContext.getDefault()`), as Paho does when no socket factory is set, so the
`javax.net.ssl.keyStore`, `javax.net.ssl.trustStore` and related system properties apply. To use your own
keystores, give the options an `SSLContext`; a context set on a `tcp://` URL also turns TLS on.

```java
KeyStore trustStore = KeyStore.getInstance("JKS");
try (InputStream in = new FileInputStream("truststore.jks")) {
    trustStore.load(in, "changeit".toCharArray());
}
TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
tmf.init(trustStore);
SSLContext sslContext = SSLContext.getInstance("TLS");
sslContext.init(null, tmf.getTrustManagers(), null);   // pass KeyManagers too for a client certificate

MqttConnectOptions options = new MqttConnectOptions();
options.setServerURI("mqtts://broker.example.com:8883");
options.setSslContext(sslContext);

// Verify that the server certificate matches the host name. Off by default; on by default in Paho.
options.setHttpsHostnameVerificationEnabled(true);
```

If the handshake fails (for example on a hostname mismatch), the connect token fails with an `MqttException` whose
cause chain contains the `SSLHandshakeException`.

To use PEM files from `openssl` (`ca.crt`, `client.crt`, `client.key`) instead of keystores, to set up Mosquitto
with TLS, or for more on hostname verification, see [UsingSSL.md](UsingSSL.md).

## Proxies

```java
// SOCKS5 (pass socks5 = false for SOCKS4, which has no password authentication)
options.setProxy("proxy.example.com", 1080, "user", "secret", true);

// HTTP CONNECT
options.setHTTPProxy("proxy.example.com", 3128, "user", "secret", false);
```

A proxy works with every transport: plain TCP, TLS, WebSockets and secure WebSockets. The URL scheme never
implies one.

If the proxy cannot be reached or refuses the login, the connection attempt fails. The client never falls back
to connecting without the proxy.

## Automatic reconnect

With `setAutomaticReconnect(true)`, the client reconnects after a connection that was established is lost. As in
Paho, it backs off: it waits `setRetryInterval` seconds before the first attempt and doubles the wait after each
failed attempt, up to `setMaxReconnectDelay` milliseconds (128 s by default). It makes at most `setMaxRetries`
attempts; when the limit is reached, `onReconnectFailed` is called. A failure of the very first connection is
reported and not retried (unless there are [several brokers](#several-brokers-high-availability)), and an
explicit `disconnect()` stops reconnection until the next `connect`.

`reconnect()` starts a reconnect immediately, using the existing options.

With several brokers, reconnecting can move to another broker; see the next section.

## Several brokers (high availability)

To connect to whichever of several brokers is available, put one `MqttConnectOptions` per broker in an
`MqttServerList` and pass the list to `connect`. The client tries the first broker and moves on through the list
until one accepts the connection. After a lost connection, automatic reconnect moves through the list in the same
way.

### Adding brokers

There is no separate factory class: each broker's options are built from the first broker's, in one of two
ways. Both copy every setting (client ID, credentials, Will, TLS, keep-alive, reconnect settings, and so on), so
you only change what differs.

1. **`servers.addServer(uri)`**, for brokers that differ only in their address. It adds a copy of the first
   broker's options with the new address, and returns the copy, in case you want to change anything else.
2. **`new MqttConnectOptions(other)`**, the copy constructor, for brokers that need more changes, such as a
   different certificate or user name. Change the copy, then `servers.add(copy)`.

Options built from scratch with `new MqttConnectOptions()` work too; each broker's options are complete options of
their own.

```java
// The first broker: everything the brokers have in common.
MqttConnectOptions a = new MqttConnectOptions();
a.setServerURI("tcp://broker-a.example.com:1883");
a.setClientId("sensor-42");
a.setUserName("user");
a.setPassword("secret".toCharArray());
a.setCleanSession(false);
a.setAutomaticReconnect(true);       // needed for reconnecting after a lost connection, as with one broker

MqttServerList servers = new MqttServerList(a);

// A second broker that differs only in its address.
servers.addServer("tcp://broker-b.example.com:1883");

// A third on another port: addServer returns the copy, so it can be changed further.
MqttConnectOptions c = servers.addServer("tcp://broker-c.example.com");
c.setPort(1885);

// A fourth with different credentials, from the copy constructor.
MqttConnectOptions d = new MqttConnectOptions(a);
d.setServerURI("ssl://broker-d.example.com:8883");
d.setUserName("other-user");
d.setPassword("other-secret".toCharArray());
servers.add(d);
```

Then connect with the list instead of a single set of options:

```java
// Blocking client: returns once a broker has accepted the connection, or throws if none did.
MqttClient client = new MqttClient(a);
client.connect(servers);
System.out.println("Connected to " + client.getServerURI());

// Asynchronous client: the token completes the same way.
MqttAsyncClient async = new MqttAsyncClient(a);
async.connect(servers).waitForCompletion();
// or, with a listener (connect(servers, context, listener) would make Paho's connect(null, null, null) ambiguous):
async.connectToServers(servers, null, new IMqttActionListener() {
    public void onSuccess(IMqttToken token) { System.out.println("Connected to " + async.getServerURI()); }
    public void onFailure(IMqttToken token, Throwable cause) { System.out.println("No broker accepted: " + cause); }
});
```

`getServerURI()` and the callbacks (`onConnectComplete`, `onConnectionLost`, `onConnecting` and so on) report the
broker of the current attempt or connection. The blocking client's wait, `setTimeToWait` (30 s by default), covers
the whole walk through the list, not each broker, so raise it if the list is long.

### Choosing how the client moves between brokers

There are two modes, and exactly one is on; turning one on turns the other off:

- `servers.reconnectUsingNextServer(true)` (the default): after each failure, move to another broker. With
  `servers.setStrategy(MqttServerList.Strategy.ROUND_ROBIN)` (the default) that is the next in the list, wrapping
  round from the end. With `Strategy.RANDOM` it is a random broker that has not failed since the last connection
  (never the one that has just failed), so that clients leaving a failed broker spread over the others instead of
  all moving to the same one. The client gives up when the failures since the last connection exceed
  `servers.setMaxRetries(n)`; by default that is the first broker's `setMaxRetries`, and `-1` means never give up.
- `servers.reconnectUntilRetryLimit(true)`: stay with a broker until it has failed more than its own
  `setMaxRetries` times in a row, then move to the next in the list. The client gives up once every broker has
  used up its retries.

```java
servers.reconnectUsingNextServer(true);
servers.setStrategy(MqttServerList.Strategy.RANDOM);
servers.setMaxRetries(10);           // give up after 10 failures in a row, across all the brokers
```

### What happens when a broker fails

- The first `connect` walks the list too, whether or not automatic reconnect is on. A single broker's first
  connect is never retried; a list's is.
- A lost connection counts as one failure of its broker. In next-server mode the client therefore moves on at
  once; in until-the-retry-limit mode it first retries the same broker.
- The wait before each attempt is that broker's `setRetryInterval`, doubled after each failure of the same broker
  (until-the-retry-limit) or after each pass through the list (next server), up to its `setMaxReconnectDelay`.
- When the client gives up, the first `connect` fails, and after a lost connection `onReconnectFailed` is called,
  as with one broker. `disconnect()` stops the client moving through the list.
- The session belongs to the client, not the broker: with clean session off, messages a broker had not
  acknowledged are sent again to the next one, as Paho does. The brokers must share sessions (a cluster) for the
  broker side of the session, such as subscriptions, to carry over; otherwise subscribe again in
  `onConnectComplete`.
- The client takes a copy of the list when `connect` is called, so changes to the list apply from the next
  `connect`. With one broker in the list, the client behaves exactly as with those options alone.

### Paho's `setServerURIs`

Paho code that uses `options.setServerURIs(String[])` works unchanged. It behaves as a list of copies of those
options, in next-server mode with round robin, except that, as in Paho, the first `connect` tries each address
once, without waiting between them, and fails if none accepts the connection. Use `MqttServerList` for random
choice, a different mode, or different settings per broker.

## Manual acknowledgements

By default incoming QoS 1 and 2 messages are acknowledged automatically when they arrive. With
`setManualAcks(true)`, the client withholds the acknowledgement (PUBACK for QoS 1, PUBREC for QoS 2) until the
application calls `messageArrivedComplete(messageId, qos)`. That way the broker keeps redelivering a message
until the application has actually dealt with it. Messages are delivered one at a time in this mode: the next
message is handed over once the current one has been acknowledged.

## Differences from Paho

The main differences are summarised below. [PahoMigration.md](PahoMigration.md) has the full migration guide: every
method that changes, the features still missing for full Paho compatibility, and the code that is in progress but
not yet tested. `options.enablePahoMode(true)` takes Paho's values for most of the defaults that differ.

| Paho | This client |
|---|---|
| `setSocketFactory(SSLSocketFactory)`, `setSSLProperties(...)` | `setSslContext(...)`; without it the JVM's default context (`javax.net.ssl.*` properties) is used, as in Paho. |
| `setHttpsHostnameVerificationEnabled` defaults to `true` | Same setter, but it defaults to `false` (see [TLS](#tls)). |
| `setServerURIs(...)` (high availability) | Supported, and `MqttServerList` adds random choice, per-broker options and a stay-until-the-retry-limit mode (see [Several brokers](#several-brokers-high-availability)). |
| `DisconnectedBufferOptions` | Not configurable; QoS 1 and 2 messages published while disconnected are always held and sent on reconnect. |
| `new MqttClient(uri, id)` persists in-flight messages to files by default | The same constructors with a third `MqttClientPersistence` argument (`MemoryPersistence`, `MqttDefaultFilePersistence`, or `MqttDbPersistence` for SQLite, on Android through `AndroidSqliteBridge`) store them; without it the state is kept in memory only. |
| Exception in `messageArrived` closes the connection | Logged by default; enable with `setMessageArrivedErrorsTriggerDisconnect(true)`. |
| `removeMessage(token)` | Not supported. |

## Building

This section assumes no experience with Maven. If you only want to use the library, you can also download the JARs
from a [GitHub release](#releases-on-github) instead of building them.

- [What you need](#what-you-need)
- [Getting the code](#getting-the-code)
- [Building from the command line](#building-from-the-command-line)
- [Building in IntelliJ IDEA](#building-in-intellij-idea)
- [Skipping steps](#skipping-steps)
- [Building without the debug logging](#building-without-the-debug-logging)
- [The build number](#the-build-number)
- [Troubleshooting the build](#troubleshooting-the-build)
- [Releases on GitHub](#releases-on-github)
- [Maven Central releases](#maven-central-releases)

### What you need

| Tool | Version | What it is for |
|---|---|---|
| A JDK (Java Development Kit) | 8 or later | Compiling and running Java. A JRE is not enough: Maven needs the compiler. |
| Maven | 3.6 or later | The build tool. It downloads the libraries the project uses and runs every step of the build. IntelliJ IDEA includes it, so you can skip installing it if you build in IntelliJ. |
| Git | any | Getting the code. |
| mosquitto | 2.x | Only to run the tests (see [Testing](#testing)). Builds that skip the tests do not need it. |

**Check what you already have.** Open a terminal (Windows: press `Win`+`R`, type `cmd`, press Enter; Linux: your
terminal application) and run:

```sh
java -version
mvn -version
git --version
```

Each prints a version if the tool is installed. `mvn -version` also prints the Java it uses, on the line starting
`Java version:`.

**Installing on Windows:**

1. **JDK:** download an installer from [Adoptium](https://adoptium.net/) (Temurin, version 8, 17 or 21) and, during
   the installation, turn on **Set JAVA_HOME variable** and **Add to PATH**.
2. **Maven:** download the "Binary zip archive" from <https://maven.apache.org/download.cgi> and unzip it, for example
   to `C:\Tools\apache-maven-3.9.9`. Then add its `bin` folder to the `PATH`: press `Win`, type
   *environment variables*, open **Edit the system environment variables** > **Environment Variables...**, select
   **Path** under *User variables*, click **Edit** > **New**, enter `C:\Tools\apache-maven-3.9.9\bin`, and click OK
   in every window.
3. **Git:** the installer from <https://git-scm.com/download/win>. It also gives you **Git Bash**, a Linux-style
   terminal.
4. **Close and reopen** every terminal, so it sees the new `PATH`, and run the checks above again.

**Installing on Linux (Debian, Ubuntu):**

```sh
sudo apt update
sudo apt install openjdk-17-jdk maven git
```

On Fedora: `sudo dnf install java-17-openjdk-devel maven git`. Any JDK from 8 on builds the library; the classes
are always compiled for Java 8.

### Getting the code

```sh
git clone https://github.com/oreillymj/evanino.mqtt.java.git
cd evanino.mqtt.java
```

Every command below is run in this folder, the one that contains `pom.xml`.

### Building from the command line

A Maven build runs a sequence of *phases*; naming one runs it and every phase before it:

| Phase | What it does | Output |
|---|---|---|
| `compile` | Compiles the library. | `target/classes` |
| `test` | Also compiles and runs the tests. | `target/surefire-reports` |
| `package` | Also builds the JARs. | `target/evanino-mqtt-java.jar`, `target/evanino-mqtt-java-sources.jar` |
| `install` | Also copies the JARs into your local Maven repository, so your other Maven projects can use the library (see [Installation](#installation)). | `~/.m2/repository/io/github/oreillymj/evanino-mqtt-java/` (Windows: `%USERPROFILE%\.m2\repository\...`) |

Put `clean` first to delete `target` and start from scratch; it avoids surprises from old files.

The most useful commands:

| What you want | Command |
|---|---|
| Build the JARs, with the tests | `mvn clean package` |
| Build and install, with the tests | `mvn clean install` |
| Build and install, quickly: no tests, no signing | `mvn clean install -Dgpg.skip=true -DskipTests` |
| The same, without the debug logging | `mvn clean install -Dgpg.skip=true -DskipTests -Pno-logging` |
| Run one test class | `mvn test -Dtest=BasicTest` |
| Run one test method | `mvn test -Dtest=BasicTest#testConnect` |

The commands are the same on Linux, macOS and the Windows **Command Prompt** (`cmd`). Two differences on Windows:

- **PowerShell** (the default terminal in Windows Terminal and in IntelliJ on Windows) splits a `-D` option at a
  dot, so Maven gets the wrong setting. Put such options in quotes; quoting the others does no harm:

  ```powershell
  mvn clean install "-Dgpg.skip=true" -DskipTests
  mvn clean install "-Dmaven.test.skip=true"
  mvn test "-Dtest=BasicTest"
  ```

- **Paths** use `\` in Command Prompt and PowerShell: `target\evanino-mqtt-java.jar`. In Git Bash they use `/` as on
  Linux.

A build ends with `BUILD SUCCESS` or `BUILD FAILURE`. On failure, the first `[ERROR]` lines say why; `mvn -e ...`
adds the stack trace, and `mvn -X ...` everything. The first build downloads Netty, Guava and Maven's plugins, so it
needs internet access and takes longer; later builds use the copies in your local repository (`~/.m2`).

### Building in IntelliJ IDEA

IntelliJ IDEA (the free Community Edition is enough) has Maven built in.

**Open the project once:**

1. **File** > **Open...**, select the project folder (or its `pom.xml`) and click **OK**. If asked, choose
   **Open as Project** and **Trust Project**.
2. Wait for the progress bar at the bottom to finish: IntelliJ reads `pom.xml` and downloads the dependencies.
3. **Set the JDK:** **File** > **Project Structure...** > **Project**, and pick an **SDK** of 8 or later. If none is
   listed, **Add SDK** > **Download JDK...** gets one. Leave **Language level** at 8.
4. **Generate `BuildConfig`:** open the **Maven** tool window (**View** > **Tool Windows** > **Maven**, or the `m`
   icon on the right edge), right-click the project, and choose **Generate Sources and Update Folders**. Until this has
   run (or a Maven build has), IntelliJ marks `BuildConfig` in red, because it is generated from
   `src/main/java-templates` (see [Building without the debug logging](#building-without-the-debug-logging)).

**Build with Maven from the Maven tool window:**

- Expand **Lifecycle** and double-click a phase, for example **install**. To run several, select them with `Ctrl`
  held (`Cmd` on macOS) and click the green **Run** arrow; select **clean** too for a clean build.
- **Skip the tests:** click the **Toggle 'Skip Tests' Mode** button in the tool window's toolbar (it crosses out the
  test phase). Click it again to run them.
- **Build without logging:** expand **Profiles** and tick **no-logging**. Untick it for the normal build. (Leave
  **release** unticked; see [Maven Central releases](#maven-central-releases).)
- **Refresh after editing `pom.xml`:** click **Reload All Maven Projects** (the circular arrows).

**Save a build as a one-click run configuration**, for example a quick install without logging:

1. **Run** > **Edit Configurations...** > **+** > **Maven**.
2. **Name:** `install, no tests, no logging`.
3. **Run** (the command line): `clean install -Dgpg.skip=true -DskipTests -Pno-logging`. The quoting PowerShell
   needs is not needed here.
4. **Working directory:** the project folder (the default).
5. Click **OK**. It now appears in the run configuration list in the toolbar; select it and click **Run**.

Settings for every Maven build in IntelliJ are under **File** > **Settings** (macOS: **IntelliJ IDEA** >
**Settings**) > **Build, Execution, Deployment** > **Build Tools** > **Maven**: the Maven to use (*Bundled* by
default), and under **Runner**, a **Skip tests** box and **Properties** you can add such as `gpg.skip` = `true`.

**Run tests from the editor:** open a test class and click the green arrow next to the class or a method. That runs
it through IntelliJ's own test runner, which is quicker than Maven.

**Use IntelliJ's Maven in a terminal**, if you have not installed Maven: it is in IntelliJ's installation folder.

```bat
REM Windows Command Prompt (adjust the IntelliJ version)
"C:\Program Files\JetBrains\IntelliJ IDEA Community Edition 2025.2.6.3\plugins\maven\lib\maven3\bin\mvn.cmd" clean install -DskipTests
```

```sh
# Linux (the folder IntelliJ was unpacked or installed into; Toolbox installs are under ~/.local/share/JetBrains/Toolbox/apps)
/opt/idea-IC/plugins/maven/lib/maven3/bin/mvn clean install -DskipTests
```

### Skipping steps

Add any of these to a `mvn` command (in IntelliJ, to the run configuration's command line):

| Option | What it skips | When to use it |
|---|---|---|
| `-DskipTests` | Running the tests (they are still compiled). | Most local builds: the tests need mosquitto and take about 15 minutes. |
| `-Dmaven.test.skip=true` | Compiling and running the tests. | When the tests do not compile, or to save the compile time too. |
| `-Dgpg.skip=true` | Signing with GPG. | Always safe; only the `release` profile signs, so it matters only with `-P release`. |
| `-Dmaven.antrun.skip=true` | The Ant step that increments the build number (see [The build number](#the-build-number)). | When you do not want the counter or `MqttAsyncClient.java` changed, for example to build an exact copy of a commit. |
| `-Dmaven.source.skip=true` | The sources JAR. | When you only need the library JAR. |
| `-o` | Downloading anything (offline). | When there is no network and everything was downloaded before. |
| `-q` | Maven's progress output (quiet). | To see only warnings and errors. |

For example, the quickest build that still installs the library, on Linux, macOS and Command Prompt:

```sh
mvn -q clean install -Dgpg.skip=true -DskipTests -Dmaven.antrun.skip=true
```

and in PowerShell:

```powershell
mvn -q clean install "-Dgpg.skip=true" -DskipTests "-Dmaven.antrun.skip=true"
```

To skip the tests in every build on your machine, add a profile to your Maven settings file
(`~/.m2/settings.xml`; Windows `%USERPROFILE%\.m2\settings.xml`; create it if it does not exist):

```xml
<settings>
  <profiles>
    <profile>
      <id>no-tests</id>
      <properties>
        <skipTests>true</skipTests>
      </properties>
    </profile>
  </profiles>
  <activeProfiles>
    <activeProfile>no-tests</activeProfile>
  </activeProfiles>
</settings>
```

### Building without the debug logging

The client's debug log calls, which you turn on at run time with `enableLogging(true)`, can be left out of the build
altogether, for a slightly smaller and faster library (the JAR is about 6 KB smaller). Calls in `catch` blocks stay,
so `enableLogging(true)` still reports caught exceptions.

| | Linux, macOS, Command Prompt | PowerShell | IntelliJ |
|---|---|---|---|
| Without logging | `mvn clean install -Pno-logging` | `mvn clean install -Pno-logging` | Tick **Profiles** > **no-logging** |
| Normal build | `mvn clean install` | `mvn clean install` | Untick it |

`-Dmqtt.logging=false` (in PowerShell, `"-Dmqtt.logging=false"`) does the same as `-Pno-logging`. Combine it with the
options above as you like, for example `mvn clean install -Dgpg.skip=true -DskipTests -Pno-logging`.

How it works: the debug log calls are guarded by the build-time constant `BuildConfig.LOGGING`, generated from
`src/main/java-templates` by the `templating-maven-plugin` into `target/generated-sources/java-templates`. In the
default build it is `true`: while logging is off each call costs a field check and its message is never built. The
`no-logging` profile sets it to `false`, and javac then leaves the calls out of the classes. Always use `clean` when
switching between the two, so no class from the other build is left in `target`. To build with `javac` directly, as
`benchmark/run.sh` does, expand the template yourself (`${mqtt.logging}` becomes `true` or `false`);
`MQTT_LOGGING=false benchmark/run.sh` benchmarks the build without logging.

### The build number

Every build runs an Ant step (`ant/ant_build.xml`) first. It increments the revision number in
`ant/build_info.properties` (a local file, not in git) and writes it into `BUILD_NUMBER` in `MqttAsyncClient.java`,
so expect that file to change after each build. `-Dmaven.antrun.skip=true` skips the step.

To keep that change out of `git status` and diffs, `.gitattributes` applies a `buildnumber` clean filter to
`MqttAsyncClient.java`: git always sees and stores `BUILD_NUMBER = "0"`, while your working copy keeps the real
number. Git filters are configured per clone, so run these once after cloning (in Git Bash on Windows, or a Linux or
macOS terminal):

```sh
git config filter.buildnumber.clean "sed -E 's/(BUILD_NUMBER = \")[0-9]+(\";)/\10\2/'"
git config filter.buildnumber.smudge cat
```

Without it, git shows the build-number bump as a change to `MqttAsyncClient.java`. Other edits to that file are
tracked as usual.

### Troubleshooting the build

| Message | Cause and fix |
|---|---|
| `'mvn' is not recognized as an internal or external command` (Windows), `mvn: command not found` (Linux) | Maven is not on the `PATH`. Install it as above (and reopen the terminal), or use IntelliJ's Maven. |
| `JAVA_HOME environment variable is not defined correctly` or `No compiler is provided in this environment` | Maven cannot find a JDK, or found a JRE. Set `JAVA_HOME` to the JDK folder: on Windows in the environment variables (as for `Path` above, a new variable `JAVA_HOME` = `C:\Program Files\Eclipse Adoptium\jdk-17...`); on Linux `export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64` in `~/.bashrc`. |
| `Unknown lifecycle phase ".skip=true"` | PowerShell split a `-D` option at the dot: quote it, `"-Dgpg.skip=true"`. |
| `cannot find symbol ... BuildConfig` in IntelliJ | Run **Generate Sources and Update Folders** (see above), or any Maven build. |
| `Could not resolve dependencies` or `Could not transfer artifact` | No network, or a proxy is needed. Check your connection; behind a company proxy, configure it in `~/.m2/settings.xml` (`<proxies>`). |
| The tests are reported as skipped | mosquitto was not found, so the broker tests are skipped rather than failed; see [Testing](#testing). |
| `There are test failures` | Look in `target/surefire-reports` for the failing class's `.txt` file. To install anyway, add `-DskipTests`. |

### Releases on GitHub

The workflow [`.github/workflows/release.yml`](.github/workflows/release.yml) builds the library on GitHub and
attaches the files to a GitHub release. It starts when you push a tag that starts with `v`, or publish a release on
GitHub:

```sh
git tag v1.2.3
git push origin v1.2.3
```

or on GitHub: **Releases** > **Draft a new release**, choose or type the tag `v1.2.3`, and **Publish release**. A tag
with a hyphen, such as `v1.2.3-rc1`, makes a pre-release.

The version comes from the tag (`v1.2.3` builds `1.2.3`, whatever `pom.xml` says). The workflow builds with Java 8,
runs the tests against mosquitto (set `RUN_TESTS: "false"` at the top of the file to skip them), and attaches:

| File | Contents |
|---|---|
| `evanino-mqtt-java-1.2.3.jar` | The library. |
| `evanino-mqtt-java-1.2.3-nologging.jar` | The library without the debug log calls. |
| `evanino-mqtt-java-1.2.3-sources.jar`, `evanino-mqtt-java-1.2.3-javadoc.jar` | Sources and API documentation, for IDEs. |
| `evanino-mqtt-java-1.2.3.pom` | The POM, listing the dependencies (Netty, Guava). |
| `evanino-mqtt-java-1.2.3-src.tar.gz` | The whole repository at the tag. |
| `SHA256SUMS.txt` | Checksums of the files. |

To use a downloaded JAR from another Maven project, install it into your local repository together with its POM, so
that Maven also fetches Netty and Guava (in PowerShell, quote each `-D` option):

```sh
mvn install:install-file -Dfile=evanino-mqtt-java-1.2.3.jar -DpomFile=evanino-mqtt-java-1.2.3.pom -Dsources=evanino-mqtt-java-1.2.3-sources.jar -Djavadoc=evanino-mqtt-java-1.2.3-javadoc.jar
```

and use version `1.2.3` in the dependency shown in [Installation](#installation). Without Maven, put the JAR on the
classpath with `netty-all` 4.1.x and `guava`.

### Maven Central releases

The `release` profile names the artifacts from the version numbers in `ant/build_info.properties`, signs them with GPG
and deploys them to Sonatype. It needs GPG and Sonatype credentials configured in `~/.m2/settings.xml`:

```sh
mvn clean deploy -P release
```

## Testing

The test suite in `src/test/java` ports the
[Eclipse Paho v3 test suite](https://github.com/eclipse-paho/paho.mqtt.java/tree/master/org.eclipse.paho.client.mqttv3.test),
keeping Paho's class and method names. Cases for Paho features this client does not have are marked `@Ignore`
with the reason, and differences from Paho are described in each test class's Javadoc.

Most tests run against a real broker. The `MosquittoBroker` JUnit rule starts a private mosquitto instance for
each test class on free local ports, and stops it afterwards. That instance has:

- a TCP listener;
- a WebSocket listener;
- a TLS listener that requires a client certificate;
- a secure WebSocket listener.

The TLS listeners use the test-only certificates in `src/test/resources/tls` (CA, a server certificate for
`localhost`, and a client certificate). The keystores use the password `password`.

mosquitto is looked up in `C:\Program Files\mosquitto`, `/usr/sbin`, `/usr/local/sbin`, `/opt/homebrew/sbin` and
on the `PATH`. If it cannot be found, the broker tests are skipped rather than failed.

Install mosquitto:

- **Windows:** the installer from <https://mosquitto.org/download/>.
- **Debian / Ubuntu:** `sudo apt install mosquitto` (you can stop the system service; the tests start their own).
  If an AppArmor profile confines `/usr/sbin/mosquitto`, as on GitHub's Ubuntu runners, mosquitto exits with
  "Unable to open config file". Copy the binary elsewhere and point the tests at the copy:
  `cp /usr/sbin/mosquitto /tmp/mosquitto && mvn test -Dmosquitto.path=/tmp/mosquitto`.
- **macOS:** `brew install mosquitto`.

System properties:

| Property | Purpose |
|---|---|
| `mosquitto.path` | Full path of the mosquitto executable. |
| `mqtt.test.serverURI` | Use an existing broker instead of starting mosquitto, e.g. `tcp://localhost:1883`. |
| `mqtt.test.webSocketServerURI` | WebSocket URL of that broker (WebSocket tests are skipped without it). |
| `mqtt.test.sslServerURI` | TLS URL of that broker (TLS tests are skipped without it). |
| `mqtt.test.secureWebSocketServerURI` | Secure WebSocket URL of that broker. |

For example:

```bash
mvn test -Dmosquitto.path=/opt/mosquitto/sbin/mosquitto
mvn test -Dmqtt.test.serverURI=tcp://localhost:1883
```

## Project layout

```
src/main/java/io/github/oreillymj/evanino/mqtt/
    MqttClient, IMqttClient            blocking client
    MqttAsyncClient, IMqttAsyncClient  asynchronous client
    MqttConnectOptions                 connection settings and broker URL parsing
    MqttCallback, IMqttMessageListener message and connection callbacks
    MqttToken, MqttDeliveryToken       tokens returned by asynchronous operations
    MqttChannelHandler                 MQTT protocol handling on the Netty pipeline
    MqttPingHandler                    keep-alive
    *ProxyHandlerFactory               SOCKS4, SOCKS5 and HTTP proxy support
    MqttWebSocketCodec, HttpResponseHandler  WebSocket transport
src/test/java/...                      JUnit tests ported from Paho, plus test fixtures
src/test/resources/tls/                test-only TLS certificates
src/main/java-templates/               BuildConfig template (logging on or off)
ant/                                   build-number Ant script
examples/                              runnable examples (TLS with PEM files) and their Maven build
benchmark/                             benchmark harness against Paho
paho-issues/                           tests for the review of Paho's open issues (PahoOpenIssues.md)
.github/workflows/release.yml          builds the JARs and attaches them to a GitHub release
scratch/                               old and experimental code, not part of the build
```

## License

The project is licensed under the [Apache License, Version 2.0](LICENSE).

It includes code from other projects, listed in [NOTICE](NOTICE):

- **Eclipse Paho.** Some classes (for example `MqttException`, `MqttMessage` and `MqttTopic`) and most of the tests
  are taken or ported from the [Eclipse Paho Java client](https://github.com/eclipse-paho/paho.mqtt.java). Paho is
  offered under the Eclipse Public License 2.0 or the Eclipse Distribution License 1.0, and this project uses it
  under the [Eclipse Distribution License 1.0](licenses/EDL-1.0.txt), a BSD-style licence. Those files keep their
  original copyright notices, and say what has been changed.
- **netty-mqtt-client.** The project started as a fork of
  [jetlinks/netty-mqtt-client](https://github.com/jetlinks/netty-mqtt-client) (Apache License 2.0).

The JARs carry `LICENSE`, `NOTICE` and the EDL text in `META-INF/`. The API is modelled on Paho's so that Paho code
can be ported, but this project is not part of, or endorsed by, the Eclipse Foundation or the Paho project.
