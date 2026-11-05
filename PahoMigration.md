# Migrating from Eclipse Paho

This guide is for moving code written against the Eclipse Paho Java client (`org.eclipse.paho.client.mqttv3`,
1.2.x) to this Netty-based client. It lists:

1. [Behaviour that changes silently](#1-behaviour-that-changes-silently): things that compile but act differently.
2. [Code changes, class by class](#2-code-changes-class-by-class): methods that are renamed, have different
   signatures, or are missing.
3. [Paho compatibility status](#3-paho-compatibility-status): every Paho feature not fully supported, and whether
   it is worth implementing for a Netty client.
4. [In progress, not tested](#4-in-progress-not-tested): code that exists here but has no automated tests or is
   not finished.

Most Paho code ports with an import change, a callback rename, and a few option changes. The status labels used
throughout are:

| Label | Meaning |
|---|---|
| **Same** | Same name and signature as Paho; no change needed. |
| **Changed** | Exists under a different name or signature; the replacement is given. |
| **Stub** | The method exists but does nothing, or returns a fixed value. |
| **Missing** | No equivalent yet. |
| **N/A** | Paho-specific plumbing that a Netty client does not need. |

## Quick checklist

1. Replace `import org.eclipse.paho.client.mqttv3.*` (and `...mqttv3.persist.*`) with
   `import io.github.oreillymj.evanino.mqtt.*`.
2. Replace `implements MqttCallback` / `implements MqttCallbackExtended` with
   `implements io.github.oreillymj.evanino.mqtt.MqttCallback`, add `on` to the front of your callback
   method names, and let your IDE add the remaining abstract handlers (see [Callbacks](#callbacks)).
3. Drop the `persistence` argument from client constructors; `(serverURI, clientId)` works as in Paho
   (see [Creating a client](#creating-a-client)).
4. Call `setHttpsHostnameVerificationEnabled(true)` if you relied on Paho's default TLS hostname verification
   (see [TLS](#tls)).
5. Review the [automatic reconnect](#automatic-reconnect) settings: the defaults are very different.
6. Or call `options.enablePahoMode(true)` to take Paho's values for most of the settings in
   [section 1](#1-behaviour-that-changes-silently) at once (see [Paho mode](#paho-mode)).

## 1. Behaviour that changes silently

These compile without changes but behave differently from Paho. `enablePahoMode(true)` switches hostname
verification, the connection timeout, the reconnect limit, message callback errors, the disconnect timeouts and the
in-flight window to Paho's behaviour; see [Paho mode](#paho-mode).

| Area | Paho | This client | What to do |
|---|---|---|---|
| TLS hostname verification | On by default since Paho 1.2.1 (`setHttpsHostnameVerificationEnabled(true)`). | **Off** unless you enable it. | Call `setHttpsHostnameVerificationEnabled(true)`; see [TLS](#tls). |
| Automatic reconnect | Retries forever, waiting 1 s and doubling the wait up to `setMaxReconnectDelay` (default 128 s). | Backs off the same way, starting at `setRetryInterval` (default 1 s), but makes at most `setMaxRetries` attempts (default 2), then calls `onReconnectFailed` and stops. | Set `setMaxRetries(-1)` for unlimited retries. |
| Connection timeout default | 30 s. | 5 s. | Call `setConnectionTimeout(30)` to match Paho. |
| `setConnectionTimeout(0)` | Waits indefinitely (until the operating system gives up). | Throws `IllegalArgumentException`, except in [Paho mode](#paho-mode). | Use `-1`: no timeout of the client's own, as Paho's `0`. |
| `setKeepAliveInterval(0)` | Disables keep-alive: CONNECT carries 0, so the broker never drops the client for being idle, and no PINGREQ is sent. | Throws `IllegalArgumentException`, except in [Paho mode](#paho-mode). | Use `-1`, which does the same. |
| MQTT 3.1 client ID longer than 23 characters | Sent as it is; the broker refuses the connection (identifier rejected). | Truncated to its first 23 characters, the most MQTT 3.1 allows. | Keep 3.1 client IDs unique within their first 23 characters. |
| Client ID | Must be passed to the constructor. | A random `netty-mqtt/XXXXXXXX` ID is generated if you do not set one. | Call `setClientId(...)` when you need a stable ID (required for `cleanSession=false`). |
| Exception thrown from `messageArrived` | Closes the connection; `connectionLost` is called. | Logged; the client stays connected. | Call `setMessageArrivedErrorsTriggerDisconnect(true)` for Paho's behaviour. |
| Publishing while disconnected | Fails unless `DisconnectedBufferOptions` enables buffering. | QoS 1 and 2 publishes are always held, without limit, and sent on reconnect. QoS 0 publishes fail with `REASON_CODE_CLIENT_NOT_CONNECTED`. | No code change; be aware that the buffer is unbounded. |
| Connection lost with QoS 1/2 publishes awaiting acknowledgement | Keeps them; with `cleanSession=false` they are resent after reconnecting and their tokens complete then. | With `cleanSession=false`, the same: after the next CONNACK, whether from automatic reconnect or a later `connect()`, each is sent again in the order it was made, with its packet identifier, as a PUBLISH with DUP set or, if PUBREC had arrived, as a PUBREL. Incoming QoS 2 messages awaiting PUBREL are kept too. With a clean session their tokens fail with `REASON_CODE_CONNECTION_LOST` (`REASON_CODE_CLIENT_DISCONNECTING` after a disconnect). Either way, publishes still pending when the client is closed fail with `REASON_CODE_CLIENT_CLOSED`; with an `MqttClientPersistence` they stay stored for the next client (see [Persistence](#persistence)). | None with `cleanSession=false`. With a clean session, republish from `onFailure`; the broker may already have the message, so expect a duplicate at QoS 1. |
| `disconnect()` with no timeout | Quiesces for up to 30 s: waits for in-flight messages before sending DISCONNECT. | Sends DISCONNECT at once. `disconnect(quiesceTimeout)` quiesces as in Paho. | Call `disconnect(30000)` to keep Paho's wait. |
| `disconnectForcibly()` with no timeouts | Quiesces for up to 30 s, then sends DISCONNECT (up to 10 s), then closes. | Closes at once without DISCONNECT. The overloads with timeouts behave as in Paho, except that `disconnectForcibly(disconnectTimeout)` does not quiesce. | Call `disconnectForcibly(30000, 10000)` to keep Paho's behaviour. |
| In-flight window | 10 QoS 1/2 publishes by default; one more throws `REASON_CODE_MAX_INFLIGHT`. | 65535 by default, the number of packet identifiers MQTT allows; `setMaxInflight` accepts 1 to 65535. Publishes held while disconnected count too. QoS 0 publishes not yet written to the socket have a separate window of the same size. As in Paho, a QoS 0 token completes once the PUBLISH is written. | Call `setMaxInflight(10)` to keep Paho's limit. |
| Persistence | `new MqttClient(uri, id)` uses file persistence (`MqttDefaultFilePersistence`), so in-flight QoS 1 and 2 messages survive a restart. | Without a persistence argument, in-flight state is kept in memory only and is lost when the process exits. | Pass `new MqttDefaultFilePersistence(dir)` as the third constructor argument; see [Persistence](#persistence). |
| Callback overloads | One method per event. | Every overload of an event is called, Paho name first. If you keep `connectionLost` **and** add `onConnectionLost`, both run. | When renaming a method to its `on` form, delete the original. |

## 2. Code changes, class by class

### Broker URLs

| Paho | This client | Status |
|---|---|---|
| `tcp://host:1883` | `tcp://` or `mqtt://` | Same |
| `ssl://host:8883` | `ssl://` or `mqtts://` (aliases) | Same |
| `ws://host:port/path` | `ws://host:port/path`; the path is normalised to end in `/mqtt`, default port 8083 | Same |
| `wss://host:port/path` | `wss://host:port/path`; the path is normalised to end in `/mqtt`, default port 8084 | Same |
| Not available | `http://` and `https://`: aliases of `ws://` and `wss://`, with the same path handling and default ports | Extra |
| `local://` | Not supported | N/A |
| Not available | Any scheme through a SOCKS4, SOCKS5 or HTTP proxy, set with `setProxy` or `setHTTPProxy` | Extra |

### Creating a client

| Paho | This client | Status |
|---|---|---|
| `new MqttAsyncClient(serverURI, clientId)` | `new MqttAsyncClient(serverURI, clientId)` | Same |
| `new MqttAsyncClient(serverURI, clientId, persistence)` | `new MqttAsyncClient(serverURI, clientId)`; persistence is not supported | **Missing** |
| `new MqttAsyncClient(serverURI, clientId, persistence, pingSender, executorService)` | `new MqttAsyncClient(options)`; pass an `EventLoopGroup` with `setEventLoop(...)` | N/A |
| `new MqttClient(serverURI, clientId)` | Same; the URI is checked straight away, as in Paho | Same |
| `new MqttClient(serverURI, clientId, persistence)` | `new MqttClient(serverURI, clientId)`; persistence is not supported | **Missing** |
| `MqttClient.generateClientId()`, `MqttAsyncClient.generateClientId()` (static) | Same; the form is `netty-mqtt/XXXXXXXX` rather than `paho` and a number | Same |

### `MqttConnectOptions`

| Paho | This client | Status |
|---|---|---|
| `setUserName`, `setPassword(char[])` | Same; `setPassword(String)` also exists | Same |
| `setCleanSession`, `setKeepAliveInterval`, `setConnectionTimeout` | Same, but `0` is rejected and the default timeout is 5 s; see [section 1](#1-behaviour-that-changes-silently) | Same |
| `setMqttVersion(MQTT_VERSION_3_1_1)`, `setMqttVersion(int)` | Same. The constants have the same names but are Netty's `MqttVersion` enum, and `setMqttVersion(int)` accepts Paho's values 0, 3 and 4. Only code that stores a constant in an `int`, or reads `getMqttVersion()` as an `int`, needs changing | Same |
| `MQTT_VERSION_DEFAULT` (tries 3.1.1, falls back to 3.1) | `MQTT_VERSION_DEFAULT` is 3.1.1 with no fallback | **Changed** |
| `setWill(MqttTopic, byte[], int, boolean)`, `setWill(String, byte[], int, boolean)` | Same; `setLastWill(MqttLastWill)` also exists | Same |
| `getWillDestination()`, `getWillMessage()` | Same; `getLastWill()` also returns the will | Same |
| `setServerURIs(String[])`, `getServerURIs()` | Same: each address is tried in turn on connect and reconnect. `MqttServerList` gives more control (random choice, options per broker); see the README's "Several brokers" | Same |
| `setAutomaticReconnect(boolean)` | Same; see [Automatic reconnect](#automatic-reconnect) | Same |
| `setMaxReconnectDelay(int)` | Same | Same |
| `setMaxInflight(int)` | Same, but the default is 1000 rather than 10, it covers QoS 1 and 2 only, and values above 65535 are rejected; QoS 0 has its own window, `setMaxUnwrittenQos0(int)` (default 65535) | Same |
| `setSocketFactory(SSLSocketFactory)` | `setSslContext(SSLContext)` or `setSslContext(SslContext)` | **Changed** |
| No socket factory with `ssl://`: the JVM's default TLS settings (`javax.net.ssl.*` system properties) | Same: a TLS URL without `setSslContext` uses `SSLContext.getDefault()` | Same |
| `setSocketFactory(SocketFactory)` for plain sockets | `setChannelClass(...)`, `setBindAddress(...)` | N/A |
| `setSSLProperties(Properties)` (`com.ibm.ssl.*` keys) | Build an `SSLContext` yourself and call `setSslContext(...)`; see [`setSSLProperties`](#setsslpropertiesproperties-and-the-comibmssl-keys) | **Missing** |
| `setHttpsHostnameVerificationEnabled(boolean)`, `isHttpsHostnameVerificationEnabled()` | Same, but off by default; see [TLS](#tls) | Same |
| `setSSLHostnameVerifier(HostnameVerifier)`, `getSSLHostnameVerifier()` | Same: called after the TLS handshake, and only while HTTPS hostname verification is off | Same |
| `setCustomWebSocketHeaders(Map)` | Same; `setCustomWebSocketHeader(name, value)` also adds one header at a time | Same |
| `setCustomWebSocketHeaders(Properties)` (Paho 1.2.1 to 1.2.4) | `setCustomWebSocketHeaders(Map)` | **Changed** |
| `setExecutorServiceTimeout(int)` | No executor service to shut down | N/A |
| `getDebug()` | `client.enableDebugOutput(...)` | **Changed** |
| `MqttConnectOptions.validateURI(String)` | Invalid URIs are rejected by `setServerURI(...)` when parsed | **Changed** |
| `toString()` | Object identity only | **Missing** |

Settings this client adds: `setManualAcks`, `setMessageArrivedErrorsTriggerDisconnect`, `setRetryInterval`,
`setMaxRetries`, `setRetransmissionInterval`, `setMaxInflightRetries`, `setMaxBytesInMessage`, `setProxy`, `setHTTPProxy`, `setProxyAuth`,
`setSslEngineConsumer`, `setBindAddress`, `setChannelClass`, `setHost` and `setPort`.

### Callbacks

The client-wide callback is the biggest change. This client has a single `MqttCallback` interface that replaces
both Paho's `MqttCallback` and `MqttCallbackExtended`. Every event has an abstract `on<Event>` handler, which is
what your IDE inserts. Where Paho has the same event, the handler takes exactly Paho's parameters, so migrating
means adding `on` to each method name:

| Paho method | Rename to | Optional overloads for more detail |
|---|---|---|
| `connectComplete(boolean reconnect, String serverURI)` | `onConnectComplete(boolean reconnect, String serverURI)` | `(reconnect, isCleanSession, serverURI)`, `(reconnect, isCleanSession, serverURI, uuid)` |
| `connectionLost(Throwable cause)` | `onConnectionLost(Throwable cause)` | `(cause, uuid)`, `(cause, serverURI, uuid)` |
| `messageArrived(String topic, MqttMessage message) throws Exception` | `onMessageArrived(String topic, MqttMessage message) throws Exception` | `onMessageArrived(String topic, String payload)` (payload as UTF-8) |
| `deliveryComplete(IMqttDeliveryToken token)` | `onDeliveryComplete(IMqttDeliveryToken token)` | None |

These handlers have no Paho equivalent. They are abstract, so they must be implemented, but they can be empty:

| Handler | Called when |
|---|---|
| `onConnectFailed(String serverURI, String reason)` | A connection attempt fails. |
| `onConnecting(boolean connecting, String serverURI)` | A connection attempt starts (`true`) or ends (`false`). |
| `onReconnectFailed(String serverURI, String reason)` | Automatic reconnect gives up after `setMaxRetries` attempts. |
| `onSuccessfulReconnect(boolean isCleanSession, String serverURI)` | Automatic reconnect succeeds. |

Each of these four also has an optional overload with a trailing `uuid`, which identifies the client instance.

The names used by earlier versions of this client are also still present as empty default methods and are
still called: `connectComplete(reconnect, isCleanSession, serverURI)` and its `uuid` form,
`connectionLost(cause, uuid)`, `connectionLost(cause, serverURI, uuid)`, `isConnecting(serverURI)` and its `uuid`
form, `onConnectFailed(serverURI)`, `onMessage(topic, MqttMessage)` and `onMessage(topic, String payload)`.

The Paho method names are still present as empty default methods and are still called. A class that keeps
Paho's names unchanged therefore works once the abstract handlers are added. Do not keep both forms of the same
event, because both would run.

| Paho | This client | Status |
|---|---|---|
| `IMqttMessageListener.messageArrived(topic, message) throws Exception` | Same | Same |
| `IMqttActionListener.onSuccess(token)`, `onFailure(token, exception)` | Same | Same |
| `client.setCallback(callback)` | Same | Same |

Callbacks run on a dedicated single thread, in order, never on Netty's I/O threads.
`MqttAsyncClient.preserveMessageOrdering(false)` switches to a thread pool.

### `MqttAsyncClient` / `IMqttAsyncClient`

| Paho | This client | Status |
|---|---|---|
| `connect()`, `connect(options)`, `connect(options, userContext, callback)` | Same | Same |
| `connect(userContext, callback)` | Same. Note that `connect(String)` here means *connect to this host*. | Same |
| `subscribe(topic, qos)`, `subscribe(topic, qos, userContext, callback)` | Same | Same |
| `subscribe(topic, qos, userContext, callback, messageListener)` | Same | Same |
| `subscribe(String[], int[])`, `subscribe(String[], int[], userContext, callback)` | Same | Same |
| `subscribe(String[], int[], userContext, callback, IMqttMessageListener[])` | Same | Same |
| `subscribe(topic, qos, IMqttMessageListener)`, `subscribe(String[], int[], IMqttMessageListener[])` | Same | Same |
| `unsubscribe(topic)`, `unsubscribe(topic, userContext, callback)`, `unsubscribe(String[], userContext, callback)` | Same | Same |
| `unsubscribe(String[])` | Same | Same |
| `publish(topic, MqttMessage)`, `publish(topic, MqttMessage, userContext, callback)` | Same | Same |
| `publish(topic, byte[], qos, retained)` | Same | Same |
| `publish(topic, byte[], qos, retained, userContext, callback)` | Same | Same |
| `disconnect()` | Same | Same |
| `disconnect(quiesceTimeout, userContext, callback)`, `disconnect(quiesceTimeout)` | Same: new work is refused with `REASON_CODE_CLIENT_DISCONNECTING` and in-flight messages get up to `quiesceTimeout` to complete | Same |
| `disconnect(userContext, callback)` | Same, but disconnects at once rather than quiescing for 30 s | Same |
| `disconnectForcibly(quiesceTimeout, disconnectTimeout)`, `disconnectForcibly(quiesce, disconnect, sendDisconnectPacket)` | Same | Same |
| `disconnectForcibly(disconnectTimeout)` | Same, but does not quiesce first | Same |
| `disconnectForcibly()` | Closes at once, without quiescing or sending DISCONNECT | **Changed** |
| `close()`, `close(boolean force)` | Same, as aliases of `shutdown()` and `shutdown(boolean force)`. `close()` disconnects cleanly first, where Paho's throws if still connected | Same |
| `reconnect()` | Same | Same |
| `isConnected()`, `getClientId()`, `getServerURI()`, `getCurrentServerURI()` | Same | Same |
| (none; Paho issue 1079) | `isReconnecting()`: `true` while automatic reconnect is under way, including between attempts, when `isConnecting()` is `false` | **Added** |
| `setManualAcks(boolean)`, `messageArrivedComplete(messageId, qos)` | Same | Same |
| `getPendingDeliveryTokens()` | Same | Same |
| `checkPing(userContext, callback)` | `sendPing()` (no token) | **Changed** |
| `getInFlightMessageCount()` | Same; includes publishes held while disconnected | Same |
| `setBufferOpts(...)`, `getBufferedMessageCount()`, `getBufferedMessage(int)`, `deleteBufferedMessage(int)` | No configurable buffer; see [section 1](#1-behaviour-that-changes-silently) | **Missing** |
| `removeMessage(IMqttDeliveryToken)` | No equivalent | **Missing** |
| `getDebug()` | `enableDebugOutput(...)` | **Changed** |

### `MqttClient` / `IMqttClient`

| Paho | This client | Status |
|---|---|---|
| `connect(options)`, `connectWithResult(options)` | Same | Same |
| `connect()` | Same | Same |
| `subscribe(topic, qos)` | Same | Same |
| `subscribe(topic)`, `subscribe(String[])` (QoS 1), `subscribe(String[], int[])` | Same | Same |
| `subscribe(topic, IMqttMessageListener)`, `subscribe(topic, qos, IMqttMessageListener)` | Same | Same |
| `subscribe(String[], IMqttMessageListener[])`, `subscribe(String[], int[], IMqttMessageListener[])` | Same | Same |
| `subscribeWithResponse(...)` (all eight overloads) | Same | Same |
| `unsubscribe(topic)` | Same | Same |
| `unsubscribe(String[])` | Same | Same |
| `publish(topic, MqttMessage)`, `publish(topic, byte[], qos, retained)` | Same | Same |
| `disconnect()`, `disconnect(quiesceTimeout)` | Same; `disconnect()` disconnects at once rather than quiescing for 30 s | Same |
| `disconnectForcibly(quiesce, disconnect)`, `disconnectForcibly(quiesce, disconnect, sendDisconnectPacket)` | Same | Same |
| `disconnectForcibly(disconnectTimeout)` | Same, but does not quiesce first | Same |
| `disconnectForcibly()` | Closes at once, without quiescing or sending DISCONNECT | **Changed** |
| `close()` | Same | Same |
| `close(boolean force)` | Same: `close(true)` closes the connection at once and releases the client even if it is connected. `close()` and `close(false)` disconnect cleanly first, where Paho's throw if still connected | Same |
| `setTimeToWait(ms)`, `getTimeToWait()` | Same | Same |
| `getTopic(String)`, `reconnect()`, `messageArrivedComplete(...)`, `setManualAcks(...)` | Same | Same |
| `getPendingDeliveryTokens()`, `isConnected()`, `getClientId()`, `getServerURI()`, `getCurrentServerURI()` | Same | Same |
| `getDebug()` | `enableDebugOutput(...)` | **Changed** |

### Tokens

| Paho | This client | Status |
|---|---|---|
| `waitForCompletion()`, `waitForCompletion(timeout)`, `isComplete()`, `getException()` | Same | Same |
| `setActionCallback(listener)`, `setUserContext(...)`, `getUserContext()`, `getTopics()` | Same | Same |
| `getGrantedQos()` | Same | Same |
| `getActionCallback()` | Same | Same |
| `getClient()` | Same | Same |
| `getMessageId()` | Same | Same |
| `getSessionPresent()` | Same: the CONNACK session-present flag on the connect token | Same |
| `getResponse()` (Paho's `MqttWireMessage`) | No equivalent | N/A |
| `IMqttDeliveryToken.getMessage()` | Same | Same |

### Messages, topics and exceptions

| Paho | This client | Status |
|---|---|---|
| `MqttMessage`: constructors, `get`/`setPayload`, `get`/`setQos`, `isRetained`/`setRetained`, `isDuplicate`, `getId`/`setId`, `clearPayload` | Same; adds `getPayloadString()` and `setPayload(String)`. A QoS other than 0, 1 or 2, given to `setQos` or anywhere else the client takes an `int` QoS, means QoS 0, where Paho throws `IllegalArgumentException` | **Changed** |
| `MqttTopic.publish(...)`, `getName()`, `MqttTopic.validate(...)`, `MqttTopic.isMatched(...)` | Same | Same |
| `MqttException` and its `REASON_CODE_*` constants, `MqttSecurityException`, `MqttPersistenceException` | Same | Same |
| UTF-8 topic strings with control characters or unpaired surrogates | Paho rejects them; Netty's codec encodes control characters and replaces unpaired surrogates with `?` | **Changed** |

### TLS

Paho 1.2.1 and later verify the broker's hostname by default. This client does not, so turn it on:

```java
options.setServerURI("mqtts://broker.example.com:8883");
options.setSslContext(sslContext);   // javax.net.ssl.SSLContext or Netty SslContext
options.setHttpsHostnameVerificationEnabled(true);
```

This sets the `SSLEngine`'s endpoint identification algorithm to `HTTPS`, which is what Paho does. The hook set
with `setSslEngineConsumer(...)` runs afterwards, so it can still change the engine's parameters, including turning
the check off for one connection with an empty algorithm (`null` leaves the previous one in place).

For a check of your own, `setSSLHostnameVerifier(HostnameVerifier)` works as in Paho: after each TLS handshake the
verifier is called with the host from the broker URI and the `SSLSession`. If it returns `false` or throws, the
connection is closed before CONNECT (or the WebSocket upgrade request) is sent, and the connect fails with an
`SSLPeerUnverifiedException` (`Host: ..., Peer Host: ...`) in its cause chain. Also as in Paho, the verifier is not
used while `setHttpsHostnameVerificationEnabled(true)` is in effect.

#### `setSSLProperties(Properties)` and the `com.ibm.ssl.*` keys

This is the one Paho TLS setting with no counterpart here, and it is deliberately not implemented.

**Why the keys say IBM.** Paho's Java client began as IBM's MQ Telemetry (WebSphere MQ / MQTT) client, which IBM
contributed to Eclipse in 2012. Its TLS settings kept the property names of IBM's JSSE provider (IBMJSSE2) and
IBM's Java runtime, so a Paho `Properties` object looks like this:

```properties
com.ibm.ssl.protocol=TLSv1.2
com.ibm.ssl.keyStore=/etc/mqtt/client.p12
com.ibm.ssl.keyStorePassword=secret
com.ibm.ssl.keyStoreType=PKCS12
com.ibm.ssl.trustStore=/etc/mqtt/truststore.jks
com.ibm.ssl.trustStorePassword=changeit
com.ibm.ssl.enabledCipherSuites=TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256
```

**What Paho does with them.** When `ssl://` is used without `setSocketFactory`, Paho's internal
`SSLSocketFactoryFactory` builds an `SSLContext` from these properties. Each value is looked up in the
`Properties` first and, for most keys, then in the matching `javax.net.ssl.*` system property; anything still
unset gets the JVM default. Two details matter when porting:

- A password may be stored `{xor}`-encoded: XORed with a key hard-coded in Paho, then Base64-encoded. This only
  hides the password from a casual glance; anyone with the Paho jar can decode it.
- A key Paho does not know (for example a `javax.net.ssl.*` name) makes `setSSLProperties` throw
  `IllegalArgumentException("... is not a valid IBM SSL property key.")`.

| Key | Meaning in Paho | Equivalent here |
|---|---|---|
| `com.ibm.ssl.protocol` | Protocol for `SSLContext.getInstance` (default `TLS`) | `SSLContext.getInstance("TLSv1.2")` |
| `com.ibm.ssl.contextProvider` | JSSE provider, for example `IBMJSSE2` or `SunJSSE` | `SSLContext.getInstance(protocol, provider)` |
| `com.ibm.ssl.keyStore` | Client keystore file (fallback `javax.net.ssl.keyStore`) | Load a `KeyStore` from the file |
| `com.ibm.ssl.keyStorePassword` | Its password, plain or `{xor}` (fallback `javax.net.ssl.keyStorePassword`) | Pass to `KeyStore.load` and `KeyManagerFactory.init`; decode `{xor}` values first |
| `com.ibm.ssl.keyStoreType` | `JKS`, `PKCS12`, `JCEKS`... (fallback `javax.net.ssl.keyStoreType`, then `KeyStore.getDefaultType()`) | `KeyStore.getInstance(type)` |
| `com.ibm.ssl.keyStoreProvider` | JCE provider of the keystore, for example `IBMJCE` | `KeyStore.getInstance(type, provider)` |
| `com.ibm.ssl.keyManager` | Key manager algorithm (fallback `ssl.KeyManagerFactory.algorithm`), for example `IbmX509` | `KeyManagerFactory.getInstance(algorithm)` |
| `com.ibm.ssl.trustStore` | Truststore file (fallback `javax.net.ssl.trustStore`) | Load a `KeyStore` from the file |
| `com.ibm.ssl.trustStorePassword` | Its password, plain or `{xor}` (fallback `javax.net.ssl.trustStorePassword`) | Pass to `KeyStore.load` |
| `com.ibm.ssl.trustStoreType` | Truststore type (default `KeyStore.getDefaultType()`) | `KeyStore.getInstance(type)` |
| `com.ibm.ssl.trustStoreProvider` | JCE provider of the truststore | `KeyStore.getInstance(type, provider)` |
| `com.ibm.ssl.trustManager` | Trust manager algorithm (fallback `ssl.TrustManagerFactory.algorithm`), for example `IbmPKIX` | `TrustManagerFactory.getInstance(algorithm)` |
| `com.ibm.ssl.enabledCipherSuites` | Comma-separated cipher suites; platform defaults if unset | `setSslEngineConsumer(engine -> engine.setEnabledCipherSuites(...))` |
| `com.ibm.ssl.clientAuthentication` | Whether the peer must present a certificate. Only meaningful for a server; the Paho client ignores it | Nothing to do: a client presents its certificate whenever its keystore has one |

**Why it is not implemented.** Everything the properties do takes a few lines of standard JSSE code, shown below,
and that code is clearer about where the keys come from. Reproducing Paho's version would mean copying
IBM-specific vocabulary and defaults (`IbmX509`, `IBMJCE`), keeping the `{xor}` scheme, which looks like
protection but is not, and keeping Paho's quirks, such as rejecting the standard `javax.net.ssl.*` names. The
common case, where only the `javax.net.ssl.*` system properties are set, needs no code at all: a TLS URL without
`setSslContext` uses the JVM's default context, which reads them.

Translating a Paho `Properties` object:

```java
static SSLContext fromPahoProperties(Properties p) throws Exception {
    KeyManager[] keyManagers = null;
    String keyStoreFile = p.getProperty("com.ibm.ssl.keyStore");
    if (keyStoreFile != null) {
        char[] password = p.getProperty("com.ibm.ssl.keyStorePassword", "").toCharArray();  // decode {xor} first
        KeyStore keyStore = KeyStore.getInstance(p.getProperty("com.ibm.ssl.keyStoreType", KeyStore.getDefaultType()));
        try (InputStream in = new FileInputStream(keyStoreFile)) {
            keyStore.load(in, password);
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(
                p.getProperty("com.ibm.ssl.keyManager", KeyManagerFactory.getDefaultAlgorithm()));
        kmf.init(keyStore, password);
        keyManagers = kmf.getKeyManagers();
    }

    TrustManager[] trustManagers = null;   // null: the JVM's default trust store
    String trustStoreFile = p.getProperty("com.ibm.ssl.trustStore");
    if (trustStoreFile != null) {
        KeyStore trustStore = KeyStore.getInstance(p.getProperty("com.ibm.ssl.trustStoreType", KeyStore.getDefaultType()));
        try (InputStream in = new FileInputStream(trustStoreFile)) {
            trustStore.load(in, p.getProperty("com.ibm.ssl.trustStorePassword", "").toCharArray());
        }
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(
                p.getProperty("com.ibm.ssl.trustManager", TrustManagerFactory.getDefaultAlgorithm()));
        tmf.init(trustStore);
        trustManagers = tmf.getTrustManagers();
    }

    SSLContext context = SSLContext.getInstance(p.getProperty("com.ibm.ssl.protocol", "TLS"));
    context.init(keyManagers, trustManagers, null);
    return context;
}

options.setSslContext(fromPahoProperties(sslProperties));
String ciphers = sslProperties.getProperty("com.ibm.ssl.enabledCipherSuites");
if (ciphers != null) {
    options.setSslEngineConsumer(engine -> engine.setEnabledCipherSuites(ciphers.split("\\s*,\\s*")));
}
```

Add the `*Provider` keys as the second argument of the matching `getInstance` call if you use them.
The code is exercised against a broker by `PahoSslPropertiesRecipeTest`.

### Paho mode

`options.enablePahoMode(true)` switches, in one call, the settings whose defaults differ from Paho's to Paho's
values:

| Setting | Default here | Paho mode (Paho's value) |
|---|---|---|
| `setHttpsHostnameVerificationEnabled` | `false` | `true` |
| `setConnectionTimeout` | 5 s | 30 s |
| `setMaxInflight` | 1000 | 10 |
| `setMaxRetries` (automatic reconnect attempts) | 2 | `-1`, unlimited |
| `setMessageArrivedErrorsTriggerDisconnect` | `false` | `true` |
| `disconnect()`, `disconnect(userContext, callback)` | DISCONNECT at once | Quiesce for up to 30 s first |
| `disconnectForcibly()` | Close at once, no DISCONNECT | Quiesce up to 30 s, send DISCONNECT (up to 10 s), close |
| `disconnectForcibly(disconnectTimeout)` | No quiescing | Quiesce for up to 30 s first |

The values are set when `enablePahoMode` is called, so call it first and change individual settings afterwards;
`enablePahoMode(false)` puts this client's defaults back. Automatic reconnect itself stays off unless you call
`setAutomaticReconnect(true)`, as in Paho. While Paho mode is on, `setConnectionTimeout(0)` and
`setKeepAliveInterval(0)` are accepted, as in Paho, and mean no timeout and no keep-alive (they are stored as `-1`);
call them after `enablePahoMode(true)`. Paho mode does not add persistence, the MQTT 3.1 fallback or a bounded
publish buffer.

### Automatic reconnect

To get close to Paho's "retry forever" behaviour:

```java
options.setAutomaticReconnect(true);
options.setMaxRetries(-1);      // unlimited; the default is 2
// the wait starts at setRetryInterval (1 s) and doubles up to setMaxReconnectDelay (128 s), as in Paho
```

### Persistence

Paho stores in-flight QoS 1 and 2 messages in an `MqttClientPersistence` (a file store by default), so they
survive a restart. This client does the same when given one, as the third argument of
`MqttAsyncClient(serverURI, clientId, persistence)` or `MqttClient(serverURI, clientId, persistence)`, or with
`MqttAsyncClient(options, persistence)`; without one it keeps the state in memory only, unlike Paho.

```java
MqttAsyncClient client = new MqttAsyncClient("tcp://broker:1883", "sensor-17",
        new MqttDefaultFilePersistence("/var/lib/myapp/mqtt"));
```

- **What is stored:** QoS 1 and 2 publishes until acknowledged (updated when PUBREC arrives), and incoming QoS 2
  messages until PUBREL. `MqttDefaultFilePersistence` writes one file per message in a directory named after the
  client ID and broker URI, so each QoS 1 or 2 message costs a small file write and delete.
- **Restoring:** with `cleanSession=false`, the first `connect()` loads what the store holds and, after CONNACK,
  sends the publishes again (PUBLISH with DUP, or PUBREL) in their original order; their tokens are returned by
  `getPendingDeliveryTokens()`. A stored incoming QoS 2 message is delivered when the broker sends its PUBREL again.
- **A clean session clears the store**, as in Paho.
- **`close()` keeps what is stored**: the tokens fail with `REASON_CODE_CLIENT_CLOSED`, but the next client with the
  same ID, broker and store sends the messages.
- `MemoryPersistence` keeps the state in memory, for a later client in the same process.
- `MqttDbPersistence` keeps it in a SQLite database, in one table shared by any number of clients (rows are
  scoped by client ID and broker URI). It works through a `SqliteBridge`: `AndroidSqliteBridge` on Android, or
  `JdbcSqliteBridge` with a SQLite JDBC driver (such as `org.xerial:sqlite-jdbc`) elsewhere. The database belongs
  to the caller: closing the client or the persistence leaves it open.

  ```java
  // Android
  SqliteBridge db = AndroidSqliteBridge.open(new File(context.getFilesDir(), "mqtt.db"));
  // desktop or server, with a SQLite JDBC driver on the classpath
  SqliteBridge db = new JdbcSqliteBridge("mqtt.db");

  MqttAsyncClient client = new MqttAsyncClient(serverURI, clientId, new MqttDbPersistence(db));
  ```

  On Android, `MqttDefaultFilePersistence` on `context.getFilesDir()` works as well.

## 3. Paho compatibility status

Every Paho feature that is missing or incomplete. "Relevant" says whether the feature makes sense for a Netty
client; "Effort" is a rough estimate of what implementing it would take.

| Feature | Paho API | Status | Relevant | Effort | Notes |
|---|---|---|---|---|---|
| Disconnected publish buffer options | `DisconnectedBufferOptions`, `setBufferOpts`, `getBufferedMessageCount`, `getBufferedMessage`, `deleteBufferedMessage` | Partial (always on, unbounded, QoS 0 not buffered) | Yes | Medium | Add size limit, delete-oldest, persist-buffer and enable/disable. |
| Hostname verification on by default | `setHttpsHostnameVerificationEnabled(boolean)` | Changed (the setter exists, but is off by default) | Yes | Small | Kept off so existing connections keep working. |
| SSL properties | `setSSLProperties(Properties)` with `com.ibm.ssl.*` keys | Missing (by choice) | Partly | Medium | IBM-specific key names from Paho's origins as IBM's MQ Telemetry client; a short JSSE equivalent is given under [TLS](#tls). The `javax.net.ssl.*` fallback is already covered by the default context. |
| Socket factory | `setSocketFactory(SocketFactory)` | Changed (`setSslContext`) | Partly | Medium | An `SSLSocketFactory` cannot be turned into an `SSLEngine`; accepting an `SSLContext` covers the TLS case. |
| MQTT 3.1 fallback | `MQTT_VERSION_DEFAULT` tries 3.1.1 then 3.1 | Missing | Yes | Small | Retry with 3.1 when the broker refuses the protocol version. |
| Ping with token | `checkPing(userContext, callback)` | Changed (`sendPing()`, no token) | Yes | Small | |
| Remove a pending message | `removeMessage(IMqttDeliveryToken)` | Missing | Yes | Small | |
| URI validation | `MqttConnectOptions.validateURI(String)` | Changed (validated when parsed) | Yes | Small | |
| Options `toString()` and `getDebug()` | `MqttConnectOptions.toString()`, `getDebug()`, client `getDebug()` | Missing | Yes | Small | |
| Wire message access | `IMqttToken.getResponse()` | Missing | No | n/a | Returns Paho's internal packet classes; Netty's `MqttMessage` could be exposed instead. |
| Pluggable network modules | `NetworkModuleFactory`, `NetworkModuleService` | N/A | No | n/a | Transports are Netty pipeline handlers; `setChannelClass(...)` chooses the channel type. |
| Ping senders | `MqttPingSender`, `TimerPingSender`, `ScheduledExecutorPingSender` | N/A | No | n/a | Keep-alive uses Netty's `IdleStateHandler` on the event loop. |
| Executor service | `ScheduledExecutorService` constructor argument, `setExecutorServiceTimeout` | N/A | No | n/a | Use `setEventLoop(EventLoopGroup)`. |
| High-resolution timer | `HighResolutionTimer` constructor argument | N/A | No | n/a | |
| Paho logging | `org.eclipse.paho.client.mqttv3.logging`, `trace` properties | N/A | No | n/a | Use `enableDebugOutput(...)`. |
| `local://` transport | `LocalNetworkModule` | N/A | No | n/a | Netty's `LocalChannel` could provide this if needed. |
| Wire-level string checks | Paho rejects control characters and unpaired surrogates | N/A | No | n/a | Netty's codec encodes them. |
| Android service | `org.eclipse.paho.android.service` | N/A | No | n/a | Separate project; this client runs on Android directly. |
| MQTT 5 | `org.eclipse.paho.mqttv5.client` | Missing | Yes | Large | A separate Paho API. `MQTT_VERSION_5` can be selected here, but v5 properties, reason codes and auth are not handled, and it is not tested. |

## 4. In progress, not tested

Code that exists but is unfinished, not connected to the client, or has no automated tests. "Tested" refers to
this project's JUnit suite. Classes the client never used (`InFlightRegistry`, `MqttWatchDogHandler`,
`MqttConnectionOptions` and others) are in `scratch/`, outside the build.

| Item | Where | State | Tested |
|---|---|---|---|
| SQLite message persistence on Android | `AndroidSqliteBridge` | Implemented over `android.database.sqlite.SQLiteDatabase`; `MqttDbPersistence` and the restart scenario are tested through `JdbcSqliteBridge`, but the Android bridge itself needs a device or emulator to test. | No |
| Options reset | `MqttConnectOptions.reset()`, `MqttAsyncClient.resetConnectionoptions()` | Does nothing. | No |
| Read timeout | `MqttConnectOptions.getReadTimeout()` | Computed; not applied. | No |
| Reconnect limit and failure event | `setMaxRetries`, `onReconnectFailed` | Implemented. Reconnecting and the back-off timing are tested, but the retry limit and `onReconnectFailed` are not. | Partly |
| `onSuccessfulReconnect` | `MqttCallback` | Implemented; only its dispatch is tested, not a real reconnect. | Partly |
| Local bind address | `setBindAddress` | Implemented. | No |
| Custom channel class and event loop | `setChannelClass`, `setEventLoop` | Implemented. By default all clients share one event loop group; a group set with `setEventLoop` is never shut down by the client. | Partly: the event loop is tested, the channel class is not. |
| Retransmission on a live connection | `setRetransmissionInterval`, `setMaxInflightRetries` | Implemented; off by default, as Paho never resends on a live connection. | Yes |
| Subscribe once | `subscribeOnce(...)` | Implemented: unsubscribes after the first message. | No |
| Unordered callbacks | `preserveMessageOrdering(false)` | Implemented: callbacks on a thread pool. | No |
| `MqttCallback` subscription handler | `subscribe(topic, MqttCallback, qos)`, `unsubscribe(topic, MqttCallback)` | Implemented. | No |
| `ByteBuf` and `MqttQoS` overloads | `publish(topic, ByteBuf, ...)`, `publish(..., MqttQoS, ...)`, `subscribe(topic, MqttQoS)` | Implemented. | No |
| Pending delivery tokens | `getPendingDeliveryTokens()` | Implemented. | No |
| Ping on demand | `sendPing()` | Implemented. | No |
| Debug output | `enableDebugOutput(...)`, `enablePingLogging`, `enableChannelLogging` | Implemented. | No |
| MQTT 5 | `MQTT_VERSION_5` | Selectable, but v5 features are not handled. | No |
| Android detection | `MqttConnectOptions.isAndroid()` | Implemented. | No |
