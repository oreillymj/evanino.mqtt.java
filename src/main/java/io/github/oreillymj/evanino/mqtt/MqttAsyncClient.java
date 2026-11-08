/*******************************************************************************
 * Copyright (c) 2017, 2026 Michael OReilly
 *
 * This program and the accompanying materials are made available under
 * the terms of the Apache License, Version 2.0, which accompanies this
 * distribution (see LICENSE).
 *
 * The Apache License is available at
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Based on netty-mqtt-client by JetLinks (https://github.com/jetlinks/netty-mqtt-client),
 * itself based on netty-mqtt by Jeffrey Kog (https://github.com/jeffreykog/netty-mqtt);
 * see NOTICE.
 *
 * Contributors:
 *    Jeffrey Kog - original netty-mqtt implementation
 *    zhouhao (JetLinks) - netty-mqtt-client
 *    Michael OReilly - Paho-compatible API, and changes since
 *******************************************************************************/

package io.github.oreillymj.evanino.mqtt;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolHandler;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.handler.codec.http.websocketx.extensions.compression.WebSocketClientCompressionHandler;
import io.netty.handler.codec.mqtt.*;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.netty.handler.proxy.ProxyHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.*;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;


/**
 * Netty-based, Paho-compatible asynchronous MQTT client for a single broker.
 * <p>
 * When automatic reconnect is enabled in {@link MqttConnectOptions}, the connection is
 * re-established after it is lost; an initial connection failure is not retried.
 * User callbacks run on a dedicated callback executor rather than the Netty event loop.
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public final class MqttAsyncClient implements IMqttAsyncClient {

	private final String TAG = "MqttAsyncClient()->";

	private static final String MAJOR_VERSION = "1";
	private static final String MINOR_VERSION = "001";
	private static final String BUILD_NUMBER = "0";

	private static final char MIN_HIGH_SURROGATE = '\uD800';
	private static final char MAX_HIGH_SURROGATE = '\uDBFF';

	private boolean enableLogging = false;

	private SimpleLogger logger = new SimpleLogger();

	private volatile long timeToWait = 30_000; // Paho's default
	private final long pingResponseTimeoutMillis = 30_000;

	private final Map<String, Integer> serverSubscriptions = new ConcurrentHashMap<>();
	private final Map<String, Integer> pendingSubscribeTopics = new ConcurrentHashMap<>();
	private final Multimap<MqttCallback, MqttSubscription> handlerToSubscription = Multimaps.synchronizedSetMultimap(HashMultimap.<MqttCallback, MqttSubscription>create());
	private final AtomicInteger nextMessageId = new AtomicInteger(1);   // also the lock for allocating identifiers

	/** How often a quiescing disconnect checks whether in-flight work has finished. */
	private static final long QUIESCE_POLL_MILLIS = 20;

	/** Set while a disconnect waits for in-flight work to finish; new work is refused. */
	private volatile boolean quiescing = false;

	/** Guards the in-flight window check in publish, together with {@link #reservedPublishIds}. */
	private final Object inFlightLock = new Object();

	/** QoS 1 and 2 publishes that have a packet identifier but are not yet in pendingPublishes. */
	private int reservedPublishIds = 0;

	/** The session's persistent store, or {@code null} to keep the state in memory only. */
	private SessionStore store;

	/** Set once the stored session has been loaded, so it is loaded at most once. */
	private boolean sessionRestored;

	/** QoS 0 publishes made but not yet written to the socket, at most {@code getMaxUnwrittenQos0()}. */
	private final AtomicInteger unwrittenQos0 = new AtomicInteger();
	private final AtomicReference<MqttToken<Void>> pendingPing = new AtomicReference<>();

	private final Map<Integer, MqttPendingPublish> pendingPublishes = new ConcurrentHashMap<>();
	private final Map<Integer, MqttPendingSubscription> pendingSubscriptions = new ConcurrentHashMap<>();
	private final Map<Integer, MqttPendingUnsubscription> pendingServerUnsubscribes = new ConcurrentHashMap<>();
	private final Map<Integer, MqttIncomingQos2Publish> qos2PendingIncomingPublishes = new ConcurrentHashMap<>();
	private final Object subscribeLock = new Object();

	private final Multimap<String, MqttSubscription> subscriptions =
			Multimaps.synchronizedMultimap(HashMultimap.<String, MqttSubscription>create());

	private MqttConnectOptions clientConfig;

	/**
	 * The brokers given to {@link #connect(MqttServerList)}, or built from {@link
	 * MqttConnectOptions#setServerURIs(String[])}; {@code null} for a single broker.
	 */
	private MqttServerList serverList;

	/**
	 * Progress through {@link #serverList}, restarted by each connect the application makes;
	 * {@code null} unless it holds more than one broker. {@link #clientConfig} is always the
	 * current broker's options.
	 */
	private volatile ServerFailover failover;

	/** The connect moving through {@link #serverList}, while one is under way. */
	private volatile Promise<MqttConnectResult> serverWalk;

	/** The next attempt of {@link #serverWalk}, while it is waiting. */
	private volatile ScheduledFuture<?> serverWalkFuture;

	// The shared group by default; see SharedEventLoop. A group given to setEventLoop belongs to
	// the caller and is never shut down here.
	private EventLoopGroup eventLoop = SharedEventLoop.acquire();

	private boolean usesSharedEventLoop = true;

	private volatile Channel channel;

	private volatile ChannelHandler handler;

	private volatile boolean userdisconnected = false;

	/** Set once {@link #shutdown(boolean)} (or {@code close}) has begun. */
	private volatile boolean closing = false;

	private volatile boolean reconnect = false;

	private volatile boolean resubscribe = false;

	private final AtomicBoolean mqttConnecting = new AtomicBoolean(false);

	private final AtomicBoolean mqttConnected = new AtomicBoolean(false);

	private final Object connectLock = new Object();

	private volatile boolean hasEverConnected = false;

	private volatile Promise<MqttConnectResult> currentConnectFuture;

	/**
	 * The Channel handler.
	 */
	MqttChannelHandler channelHandler; // package-private, set by MqttChannelInitializer

	private AtomicInteger connectRetryCount = new AtomicInteger(0);
	private MqttCallback callback;

	/**
	 * Why the client closed the current connection itself, if it did: an exception from a
	 * message callback, or a packet from the broker that could not be decoded. Reported as the
	 * cause of the connection loss, or of the failed connect.
	 */
	private volatile Throwable closeError;

	private ScheduledFuture<?> rescheduleFuture = null;

	/**
	 * Whether automatic reconnect is under way: set when a reconnect is first scheduled after a
	 * connection loss, and cleared by CONNACK, by disconnect or close, or when the retries run
	 * out. Unlike {@link #isConnecting()}, it stays set between attempts.
	 */
	private volatile boolean reconnecting;

	private final int maxTotalReconnects = 100;

	private int maxTotalReconnectCount = 0;

	private String classuuid;

	/**
	 * This client's keep-alive ping times; never shared with other clients.
	 */
	private final KeepAliveTracker keepAliveTracker = new KeepAliveTracker();

	private boolean preserveMessageOrdering = true;

	// one per client instance, created alongside the client, not per-message
	private final ExecutorService callbackExecutor = Executors.newSingleThreadExecutor(r -> {

		Thread t = new Thread(r, "mqtt-callback-" + classuuid);
		t.setDaemon(true);
		return t;
	});

	private final ExecutorService multithreadedcallbackExecutor = Executors.newCachedThreadPool(r -> {

		Thread t = new Thread(r, "mqtt-callback-" + classuuid);
		t.setDaemon(true);
		return t;
	});

	/**
	 * Runs callbacks that arrive after a client's callback executor has been shut down by
	 * {@code close()}, such as the failures of tokens that close itself fails, so each still has
	 * its outcome. One daemon thread, shared by all clients and started on first use.
	 */
	private static final class AfterClose {
		static final ExecutorService EXECUTOR = Executors.newSingleThreadExecutor(r -> {
			Thread t = new Thread(r, "mqtt-callback-after-close");
			t.setDaemon(true);
			return t;
		});

		private AfterClose() {
		}
	}

	/**
	 * Runs a token or connection-event callback on the client's single callback thread, in
	 * order, or after {@code close()} on the shared after-close thread.
	 *
	 * @param callback the callback, which handles its own exceptions
	 */
	void runCallback(Runnable callback) {
		execute(callbackExecutor, callback);
	}

	/**
	 * Runs a message callback on the executor chosen by {@link #preserveMessageOrdering(boolean)},
	 * or after {@code close()} on the shared after-close thread.
	 *
	 * @param callback the callback, which handles its own exceptions
	 */
	void runMessageCallback(Runnable callback) {
		execute(getCallbackExecutor(), callback);
	}

	/**
	 * Runs a callback on an executor, or on the after-close thread if the executor has been shut down.
	 *
	 * @param executor the executor
	 * @param callback the callback
	 */
	private static void execute(ExecutorService executor, Runnable callback) {
		try {
			executor.execute(callback);
		} catch (java.util.concurrent.RejectedExecutionException closed) {
			AfterClose.EXECUTOR.execute(callback);
		}
	}


	/**
	 * Creates a client for the given broker and client ID, with otherwise default options.
	 *
	 * @param serverURI the URI of the broker, for example {@code tcp://host:1883}
	 * @param clientId  the client identifier; may be empty
	 * @throws MqttException            declared for Paho compatibility; not currently thrown
	 * @throws IllegalArgumentException if {@code serverURI} is not a valid broker URI, or
	 *                                  {@code clientId} is {@code null} or longer than 65535
	 *                                  characters
	 */
	public MqttAsyncClient(String serverURI, String clientId) throws MqttException {
		if (clientId == null) { // Support empty client Id, 3.1.1 standard
			throw new IllegalArgumentException("Null clientId");
		}
		// From Paho's MqttAsyncClient, Copyright (c) 2009, 2018 IBM Corp., used under the Eclipse
		// Distribution License v1.0 (see NOTICE and licenses/EDL-1.0.txt).
		// Count characters, surrogate pairs count as one character.
		int clientIdLength = 0;
		for (int i = 0; i < clientId.length() - 1; i++) {
			if (Character_isHighSurrogate(clientId.charAt(i)))
				i++;
			clientIdLength++;
		}
		if (clientIdLength > 65535) {
			throw new IllegalArgumentException("ClientId longer than 65535 characters");
		}

		if (serverURI == null || serverURI.trim().isEmpty()) {
			throw new IllegalArgumentException("Null or empty serverURI");
		}

		this.clientConfig = new MqttConnectOptions();
		this.clientConfig.setServerURI(serverURI);
		this.clientConfig.setClientId(clientId);
		// Parse now, as Paho validates the URI in its constructor, so a malformed address or an
		// unsupported scheme fails here rather than at the first connect.
		this.clientConfig.getServerURI();
		classuuid = UUID.randomUUID().toString();


		Date date = new Date();
		Calendar calendar = new GregorianCalendar();
		calendar.setTime(date);
		int year = calendar.get(Calendar.YEAR);

		if (BuildConfig.LOGGING && enableLogging) log("Evanino MQTT " + MAJOR_VERSION + "." + MINOR_VERSION + "." + BUILD_NUMBER + " © 2017-" + year);
	}


	/**
	 * Creates a client for the given broker and client ID that keeps its session state in
	 * {@code persistence}, as Paho's constructor of the same form does. QoS 1 and 2 publishes
	 * awaiting acknowledgement and incoming QoS 2 messages awaiting PUBREL are stored there, so
	 * with {@link MqttConnectOptions#setCleanSession(boolean) cleanSession} {@code false} a client
	 * created later with the same ID, broker and store (after a restart, with
	 * {@link MqttDefaultFilePersistence}) sends them again after connecting; their tokens are
	 * returned by {@link #getPendingDeliveryTokens()}. A clean session clears the store.
	 *
	 * @param serverURI   the URI of the broker, for example {@code tcp://host:1883}
	 * @param clientId    the client identifier; may be empty
	 * @param persistence the store, or {@code null} to keep the state in memory only
	 * @throws MqttException            declared for Paho compatibility; not currently thrown
	 * @throws IllegalArgumentException as for {@link #MqttAsyncClient(String, String)}
	 */
	public MqttAsyncClient(String serverURI, String clientId, MqttClientPersistence persistence) throws MqttException {
		this(serverURI, clientId);
		this.store = persistence == null ? null : new SessionStore(persistence);
	}

	/**
	 * Creates a client with the given options that keeps its session state in
	 * {@code persistence}; see {@link #MqttAsyncClient(String, String, MqttClientPersistence)}.
	 *
	 * @param clientConfig the connection options, including the broker URI
	 * @param persistence  the store, or {@code null} to keep the state in memory only
	 */
	public MqttAsyncClient(MqttConnectOptions clientConfig, MqttClientPersistence persistence) {
		this(clientConfig);
		this.store = persistence == null ? null : new SessionStore(persistence);
	}

	/**
	 * Creates a client with default options and no callback.
	 */
	public MqttAsyncClient() {
		this.clientConfig = new MqttConnectOptions();
		classuuid = UUID.randomUUID().toString();

		Date date = new Date();
		Calendar calendar = new GregorianCalendar();
		calendar.setTime(date);
		int year = calendar.get(Calendar.YEAR);

		if (BuildConfig.LOGGING && enableLogging) log("Evanino MQTT " + MAJOR_VERSION + "." + MINOR_VERSION + "." + BUILD_NUMBER + " © 2017-" + year);
	}


	/**
	 * Creates a client with default options.
	 *
	 * @param callbackHandler the callback for connection events and incoming messages
	 */
	public MqttAsyncClient(MqttCallback callbackHandler) {
		this.clientConfig = new MqttConnectOptions();
		this.callback = callbackHandler;
		classuuid = UUID.randomUUID().toString();

		Date date = new Date();
		Calendar calendar = new GregorianCalendar();
		calendar.setTime(date);
		int year = calendar.get(Calendar.YEAR);

		if (BuildConfig.LOGGING && enableLogging) log("Evanino MQTT " + MAJOR_VERSION + "." + MINOR_VERSION + "." + BUILD_NUMBER + " © 2017-" + year);
	}

	/**
	 * Creates a client with the given options and callback.
	 * The options can also be replaced using {@link #setOptions(MqttConnectOptions)}.
	 *
	 * @param clientConfig   The config object to use while looking for settings
	 * @param defaultHandler the callback for connection events and incoming messages
	 */
	public MqttAsyncClient(MqttConnectOptions clientConfig, MqttCallback defaultHandler) {
		this.clientConfig = clientConfig;
		this.callback = defaultHandler;
		classuuid = UUID.randomUUID().toString();

		Date date = new Date();
		Calendar calendar = new GregorianCalendar();
		calendar.setTime(date);
		int year = calendar.get(Calendar.YEAR);

		if (BuildConfig.LOGGING && enableLogging) log("Evanino MQTT " + MAJOR_VERSION + "." + MINOR_VERSION + "." + BUILD_NUMBER + " © 2017-" + year);
	}


	/**
	 * Creates a client with the given options and no callback.
	 *
	 * @param clientConfig the connection options
	 */
	public MqttAsyncClient(MqttConnectOptions clientConfig) {
		this.clientConfig = clientConfig;
		this.callback = null;
		classuuid = UUID.randomUUID().toString();
		Date date = new Date();
		Calendar calendar = new GregorianCalendar();
		calendar.setTime(date);
		int year = calendar.get(Calendar.YEAR);

		if (BuildConfig.LOGGING && enableLogging) log("Evanino MQTT " + MAJOR_VERSION + "." + MINOR_VERSION + "." + BUILD_NUMBER + " © 2017-" + year);
	}


	/**
	 * Logs a message if debug output is enabled.
	 *
	 * @param data the message to log
	 */
	private void log(String data) {
		if (enableLogging) {
			logger.log(data);
		}
	}


	/**
	 * Returns the client library version.
	 *
	 * @return the version in {@code major.minor.build} form
	 */
	public String getVersion() {
		return MAJOR_VERSION + "." + MINOR_VERSION + "." + BUILD_NUMBER;
	}

	/**
	 * Returns the major version of the client library.
	 *
	 * @return the major version
	 */
	public String getMajorVersion() {
		return MAJOR_VERSION;
	}

	/**
	 * Returns the minor version of the client library.
	 *
	 * @return the minor version
	 */
	public String getMinorVersion() {
		return MINOR_VERSION;
	}

	/**
	 * Returns the build number of the client library.
	 *
	 * @return the build number
	 */
	public String getRevision() {
		return BUILD_NUMBER;
	}


	/**
	 * Checks whether a character is a UTF-16 high surrogate.
	 *
	 * @param ch the character to check.
	 * @return returns 'true' if the character is a high-surrogate code unit
	 */
	protected static boolean Character_isHighSurrogate(char ch) {
		return (ch >= MIN_HIGH_SURROGATE) && (ch <= MAX_HIGH_SURROGATE);
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getServerURI() {
		URI uri = clientConfig.getServerURI();
		return uri != null ? uri.toString() : "unknown";
	}

	/**
	 * Returns the URI of the broker. Equivalent to {@link #getServerURI()}.
	 *
	 * @return the current server uri
	 */
	public String getCurrentServerURI() {
		return getServerURI();
	}

	/**
	 * Returns the executor used to run user callbacks: a single thread when message ordering
	 * is preserved (the default), otherwise a cached thread pool.
	 *
	 * @return the callback executor
	 */
	final ExecutorService getCallbackExecutor() {
		if (this.preserveMessageOrdering) {
			return this.callbackExecutor;
		} else {
			return this.multithreadedcallbackExecutor;
		}
	}

	/**
	 * Returns a new randomly generated client identifier, as Paho's static method does. Use it
	 * when a client needs a unique ID and does not need to resume a session. The form is
	 * {@code netty-mqtt/XXXXXXXX}, where Paho's is {@code paho} followed by a number; both fit
	 * MQTT 3.1's 23-character limit. {@link #getClientId()} returns the ID this client uses.
	 *
	 * @return a new client identifier
	 * @see MqttConnectOptions#setCleanSession(boolean)
	 */
	public static String generateClientId() {
		return MqttConnectOptions.newRandomClientId();
	}

	/**
	 * Gets client id.
	 *
	 * @return the client id
	 */
	public String getClientID() {
		return clientConfig.getClientId();
	}

	/**
	 * Gets client id.
	 *
	 * @return the client id
	 */
	public String getClientId() {
		return clientConfig.getClientId();
	}


	/**
	 * Returns the unique identifier of this client instance, passed to callbacks as {@code uuid}.
	 *
	 * @return the instance identifier
	 */
	public String getInstanceID() {
		return this.classuuid;
	}

	/**
	 * Returns a topic object bound to this client, for publishing to a single topic.
	 *
	 * @param topic the topic name
	 * @return the topic
	 * @throws IllegalArgumentException if the topic is empty or contains wildcards
	 */
	public MqttTopic getTopic(String topic) {
		if (topic == null || topic.isEmpty()) {
			throw new IllegalArgumentException("Topic must not be empty");
		}
		if (topic.indexOf('+') >= 0 || topic.indexOf('#') >= 0) {
			throw new IllegalArgumentException("Wildcards not allowed in publish topic: " + topic);
		}
		return new MqttTopic(topic, this);
	}

	/**
	 * Records whether the client has completed a connection since the last disconnect.
	 *
	 * @param v {@code true} once a CONNACK has been accepted
	 */
	void setHasEverConnected(boolean v) {
		this.hasEverConnected = v;
	}

	/**
	 * Returns whether the client has completed a connection since the last disconnect.
	 * Automatic reconnect is only attempted when this is {@code true}.
	 *
	 * @return {@code true} if a connection has been established
	 */
	boolean hasEverConnected() {
		return this.hasEverConnected;
	}

	/**
	 * {@inheritDoc}
	 *
	 * @param enableclient whether to log client activity
	 */
	public void enableDebugOutput(boolean enableclient) {
		enableLogging = enableclient;
	}

	/**
	 * {@inheritDoc}
	 *
	 * @param enableclient  whether to log client activity
	 * @param enablechannel whether to log channel activity
	 * @param enableping    whether to log keep-alive pings
	 */
	public void enableDebugOutput(boolean enableclient, boolean enablechannel, boolean enableping) {
		enableDebugOutput(enableclient);
		if (this.clientConfig != null) {
			clientConfig.enablePingLogging(enableping);
			clientConfig.enableChannelLogging(enablechannel);
		}


	}


	/**
	 * Handles the channel becoming inactive. If automatic reconnect is disabled the callback is
	 * told the connection was lost; a reconnect is then scheduled if one is required and not
	 * already pending. Does nothing if no callback is set.
	 *
	 * @param ctx the channel handler context
	 * @throws Exception if handling fails
	 */
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "channelInactive!!! Reconnect=" + this.clientConfig.getAutomaticReconnect());
		if (callback == null) {
			return;
		}
		ChannelClosedException e = new ChannelClosedException("channelInactive");
		//connected.set(false); //already set by ChannelHandler
		if (!this.getClientConfig().getAutomaticReconnect()) {
			MqttCallbackEvents.connectionLost(callback, e, getServerURI(), classuuid);
		}

		synchronized (this) {
			if (this.rescheduleFuture == null) {
				if (BuildConfig.LOGGING && enableLogging) log(TAG + "channelInactive->scheduleConnectIfRequired");
				scheduleConnectIfRequired(true);
			}
		}
	}//channelInactive

	/**
	 * Starts a connection attempt and wraps its future in a token.
	 *
	 * @param reconnect   whether this is an automatic reconnect
	 * @param userContext an object attached to the token, or {@code null}
	 * @param callback    a listener notified when the connect completes or fails, or {@code null}
	 * @return a token tracking the connection attempt
	 */
	private IMqttToken connectAndWrap(boolean reconnect, Object userContext, IMqttActionListener callback) {
		Future<MqttConnectResult> f = (!reconnect && startFailover()) ? walkServers() : connect(reconnect);
		MqttToken<MqttConnectResult> token = newToken((Promise<MqttConnectResult>) f);
		token.setUserContext(userContext);
		token.setActionCallback(callback);
		return token;
	}

	/**
	 * Starts again at the first broker for a connect the application makes, if there are
	 * several: those of {@link #serverList}, or of {@link MqttConnectOptions#setServerURIs(String[])}
	 * on the options. Leaves things alone while a connection attempt is in progress, as
	 * {@link #applyConnectOptions(MqttConnectOptions)} does, or while connected.
	 *
	 * @return {@code true} if the connect should move through a list of brokers
	 */
	private boolean startFailover() {
		if (isConnecting() || isConnected()) {
			return false;   // connect(false) returns the attempt in progress, or the connection
		}
		if (serverList == null) {
			String[] uris = clientConfig.getServerURIs();
			if (uris != null && uris.length > 1) {
				serverList = MqttServerList.fromServerURIs(clientConfig);
			}
		}
		if (serverList == null || serverList.size() < 2) {
			failover = null;
			return false;
		}
		ServerFailover servers = new ServerFailover(serverList);
		failover = servers;
		clientConfig = servers.current();
		return true;
	}

	/**
	 * Connects to the first broker of {@link #failover} that accepts the connection: after each
	 * failed attempt the next broker is tried, after its delay, until one connects or the retries
	 * run out. Runs whether or not automatic reconnect is enabled, as Paho's list of server URIs
	 * does. {@link #disconnect()} and {@link #close()} stop it.
	 *
	 * @return a future completed by the first successful attempt, or failed with the last
	 *         attempt's error when the client gives up
	 */
	private Promise<MqttConnectResult> walkServers() {
		if (this.eventLoop == null) {
			useSharedEventLoop();
		}
		Promise<MqttConnectResult> walk = new DefaultPromise<>(eventLoop.next());
		// Kept, even once the walk has given up, until a connection succeeds or the application
		// connects or disconnects again: while it is set, automatic reconnect leaves the
		// attempts to the walk, and a walk that gave up is a failed connect, not retried.
		serverWalk = walk;
		attemptNextServer(walk, 1);
		return walk;
	}

	/**
	 * Makes one attempt of a {@link #walkServers()} connect, with the current broker's options.
	 *
	 * @param walk    the connect's future
	 * @param attempt the number of this attempt in the connect, from 1
	 */
	private void attemptNextServer(Promise<MqttConnectResult> walk, int attempt) {
		serverWalkFuture = null;
		ServerFailover servers = failover;
		if (walk.isDone() || serverWalk != walk || servers == null) {
			return;
		}
		if (userdisconnected || closing) {
			walk.tryFailure(new MqttException(closing
					? MqttException.REASON_CODE_CLIENT_CLOSED
					: MqttException.REASON_CODE_CLIENT_DISCONNECTING));
			return;
		}
		clientConfig = servers.current();
		Future<MqttConnectResult> attemptFuture = connect(false);
		if (attemptFuture == null) {
			walk.tryFailure(new MqttException(MqttException.REASON_CODE_CLIENT_EXCEPTION));
			return;
		}
		attemptFuture.addListener(done -> {
			if (done.isSuccess()) {
				walk.trySuccess(attemptFuture.getNow());
				return;
			}
			if (walk.isDone() || serverWalk != walk || failover != servers) {
				return;
			}
			boolean onePassDone = servers.isOnePassOnFirstConnect() && attempt >= servers.size();
			if (!servers.failed() || onePassDone) {
				walk.tryFailure(done.cause());
				return;
			}
			if (BuildConfig.LOGGING && enableLogging) log(TAG + "walkServers()->trying "
					+ servers.current().getServerURI() + " next");
			// Paho's first connect through its server URIs moves to the next one at once.
			long delay = servers.isOnePassOnFirstConnect() ? 0 : servers.delayMillis();
			serverWalkFuture = eventLoop.schedule(() -> attemptNextServer(walk, attempt + 1),
					delay, TimeUnit.MILLISECONDS);
		});
	}


	/**
	 * {@inheritDoc}
	 * <p>
	 * A non-null {@code options} replaces the client's current options, unless a connection
	 * attempt is already in progress; {@code null} keeps the current options.
	 */
	public IMqttToken connect(MqttConnectOptions options) {
		applyConnectOptions(options);
		beginUserConnect();
		return connectAndWrap(false, null, null);
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * A non-null {@code options} replaces the client's current options, unless a connection
	 * attempt is already in progress; {@code null} keeps the current options.
	 */
	@Override
	public IMqttToken connect(MqttConnectOptions options, Object userContext, IMqttActionListener callback) {
		applyConnectOptions(options);
		beginUserConnect();
		return connectAndWrap(false, userContext, callback);
	}

	/**
	 * Prepares for a connection requested by the application rather than by automatic
	 * reconnect: clears a previous {@link #disconnect()}, the reconnect flag and the retry count,
	 * so the attempt is not rejected as a cancelled automatic reconnect.
	 */
	private void beginUserConnect() {
		this.userdisconnected = false;
		this.reconnect = false;
		connectRetryCount.set(0);
	}

	/**
	 * Replaces the client's options with those passed to a connect call. Ignored for
	 * {@code null}, and while a connection attempt is in progress so the attempt keeps a
	 * consistent configuration.
	 *
	 * @param options the options to use, or {@code null}
	 */
	private void applyConnectOptions(MqttConnectOptions options) {
		if ((options != null) && (!isConnecting())) {
			this.clientConfig = options;
			this.serverList = null;   // rebuilt from options.getServerURIs(), if it has several
		}
	}

	/**
	 * Connects to one of several brokers, for high availability: tries the first, and moves on
	 * through the list, as {@link MqttServerList} describes, until one accepts the connection or
	 * the retries run out. After a lost connection, automatic reconnect (if the broker's options
	 * enable it) moves through the list in the same way. With one broker in the list this is
	 * {@link #connect(MqttConnectOptions)} with its options.
	 * <p>
	 * The token completes when a broker accepts the connection, or fails with the last broker's
	 * error when the client gives up. {@link #getServerURI()} and the callbacks report the broker
	 * of the current attempt or connection.
	 *
	 * @param servers the brokers
	 * @return a token tracking the connect
	 * @throws IllegalArgumentException if {@code servers} is {@code null} or empty
	 */
	public IMqttToken connect(MqttServerList servers) {
		return connectToServers(servers, null, null);
	}

	/**
	 * Connects to one of several brokers, with a listener; see {@link #connect(MqttServerList)}.
	 * It is not an overload of {@code connect}, because Paho code often calls
	 * {@code connect(null, null, null)}, which would then be ambiguous.
	 *
	 * @param servers     the brokers
	 * @param userContext an object attached to the token, or {@code null}
	 * @param callback    a listener notified when the connect completes or fails, or {@code null}
	 * @return a token tracking the connect
	 * @throws IllegalArgumentException if {@code servers} is {@code null} or empty
	 */
	public IMqttToken connectToServers(MqttServerList servers, Object userContext, IMqttActionListener callback) {
		if (servers == null || servers.size() == 0) {
			throw new IllegalArgumentException("The server list must hold at least one broker");
		}
		if (!isConnecting()) {
			this.clientConfig = servers.getServers().get(0);
			this.serverList = servers.size() > 1 ? servers : null;
		}
		beginUserConnect();
		return connectAndWrap(false, userContext, callback);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IMqttToken connect() {
		return connect((Object) null, null);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @throws IllegalStateException if no options or no host are configured
	 */
	@Override
	public IMqttToken connect(Object userContext, IMqttActionListener callback) {
		if (clientConfig == null) {
			throw new IllegalStateException("MqttConnectOptions not configured");
		}
		if (clientConfig.getHost() == null && serverList == null && clientConfig.getServerURIs() == null) {
			throw new IllegalStateException("No host configured");
		}
		beginUserConnect();
		return connectAndWrap(false, userContext, callback);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @throws IllegalStateException if no options are configured
	 */
	public IMqttToken connect(String host) {
		if (clientConfig == null) {
			throw new IllegalStateException("MqttConnectOptions not configured");
		}
		int configuredPort = clientConfig.getPort();
		serverList = null;   // an explicit broker replaces a list
		clientConfig.setHost(host); // was setting host twice — the first setHost(host) call was immediately overwritten, removed
		if (configuredPort != -1 && MqttConnectOptions.explicitPort(host) == -1) {
			clientConfig.setPort(configuredPort); // keep the port already configured, as documented
		}
		beginUserConnect();
		return connectAndWrap(false, null, null);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IMqttToken connect(String host, int port) {
		if (clientConfig == null) {
			throw new IllegalStateException("MqttConnectOptions not configured");
		}
		serverList = null;   // an explicit broker replaces a list
		clientConfig.setHost(host);
		clientConfig.setPort(port); // after setHost, so the explicit port wins over any in host
		beginUserConnect();
		return connectAndWrap(false, null, null);
	}


	/**
	 * Starts a connection attempt. Concurrent callers share an attempt already in progress,
	 * and an automatic reconnect is rejected if the user has disconnected. The returned future
	 * completes when CONNACK is received, or fails if the TCP connection fails or the channel
	 * closes first.
	 *
	 * @param reconnect whether this is an automatic reconnect
	 * @return a future completed with the connect result
	 */
	private Future<MqttConnectResult> connect(boolean reconnect) {

		if (BuildConfig.LOGGING && enableLogging) log(TAG + "connect()->start");
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "connect()->clasuuid=" + classuuid);
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "connect()->isconnecting=" + isConnecting());
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "connect()->isconnected=" + isConnected());
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "connect()->enablelogging=" + enableLogging);
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "connect()->URI=" + clientConfig.getServerURI().toString());
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "connect()->Port=" + clientConfig.getPort());

		final Promise<MqttConnectResult> connectFuture;

		/*
		 * Only protect the connection-state transition.
		 *
		 * We do NOT hold this lock while doing any Netty work.
		 */
		synchronized (connectLock) {

			/*
			 * An automatic reconnect must never override an explicit
			 * disconnect().
			 *
			 * A normal connect(reconnect=false) is a new user/application
			 * connection request and is therefore allowed to clear the
			 * userDisconnected state.
			 */
			if (userdisconnected) {

				if (reconnect) {

					if (BuildConfig.LOGGING && enableLogging) log(TAG
							+ "connect()->automatic reconnect rejected because "
							+ "userdisconnected=true");

					/*
					 * We need to return a completed Future rather than null.
					 *
					 * Use the EventLoop if it exists; otherwise create the
					 * Promise using the current thread's context.
					 */
					if (this.eventLoop == null) {
						useSharedEventLoop();
					}


					if (eventLoop != null) {

						Promise<MqttConnectResult> cancelled =
								new DefaultPromise<>(eventLoop.next());

						cancelled.tryFailure(new MqttException(0, "Automatic reconnect cancelled by user disconnect"));
						return cancelled;
					}

					/*
					 * Normally eventLoop should exist if an automatic reconnect
					 * could have been scheduled, so this should be exceptional.
					 */
					if (BuildConfig.LOGGING && enableLogging) log(TAG
							+ "connect()->eventLoop is null while rejecting "
							+ "automatic reconnect");

					return null;
				}

				/*
				 * reconnect == false means this is a deliberate new connection.
				 */
				if (BuildConfig.LOGGING && enableLogging) log(TAG
						+ "connect()->fresh connection requested, "
						+ "clearing userdisconnected");

				userdisconnected = false;
			}




			/*
			 * Someone else is already attempting to connect.
			 *
			 * Return the same Future so concurrent callers share the
			 * existing connection attempt.
			 */
			if (isConnecting()) {
				if (BuildConfig.LOGGING && enableLogging) log(TAG + "connect()->connection already in progress");
				return currentConnectFuture;
			}

			/*
			 * We own the connection attempt from this point onwards.
			 */
			isConnecting(true);

			/*
			 * Someone is already connected. There is nothing to do.
			 *
			 * currentConnectFuture should represent the most recent
			 * successful connection.
			 */
			if (isConnected()) {
				if (BuildConfig.LOGGING && enableLogging) log(TAG + "connect()->already connected");
				return currentConnectFuture;
			}

			/*
			 * This is a deliberate new connection attempt.
			 * Must come after isConnecting/isConnected checks
			 */
			userdisconnected = false;

			/*
			 * The EventLoopGroup belongs to the client and should normally
			 * be created once and reused for subsequent connections.
			 */
			if (this.eventLoop == null) {
				useSharedEventLoop();
			}

			/*
			 * Create the Future while holding connectLock so that another
			 * caller cannot observe:
			 *
			 *     mqttConnecting == true
			 *     currentConnectFuture == null
			 */
			/*
			 * ConnectPromise is the single place where the connection-attempt
			 * flag is cleared, for BOTH success and failure. It clears the flag
			 * before completing, so a caller woken by waitForCompletion() never
			 * sees isConnecting() == true for a finished attempt.
			 */
			connectFuture = new ConnectPromise(eventLoop.next());

			currentConnectFuture = connectFuture;
		}

		/*
		 * From here onwards we are outside connectLock.
		 *
		 * Netty connection setup can therefore proceed asynchronously
		 * without blocking other callers.
		 */

		clientConfig.parseBrokerUrl(); // This will parse the connection options.
		openSession();

		final Bootstrap bootstrap = new Bootstrap();

		bootstrap.group(eventLoop);
		bootstrap.channel(clientConfig.getChannelClass());

		bootstrap.option(ChannelOption.SO_REUSEADDR, clientConfig.getSOReuseAddr());
		bootstrap.option(ChannelOption.SO_KEEPALIVE, clientConfig.getSOKeepAlive());
		bootstrap.option(ChannelOption.TCP_NODELAY, clientConfig.isTCPNoDelayEnabled());

		bootstrap.option(
				ChannelOption.CONNECT_TIMEOUT_MILLIS,
				clientConfig.getConnectTimeoutMillis());

		bootstrap.remoteAddress(
				clientConfig.getHost(),
				clientConfig.getPort());

		if (clientConfig.getBindAddress() != null) {
			bootstrap.localAddress(clientConfig.getBindAddress());
		}

		if (BuildConfig.LOGGING && enableLogging) log(TAG + "connect()->Initializing MqttChannelInitializer for "
				+ clientConfig.getHost() + ":"
				+ clientConfig.getPort());

		handler = new MqttChannelInitializer(
				this,
				connectFuture,
				clientConfig);

		if (handler == null) {
			if (BuildConfig.LOGGING && enableLogging) log(TAG + "connect()->Error initializing MqttChannelInitializer");

			ChannelClosedException e =
					new ChannelClosedException(
							"Failed to initialize channel handler",
							true);

			connectFuture.tryFailure(e);

			return connectFuture;
		}

		bootstrap.handler(handler);

		/*
		 * Start the TCP connection.
		 *
		 * This does NOT mean MQTT is connected. The connectFuture remains
		 * incomplete until CONNACK is received.
		 */
		final long connectStart = System.nanoTime();
		ChannelFuture channelFuture = bootstrap.connect();

		channelFuture.addListener((ChannelFutureListener) f -> {

			if (f.isSuccess()) {

				/*
				 * TCP connection established.
				 */
				MqttAsyncClient.this.channel = f.channel();
				limitConnectTime(f.channel(), connectFuture, connectStart);

				if (BuildConfig.LOGGING && enableLogging) log(TAG + "connect()->TCP connection established");

				/*
				 * Wait for the channel to close.
				 */
				MqttAsyncClient.this.channel.closeFuture().addListener(
						(ChannelFutureListener) closeFut -> {

							if (BuildConfig.LOGGING && enableLogging) log(TAG + "connect()->channel.closeFuture fired");

							/*
							 * The TCP channel is no longer usable.
							 */
							isConnected(false);

							/*
							 * Fail what was in flight on this connection before the
							 * application hears of the loss, so every publish, subscribe
							 * and unsubscribe has an outcome.
							 */
							failInFlight(new MqttException(closing
									? MqttException.REASON_CODE_CLIENT_CLOSED
									: userdisconnected
									? MqttException.REASON_CODE_CLIENT_DISCONNECTING
									: MqttException.REASON_CODE_CONNECTION_LOST));

							/*
							 * Channel closed BEFORE CONNACK.
							 *
							 * Therefore the connection attempt itself failed.
							 */
							if (!connectFuture.isDone()) {

								Throwable error = closeError;
								closeError = null;
								ChannelClosedException e =
										new ChannelClosedException(
												"Channel closed before CONNACK received",
												error != null ? error : closeFut.cause(),
												true);

								if (BuildConfig.LOGGING && enableLogging) log(TAG
										+ "connect()->channel closed before CONNACK"
										+ ", retryCount="
										+ connectRetryCount.get());

								/*
								 * Complete the Future FIRST.
								 *
								 * This causes mqttConnecting=false before the
								 * failure callback gets an opportunity to initiate
								 * another connection.
								 */
								connectFuture.tryFailure(e);

								/*
								 * Now notify the application.
								 */
								this.connectionLost(e);

							} else if (connectFuture.isSuccess()) {

								/*
								 * CONNACK was received and the connection had
								 * previously been established.
								 *
								 * This is a genuine post-connect connection loss.
								 */
								if (BuildConfig.LOGGING && enableLogging) log(TAG
										+ "connect()->connection lost after "
										+ "successful connect");

								// If the client closed the connection itself, because a message
								// callback threw (as Paho does) or a packet could not be decoded,
								// report that as the cause.
								Throwable callbackError = closeError;
								closeError = null;
								ChannelClosedException e =
										new ChannelClosedException(
												"Connection lost",
												callbackError != null ? callbackError : closeFut.cause(),
												false);

								this.connectionLost(e);
							}

							/*
							 * Connection-specific state is no longer valid; subscriptions
							 * are kept for a persistent session (see connectionEnded()).
							 */
							connectionEnded();

							rescheduleFuture = null;

							/*
							 * Automatic reconnect logic.
							 *
							 * This must itself be protected against duplicate
							 * scheduling.
							 */
							scheduleConnectIfRequired(true);
						});

			} else {

				/*
				 * TCP connection itself failed.
				 */
				Throwable cause = f.cause();

				if (BuildConfig.LOGGING && enableLogging) log(TAG
						+ "MqttAsyncClient()->connect()->connectionLost"
						+ ", retryCount="
						+ connectRetryCount.get());

				/*
				 * Complete the Future BEFORE notifying the callback.
				 *
				 * This is important because completion clears
				 * mqttConnecting.
				 */
				connectFuture.tryFailure(
						cause != null
								? cause
								: new ChannelClosedException(
								"Connection failed",
								true));

				if (hasEverConnected) {
					this.connectionLost(cause);
				} else {
					this.onConnectFailed(
							cause != null
									? cause.getMessage()
									: "Connection failed, check broker options");
				}

				rescheduleFuture = null;

				scheduleConnectIfRequired(reconnect);
			}
		});

		if (BuildConfig.LOGGING && enableLogging) log(TAG + "connect()->end");

		return connectFuture;
	}

	/**
	 * Returns whether the client is closing a channel itself, for a reason it will report when
	 * the channel has closed (see {@link #closeError}).
	 *
	 * @param channel the channel
	 * @return {@code true} if it is the current channel and a reason is recorded
	 */
	boolean isClosingWithError(Channel channel) {
		return channel == this.channel && closeError != null;
	}

	/**
	 * Applies the connection timeout to the whole connect, not only to the TCP connect that
	 * Netty's {@code CONNECT_TIMEOUT_MILLIS} covers: if CONNACK has not been received when it
	 * runs out (because the TLS or WebSocket handshake, or the broker, does not answer), the
	 * channel is closed and the connect fails with {@link MqttException#REASON_CODE_CLIENT_TIMEOUT}.
	 * Paho issues 1071, 856, 711 and 511 are about connects that wait for ever.
	 *
	 * @param channel        the channel just connected
	 * @param connectFuture  the connect's future, completed on CONNACK
	 * @param connectStart   when the connect started, from {@link System#nanoTime()}
	 */
	private void limitConnectTime(Channel channel, Promise<MqttConnectResult> connectFuture, long connectStart) {
		int timeoutMillis = clientConfig.getConnectTimeoutMillis();
		if (timeoutMillis <= 0) {
			return;   // no timeout of the client's own; keep-alive still ends a silent connection
		}
		long remaining = timeoutMillis - TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - connectStart);
		io.netty.util.concurrent.ScheduledFuture<?> timer = channel.eventLoop().schedule(() -> {
			if (!connectFuture.isDone() && channel.isActive()) {
				log(TAG + "connect()->no CONNACK within " + timeoutMillis + " ms; closing");
				if (channel == this.channel && closeError == null) {
					closeError = new MqttException(MqttException.REASON_CODE_CLIENT_TIMEOUT);
				}
				channel.close();
			}
		}, Math.max(0, remaining), TimeUnit.MILLISECONDS);
		connectFuture.addListener(done -> timer.cancel(false));
		channel.closeFuture().addListener(closed -> timer.cancel(false));
	}

	/**
	 * Schedules a reconnect after the configured retry interval, if automatic reconnect is
	 * enabled, the user has not disconnected, the client has connected before, and neither a
	 * connection nor a scheduled reconnect is already in progress. When the maximum number of
	 * retries is exceeded the callback is notified instead.
	 *
	 * @param reconnect whether to count this as a reconnect attempt
	 */
	private void scheduleConnectIfRequired(boolean reconnect) {//orig
		if (!clientConfig.getAutomaticReconnect()) {
			return;
		}
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "ScheduleConnectIfRequired()->start");
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "ScheduleConnectIfRequired()->Last connected to " + getServerURI());
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "ScheduleConnectIfRequired()->isConnected()=" + isConnected());
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "ScheduleConnectIfRequired()->isConnecting()=" + this.isConnecting());
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "ScheduleConnectIfRequired()->userdisconnected()=" + userdisconnected);

		if (userdisconnected) {
			if (BuildConfig.LOGGING && enableLogging) log(TAG
					+ "ScheduleConnectIfRequired()->"
					+ "user disconnected, not scheduling reconnect");
			return;
		}

		if (!hasEverConnected) {
			if (BuildConfig.LOGGING && enableLogging) log(TAG + "ScheduleConnectIfRequired()->no successful connection yet, not scheduling automatic retry");
			return; // initial connect failures are the caller's problem to handle, not ours to retry
		}

		if (serverWalk != null) {
			return;   // a connect moving through the brokers makes its own attempts (walkServers())
		}

		if (BuildConfig.LOGGING && enableLogging) log(TAG + "ScheduleConnectIfRequired()->previous keep alive time=" + keepAliveTracker.getLastPingDateTime());
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "ScheduleConnectIfRequired()->last keep alive time=" + keepAliveTracker.getPingDateTime());
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "uScheduleConnectIfRequired()->last keep alive delta secs=" + keepAliveTracker.getPingDeltaSecs());


		if (rescheduleFuture != null && !rescheduleFuture.isDone()) {
			return;
		} // already scheduled and still pending
		if ((isConnected()) || (this.isConnecting())) {
			return;
		}
		if (clientConfig.getAutomaticReconnect() && !userdisconnected) {
			if (reconnect) {
				this.reconnect = true;
				connectRetryCount.getAndIncrement();
			}

			ServerFailover servers = failover;
			if (servers != null) {
				// Several brokers: the lost connection or failed attempt counts against the
				// current one, and the list decides which to try next, or to give up.
				if (!servers.failed()) {
					reconnecting = false;
					this.onReconnectFailed(getServerURI(), "Max reconnect attempts exceeded on every server");
					return;
				}
				clientConfig = servers.current();
				if (BuildConfig.LOGGING && enableLogging) log(TAG + "ScheduleConnectIfRequired()->next server " + getServerURI());
			} else if (clientConfig.getMaxRetries() > -1) {
				if (BuildConfig.LOGGING && enableLogging) log(TAG + "ScheduleConnectIfRequired()->Max retries=" + clientConfig.getMaxRetries());
				if (BuildConfig.LOGGING && enableLogging) log(TAG + "ScheduleConnectIfRequired()->Retries=" + connectRetryCount);
				if (connectRetryCount.get() > clientConfig.getMaxRetries()) {
					reconnecting = false;
					String serverURI = getServerURI();
					this.onReconnectFailed(serverURI, "Max reconnect attempts exceeded");
					return;
				}
			}

			if ((!isConnected()) && (!this.isConnecting())) {
				if (BuildConfig.LOGGING && enableLogging) log(TAG + "ScheduleConnectIfRequired()->scheduled");
				reconnecting = true;
				rescheduleFuture = eventLoop.schedule(
						new Runnable() {
							@Override
							public void run() {
								if (BuildConfig.LOGGING && enableLogging) log(TAG + "ScheduleConnectIfRequired()->run()"
										+ " userdisconnected=" + userdisconnected
										+ " isConnected=" + isConnected()
										+ " isConnecting=" + isConnecting());

								if (userdisconnected) {
									if (BuildConfig.LOGGING && enableLogging) log(TAG + "ScheduleConnectIfRequired()->run() cancelled by user disconnect");
									reconnecting = false;
									rescheduleFuture = null;
									return;
								}

								if ((!isConnected()) && (!MqttAsyncClient.this.isConnecting())) {
									if (BuildConfig.LOGGING && enableLogging) log(TAG + "ScheduleConnectIfRequired()->calling connect");
									connect(reconnect);
								}

								rescheduleFuture = null;
							}
						}, reconnectDelayMillis(), TimeUnit.MILLISECONDS);
			}
		}
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "ScheduleConnectIfRequired()->end");
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isConnected() {
		boolean retval = !userdisconnected && this.mqttConnected.get() && channel != null && channel.isActive();
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "isConnected()-> userdisconnected=" + userdisconnected + " this.mqttConnected=" + this.mqttConnected.get());
		if (channel != null) {
			if (BuildConfig.LOGGING && enableLogging) log(TAG + "isConnected()-> channel.isActive()=" + channel.isActive());
		} else {
			if (BuildConfig.LOGGING && enableLogging) log(TAG + "isConnected()-> channel=null");
		}
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "isConnected()->isConnecting=" + mqttConnecting.get());
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "isConnected()-> returning retval=" + retval);
		return retval;
	}


	/**
	 * Records whether a CONNACK has been accepted on the current channel.
	 *
	 * @param connected the MQTT connection state
	 */
	protected void isConnected(boolean connected) {
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "isConnected()->setting to " + connected);
		this.mqttConnected.set(connected);
	}


	/**
	 * {@inheritDoc}
	 */
	public boolean isConnecting() {
		return mqttConnecting.get();
	}

	/**
	 * Returns whether the client is reconnecting automatically after losing its connection:
	 * from the connection loss until a reconnect succeeds, {@link #disconnect()} or
	 * {@link #close()} is called, or {@link MqttConnectOptions#setMaxRetries(int) the retries}
	 * run out. Unlike {@link #isConnecting()}, which is only {@code true} while an attempt is
	 * in progress, it stays {@code true} while waiting between attempts. Paho has no
	 * equivalent (Paho issue 1079).
	 *
	 * @return {@code true} while automatic reconnect is under way
	 */
	public boolean isReconnecting() {
		return reconnecting && !userdisconnected && !isConnected();
	}

	/**
	 * Records whether a connection attempt is in progress, and notifies the callback
	 * through {@link MqttCallback#onConnecting(boolean, String)} on the callback executor.
	 *
	 * @param connecting {@code true} when an attempt starts, {@code false} when it ends
	 */
	protected void isConnecting(boolean connecting) {
		this.mqttConnecting.set(connecting);
		MqttCallback cb;
		cb = this.callback; // snapshot — avoid a race if setCallback() runs concurrently
		if (cb == null) {
			if (BuildConfig.LOGGING && enableLogging) log(TAG + "isConnecting()->returning due to missing callback handler");
			return;
		}
		String serverURI = getServerURI();
		runCallback(() -> {
			try {
				MqttCallbackEvents.connecting(cb, connecting, serverURI, classuuid);
			} catch (Exception ex) {
				log(TAG + "isConnecting()->exception in callback: " + ex);
			}
		});
	}


	/**
	 * Returns the number of automatic reconnect attempts since the last successful connection.
	 *
	 * @return the retry count
	 */
	public int getConnectRetryCount() {
		return connectRetryCount.get();
	}

	/**
	 * Returns the maximum number of automatic reconnect attempts.
	 *
	 * @return the configured maximum, or {@code -1} for no limit
	 */
	public int getMaxConnectRetries() {
		return clientConfig.getMaxRetries();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IMqttToken reconnect() {
		if (clientConfig.getHost() == null) {
			throw new IllegalStateException(logger.getDate() + TAG + "reconnect()->Cannot reconnect. Call connect() first");
		}
		// Reconnect with the existing options. Passing getHost()/getPort() back through
		// connect(host, port) would replace the broker URL with the bare parsed host, losing
		// its scheme (ssl://, wss://) and WebSocket path.
		beginUserConnect();
		return connectAndWrap(false, null, null);
	}

	/**
	 * Retrieve the netty {@link EventLoopGroup} we are using
	 *
	 * @return The netty {@link EventLoopGroup} we use for the connection
	 */
	@Override
	public EventLoopGroup getEventLoop() {
		return eventLoop;
	}

	/**
	 * Returns the current channel to the broker.
	 *
	 * @return the channel, or {@code null} if there is none
	 */
	Channel getChannel() {
		return channel;
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * By default every client uses one shared NIO event loop group, so many clients share
	 * a few I/O threads. A group given here is used instead and stays the caller's: shutting
	 * the client down does not shut it down. With {@code null}, the next connect goes back to
	 * the shared group.
	 */
	@Override
	public void setEventLoop(EventLoopGroup eventLoop) {
		if (eventLoop == this.eventLoop) {
			return;
		}
		releaseEventLoop();
		this.eventLoop = eventLoop;
	}

	/**
	 * Sizes the event loop group shared by clients that are not given one with
	 * {@link #setEventLoop(EventLoopGroup)}: one thread per processor when enabled, rather than
	 * Netty's default of two. It is off by default. The group is shared by every client in the
	 * JVM, so this is set for all of them, and takes effect when the group is next created: call
	 * it before creating the first client, or after the last one has been shut down. Each client
	 * also has a callback thread, and the application has threads of its own, so on machines with
	 * few processors fewer I/O threads can leave more time for them. Netty's
	 * {@code io.netty.eventLoopThreads} system property, if set, gives the size instead.
	 *
	 * @param enable {@code true} for one thread per processor
	 */
	public static void enableSharedGroupSizing(boolean enable) {
		SharedEventLoop.setSizedToProcessors(enable);
	}

	/**
	 * Returns whether {@link #enableSharedGroupSizing(boolean)} is on.
	 *
	 * @return {@code true} if the shared group is created with one thread per processor
	 */
	public static boolean isSharedGroupSizingEnabled() {
		return SharedEventLoop.isSizedToProcessors();
	}

	/**
	 * Switches to the shared event loop group, taking a reference to it.
	 */
	private void useSharedEventLoop() {
		this.eventLoop = SharedEventLoop.acquire();
		this.usesSharedEventLoop = true;
	}

	/**
	 * Lets go of the current event loop group: releases the reference to the shared group, which
	 * is shut down once no client uses it, and leaves a group set with
	 * {@link #setEventLoop(EventLoopGroup)} running.
	 */
	private void releaseEventLoop() {
		if (usesSharedEventLoop) {
			SharedEventLoop.release(this.eventLoop);
			usesSharedEventLoop = false;
		}
		this.eventLoop = null;
	}

	/**
	 * Sets whether incoming messages must be acknowledged explicitly with
	 * {@link #messageArrivedComplete(int, int)}.
	 *
	 * @param manualAcks {@code true} to enable manual acknowledgements
	 */
	public void setManualAcks(boolean manualAcks) {
		//does nothing right now
		if (this.clientConfig != null) {
			this.clientConfig.setManualAcks(manualAcks);
		}
	}

	/**
	 * Returns whether manual acknowledgements are enabled.
	 *
	 * @return {@code true} if incoming messages must be acknowledged explicitly
	 */
	public boolean isManualAcks() {
		boolean retVal = false;
		if (this.clientConfig != null) {
			retVal = this.clientConfig.isManualAcks();
		}
		return retVal;
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public void messageArrivedComplete(int messageId, int qos) throws MqttException {
		if (!this.clientConfig.isManualAcks()) {
			return;
		}
		MqttChannelHandler handler = this.channelHandler; // snapshot — same volatile-read pattern as callback
		if (handler == null) {
			throw new MqttException(MqttException.REASON_CODE_CLIENT_NOT_CONNECTED);
		}
		handler.completeIncomingAck(messageId);
	}

	/**
	 * Subscribes to a topic filter, delivering messages to the default callback.
	 *
	 * @param topic       the topic filter
	 * @param qos         the qos to request from the server
	 * @param once        whether the subscription is removed after the first message
	 * @param userContext an object attached to the returned token, or {@code null}
	 * @param callback    a listener notified when the subscribe completes or fails, or {@code null}
	 * @return a token which will be completed when the server acknowledges the subscribe request
	 * @throws MqttException            if the subscribe cannot be started
	 * @throws IllegalArgumentException if the topic is empty
	 */
	IMqttToken subscribe(String topic, MqttQoS qos, boolean once,
						 Object userContext, IMqttActionListener callback) throws MqttException {
		if (topic == null || topic.isEmpty()) {
			throw new IllegalArgumentException("Topic must not be empty");
		}
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "subscribe()->start");
		return createSubscription(topic, this.getDefaultCallbackHandler(), once, qos, userContext, callback);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IMqttToken subscribe(String topicFilter, int qos, Object userContext, IMqttActionListener callback) throws MqttException {
		return subscribe(topicFilter, qosFromInt(qos), false, userContext, callback);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IMqttToken subscribe(String topicFilter, int qos) throws MqttException {
		return subscribe(topicFilter, qosFromInt(qos), false, null, null);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IMqttToken subscribe(String topicFilter, int qos, IMqttMessageListener messageListener) throws MqttException {
		return subscribe(topicFilter, qos, null, null, messageListener);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IMqttToken subscribe(String[] topicFilters, int[] qos, IMqttMessageListener[] messageListeners) throws MqttException {
		return subscribe(topicFilters, qos, null, null, messageListeners);
	}

	/**
	 * {@inheritDoc}
	 */
	public IMqttToken subscribe(String topic, MqttQoS qos) throws MqttException {
		return subscribe(topic, qos, false, null, null);
	}

	/**
	 * Subscribes to a topic filter, delivering messages to the given handler.
	 *
	 * @param topic   the topic filter
	 * @param handler the callback for matching messages
	 * @param qos     the qos to request from the server: 0, 1 or 2
	 * @return a token which will be completed when the server acknowledges the subscribe request
	 * @throws MqttException if the subscribe cannot be started
	 */
	public IMqttToken subscribe(String topic, MqttCallback handler, int qos) throws MqttException {
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "subscribe()->start");
		return createSubscription(topic, handler, false, qosFromInt(qos), null, null);
	}

	/**
	 * Subscribes to several topic filters in a single SUBSCRIBE packet, each with its own
	 * message listener.
	 *
	 * @param topicFilters     the topic filters to subscribe to
	 * @param qos              the qos to request for each topic filter, in the same order
	 * @param userContext      an object attached to the returned token, or {@code null}
	 * @param callback         a listener notified when the subscribe completes or fails, or {@code null}
	 * @param messageListeners a listener per topic filter; a {@code null} array or entry uses the default callback
	 * @return a token which will be completed when the server acknowledges the subscribe request
	 * @throws MqttException            if the subscribe cannot be started
	 * @throws IllegalArgumentException if the arrays differ in length, or a filter encodes to
	 *                                  more than 65535 bytes of UTF-8
	 */
	public IMqttToken subscribe(String[] topicFilters, int[] qos, Object userContext,
								IMqttActionListener callback,
								IMqttMessageListener[] messageListeners) throws MqttException {
		checkNotQuiescing();
		if (topicFilters.length != qos.length
				|| (messageListeners != null && messageListeners.length != topicFilters.length)) {
			throw new IllegalArgumentException("topicFilters, qos and messageListeners must be the same length");
		}
		checkFilters(topicFilters);

		List<String> topics = Arrays.asList(topicFilters);
		List<MqttQoS> qosLevels = new ArrayList<>();
		List<MqttTopicSubscription> subs = new ArrayList<>();
		for (int i = 0; i < topicFilters.length; i++) {
			MqttQoS q = qosFromInt(qos[i]);
			qosLevels.add(q);
			subs.add(new MqttTopicSubscription(topicFilters[i], q));
		}

		MqttCallback[] handlers = new MqttCallback[topicFilters.length];
		for (int i = 0; i < topicFilters.length; i++) {
			handlers[i] = (messageListeners != null && messageListeners[i] != null)
					? new MqttMessageListenerAdapter(messageListeners[i])
					: getDefaultCallbackHandler();
		}
		MqttToken<Void> held = subscribeLocallyIfHeld(topicFilters, qosLevels, handlers, userContext, callback);
		if (held != null) {
			return held;
		}

		Promise<Void> promise = new DefaultPromise<>(this.eventLoop.next());
		MqttToken<Void> token = newToken(promise);
		token.setUserContext(userContext);
		token.setActionCallback(callback);
		token.setTopics(topicFilters);

		MqttFixedHeader fixedHeader = MqttSubscribeFixedHeader.getFixedHeader();
		MqttMessageIdVariableHeader variableHeader = getNewMessageId();
		token.setMessageId(variableHeader.messageId());
		MqttSubscribeMessage message = new MqttSubscribeMessage(fixedHeader, variableHeader, new MqttSubscribePayload(subs));

		MqttPendingSubscription pending = new MqttPendingSubscription(token, topics, message, qosLevels);
		for (int i = 0; i < topicFilters.length; i++) {
			pending.addHandler(handlers[i], false, topicFilters[i]);   // null handlers are ignored inside addHandler
		}

		this.pendingSubscriptions.put(variableHeader.messageId(), pending);
		for (int i = 0; i < topics.size(); i++) {
			this.pendingSubscribeTopics.put(topics.get(i), qosLevels.get(i).value());
		}
		sendSubscribe(pending, message);
		pending.startRetransmitTimer(this.eventLoop.next(), this::sendAndFlushPacket, clientConfig.getRetransmissionInterval());
		return token;
	}


	/**
	 * {@inheritDoc}
	 */
	public IMqttToken subscribeOnce(String topic) throws MqttException {
		return subscribe(topic, MqttQoS.AT_MOST_ONCE, true, null, null);
	}

	/**
	 * Subscribes to a topic filter, delivering only the first matching message to the default
	 * callback, after which the subscription is removed.
	 *
	 * @param topic the topic filter
	 * @param qos   the qos to request from the server
	 * @return a token which will be completed when the server acknowledges the subscribe request
	 * @throws MqttException if the subscribe cannot be started
	 */
	public IMqttToken subscribeOnce(String topic, MqttQoS qos) throws MqttException {
		return subscribe(topic, qos, true, null, null);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IMqttToken subscribe(String[] topicFilters, int[] qos, Object userContext, IMqttActionListener callback) throws MqttException {
		checkNotQuiescing();
		if (topicFilters.length != qos.length) {
			throw new IllegalArgumentException("topicFilters and qos arrays must be the same length");
		}
		checkFilters(topicFilters);
		List<String> topics = Arrays.asList(topicFilters);
		List<MqttQoS> qosLevels = new ArrayList<>();
		List<MqttTopicSubscription> subs = new ArrayList<>();
		for (int i = 0; i < topicFilters.length; i++) {
			MqttQoS q = qosFromInt(qos[i]);
			qosLevels.add(q);
			subs.add(new MqttTopicSubscription(topicFilters[i], q));
		}
		MqttCallback[] handlers = new MqttCallback[topicFilters.length];
		Arrays.fill(handlers, this.getDefaultCallbackHandler());
		MqttToken<Void> held = subscribeLocallyIfHeld(topicFilters, qosLevels, handlers, userContext, callback);
		if (held != null) {
			return held;
		}

		Promise<Void> promise = new DefaultPromise<>(this.eventLoop.next());
		MqttToken<Void> token = newToken(promise);
		token.setUserContext(userContext);
		token.setActionCallback(callback);
		token.setTopics(topicFilters);

		MqttFixedHeader fixedHeader = MqttSubscribeFixedHeader.getFixedHeader();
		MqttMessageIdVariableHeader variableHeader = getNewMessageId();
		token.setMessageId(variableHeader.messageId());
		MqttSubscribePayload payload = new MqttSubscribePayload(subs);
		MqttSubscribeMessage message = new MqttSubscribeMessage(fixedHeader, variableHeader, payload);

		MqttPendingSubscription pendingSubscription = new MqttPendingSubscription(token, topics, message, qosLevels);
		pendingSubscription.addHandler(this.getDefaultCallbackHandler(), false);
		this.pendingSubscriptions.put(variableHeader.messageId(), pendingSubscription);
		for (int i = 0; i < topics.size(); i++) {
			this.pendingSubscribeTopics.put(topics.get(i), qosLevels.get(i).value());
		}


		sendSubscribe(pendingSubscription, message);
		pendingSubscription.startRetransmitTimer(this.eventLoop.next(), this::sendAndFlushPacket, clientConfig.getRetransmissionInterval());

		return token;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IMqttToken subscribe(String[] topicFilters, int[] qos) throws MqttException {
		return subscribe(topicFilters, qos, null, null);
	}

	/**
	 * Subscribes to a topic filter with a Paho-style message listener.
	 *
	 * @param topicFilter     the topic filter
	 * @param qos             the qos to request from the server: 0, 1 or 2
	 * @param userContext     an object attached to the returned token, or {@code null}
	 * @param callback        a listener notified when the subscribe completes or fails, or {@code null}
	 * @param messageListener the listener for matching messages, or {@code null} to use the default callback
	 * @return a token which will be completed when the server acknowledges the subscribe request
	 * @throws MqttException if the subscribe cannot be started
	 */
	public IMqttToken subscribe(String topicFilter, int qos, Object userContext,
								IMqttActionListener callback, IMqttMessageListener messageListener) throws MqttException {
		MqttCallback handler = (messageListener != null) ? new MqttMessageListenerAdapter(messageListener) : this.getDefaultCallbackHandler();
		return createSubscription(topicFilter, handler, false, qosFromInt(qos), userContext, callback);
	}


	// canonical — everything above funnels here

	/**
	 * For a subscribe to several filters: if the broker already holds every one of them at the
	 * QoS asked for, registers the handlers locally and returns a completed token, sending no
	 * SUBSCRIBE, as a single-topic subscribe does in that case. Otherwise returns {@code null},
	 * and one SUBSCRIBE for all the filters is sent (re-subscribing at the same QoS is harmless).
	 *
	 * @param topicFilters the filters
	 * @param qos          the QoS asked for each
	 * @param handlers     the handler for each; {@code null} entries are left out
	 * @param userContext  an object attached to the token, or {@code null}
	 * @param callback     a listener notified when the token completes, or {@code null}
	 * @return the completed token, or {@code null} if a SUBSCRIBE is needed
	 */
	private MqttToken<Void> subscribeLocallyIfHeld(String[] topicFilters, List<MqttQoS> qos, MqttCallback[] handlers,
												  Object userContext, IMqttActionListener callback) {
		synchronized (subscribeLock) {
			for (int i = 0; i < topicFilters.length; i++) {
				Integer held = serverSubscriptions.get(topicFilters[i]);
				if (held == null || held != qos.get(i).value()) {
					return null;
				}
			}
			for (int i = 0; i < topicFilters.length; i++) {
				if (handlers[i] != null) {
					MqttSubscription subscription = new MqttSubscription(topicFilters[i], handlers[i], qos.get(i), false);
					this.subscriptions.put(topicFilters[i], subscription);
					this.handlerToSubscription.put(handlers[i], subscription);
				}
			}
			MqttToken<Void> done = newToken(new DefaultPromise<>(eventLoop.next()));
			done.setUserContext(userContext);
			done.setActionCallback(callback);   // before completing, so onSuccess is delivered
			done.setTopics(topicFilters);
			done.trySuccess(null);
			return done;
		}
	}

	/**
	 * Checks that every topic filter is valid (see {@link MqttStrings#checkTopicFilter(String)}):
	 * no longer than MQTT's 16-bit length prefix allows, and with wildcards where MQTT allows
	 * them. A broker closes the connection on an invalid filter.
	 *
	 * @param topicFilters the filters
	 * @throws IllegalArgumentException if a filter is not valid
	 */
	private static void checkFilters(String[] topicFilters) {
		for (String filter : topicFilters) {
			MqttStrings.checkTopicFilter(filter);
		}
	}

	/**
	 * Creates a single-topic subscription. If the same topic and QoS is already being
	 * subscribed, the handler joins that request; if the broker already holds the subscription
	 * at that QoS, the handler is registered locally and the token completes immediately;
	 * otherwise a new SUBSCRIBE is sent.
	 *
	 * @param topic       the topic filter
	 * @param handler     the callback for matching messages
	 * @param once        whether the subscription is removed after the first message
	 * @param qos         the qos to request from the server
	 * @param userContext an object attached to the returned token, or {@code null}
	 * @param callback    a listener notified when the subscribe completes or fails, or {@code null}
	 * @return a token which will be completed when the subscription is in place
	 * @throws MqttException if the subscribe cannot be started
	 */
	private IMqttToken createSubscription(String topic, MqttCallback handler, boolean once, MqttQoS qos,
										  Object userContext, IMqttActionListener callback) throws MqttException {
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "createSubscription()->Start");
		MqttStrings.checkTopicFilter(topic);
		checkNotQuiescing();

		final MqttToken<Void> token;
		final MqttPendingSubscription pendingSubscription;
		final MqttSubscribeMessage message;

		synchronized (subscribeLock) {

			// 1. Same topic + QoS already in flight: follow it instead of sending a second SUBSCRIBE
			Integer pendingQos = pendingSubscribeTopics.get(topic);
			if (pendingQos != null && pendingQos == qos.value()) {
				MqttPendingSubscription inflight = findPending(topic, qos);
				if (inflight != null) {
					inflight.addHandler(handler, once);
					MqttToken<Void> follower = newToken(new DefaultPromise<>(eventLoop.next()));
					follower.setUserContext(userContext);
					follower.setActionCallback(callback);
					follower.setTopics(new String[]{topic});
					follower.setMessageId(inflight.getToken().getMessageId());   // the SUBSCRIBE it follows
					inflight.getToken().propagateTo(follower);
					if (BuildConfig.LOGGING && enableLogging) log(TAG + "createSubscription()->following in-flight SUBSCRIBE for " + topic);
					return follower;
				}
			}

			// 2. Already subscribed on the broker at this QoS: register the handler locally, no packet
			Integer existingQos = serverSubscriptions.get(topic);
			if (existingQos != null && existingQos == qos.value()) {
				MqttSubscription subscription = new MqttSubscription(topic, handler, qos, once);
				this.subscriptions.put(topic, subscription);
				this.handlerToSubscription.put(handler, subscription);

				MqttToken<Void> done = newToken(new DefaultPromise<>(eventLoop.next()));
				done.setUserContext(userContext);
				done.setActionCallback(callback);   // before completing, so onSuccess is delivered
				done.setTopics(new String[]{topic});
				done.trySuccess(null);
				if (BuildConfig.LOGGING && enableLogging) log(TAG + "createSubscription()->already subscribed at QOS " + qos.value());
				return done;
			}

			// 3. New subscription, or a different QoS (a second SUBSCRIBE replaces the first per the spec)
			token = newToken(new DefaultPromise<>(this.eventLoop.next()));
			token.setUserContext(userContext);
			token.setActionCallback(callback);
			token.setTopics(new String[]{topic});

			MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.SUBSCRIBE, false, MqttQoS.AT_LEAST_ONCE, false, 2);
			MqttMessageIdVariableHeader variableHeader = getNewMessageId();
			token.setMessageId(variableHeader.messageId());
			MqttSubscribePayload payload = new MqttSubscribePayload(
					Collections.singletonList(new MqttTopicSubscription(topic, qos)));
			message = new MqttSubscribeMessage(fixedHeader, variableHeader, payload);

			pendingSubscription = new MqttPendingSubscription(token, topic, message, qos);
			pendingSubscription.addHandler(handler, once);

			// Register BOTH before releasing the lock, so no other caller can see the
			// topic marker without the pending entry it points to.
			this.pendingSubscriptions.put(variableHeader.messageId(), pendingSubscription);
			this.pendingSubscribeTopics.put(topic, qos.value());
		}

		// Network work outside the lock
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "createSubscription()->sending SUBSCRIBE for topic " + topic);
		sendSubscribe(pendingSubscription, message);
		pendingSubscription.startRetransmitTimer(this.eventLoop.next(), this::sendAndFlushPacket, clientConfig.getRetransmissionInterval());
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "createSubscription()->End");
		return token;
	}

	/**
	 * Finds an unacknowledged SUBSCRIBE that includes the given topic at the given QoS.
	 *
	 * @param topic the topic filter
	 * @param qos   the requested QoS
	 * @return the pending subscription, or {@code null} if there is none
	 */
	private MqttPendingSubscription findPending(String topic, MqttQoS qos) {
		for (MqttPendingSubscription p : pendingSubscriptions.values()) {
			int idx = p.getTopics().indexOf(topic);
			if (idx >= 0 && p.getQosLevels().get(idx) == qos) {
				return p;
			}
		}
		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IMqttToken unsubscribe(String topic) throws MqttException {
		return unsubscribe(topic, null, null);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IMqttToken unsubscribe(String[] topicFilters) throws MqttException {
		return unsubscribe(topicFilters, null, null);
	}

	/**
	 * {@inheritDoc}
	 */
	public IMqttToken unsubscribe(String topic, MqttCallback handler) throws MqttException {
		MqttStrings.checkTopicFilter(topic);
		checkNotQuiescing();
		Promise<Void> promise = new DefaultPromise<>(this.eventLoop.next());
		MqttToken<Void> token = newToken(promise);
		token.setTopics(new String[]{topic});
		for (MqttSubscription subscription : this.handlerToSubscription.get(handler)) {
			this.subscriptions.remove(topic, subscription);
		}
		this.handlerToSubscription.removeAll(handler);
		ArrayList<String> topics = new ArrayList<String>();
		topics.add(topic);
		checkSubscriptionsForUnsubscribe(topics, token);
		return token;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IMqttToken unsubscribe(String topic, Object userContext, IMqttActionListener callback) throws MqttException {
		MqttStrings.checkTopicFilter(topic);
		checkNotQuiescing();
		Promise<Void> promise = new DefaultPromise<>(this.eventLoop.next());
		MqttToken<Void> token = newToken(promise);
		token.setUserContext(userContext);
		token.setActionCallback(callback);
		token.setTopics(new String[]{topic});

		ImmutableSet<MqttSubscription> subs = ImmutableSet.copyOf(this.subscriptions.get(topic));
		for (MqttSubscription subscription : subs) {
			for (MqttSubscription handSub : this.handlerToSubscription.get(subscription.getHandler())) {
				this.subscriptions.remove(topic, handSub);
			}
			this.handlerToSubscription.remove(subscription.getHandler(), subscription);
		}

		ArrayList<String> topics = new ArrayList<String>();
		topics.add(topic);
		checkSubscriptionsForUnsubscribe(topics, token);
		return token;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IMqttToken unsubscribe(String[] topicFilters, Object userContext, IMqttActionListener callback) throws MqttException {
		checkFilters(topicFilters);
		checkNotQuiescing();
		Promise<Void> promise = new DefaultPromise<>(this.eventLoop.next());
		MqttToken<Void> token = newToken(promise);
		token.setUserContext(userContext);
		token.setActionCallback(callback);
		token.setTopics(topicFilters);

		for (String topic : topicFilters) {
			ImmutableSet<MqttSubscription> subs = ImmutableSet.copyOf(this.subscriptions.get(topic));
			for (MqttSubscription subscription : subs) {
				for (MqttSubscription handSub : this.handlerToSubscription.get(subscription.getHandler())) {
					this.subscriptions.remove(topic, handSub);
				}
				this.handlerToSubscription.remove(subscription.getHandler(), subscription);
			}
		}
		// NOTE: checkSubscriptions as currently shaped only handles one topic per call.
		// A real batch UNSUBSCRIBE (one packet, multiple topics) needs the same list-based
		// treatment as MqttPendingSubscription got above — flagging rather than guessing
		// at MqttPendingUnsubscription's shape without seeing it extended first.
		checkSubscriptionsForUnsubscribe(Arrays.asList(topicFilters), token);
		return token;
	}

	/**
	 * Sends one UNSUBSCRIBE for the given topics that have no local handlers left, are
	 * subscribed on the broker and are not already being unsubscribed. Completes the token
	 * immediately if no topic needs unsubscribing.
	 *
	 * @param topics the topics whose handlers have been removed
	 * @param token  the token to complete when the UNSUBACK arrives
	 * @throws MqttException if no packet identifier is free
	 */
	private void checkSubscriptionsForUnsubscribe(List<String> topics, MqttToken<Void> token) throws MqttException {
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "checkSubscriptionsForUnsubscribe()->Start");

		final MqttUnsubscribeMessage message;
		final MqttPendingUnsubscription pending;

		synchronized (subscribeLock) {
			List<String> topicsNeedingUnsubscribe = new ArrayList<>();
			for (String topic : topics) {
				boolean stillHasLocalSubscribers = !this.subscriptions.get(topic).isEmpty();
				boolean subscribedServerSide = this.serverSubscriptions.containsKey(topic);
				boolean alreadyUnsubscribing = isUnsubscribeInFlight(topic);
				if (!stillHasLocalSubscribers && subscribedServerSide && !alreadyUnsubscribing) {
					topicsNeedingUnsubscribe.add(topic);
				}
			}

			if (topicsNeedingUnsubscribe.isEmpty()) {
				if (BuildConfig.LOGGING && enableLogging) log(TAG + "checkSubscriptionsForUnsubscribe()->No topics to unsubscribe");
				token.trySuccess(null);
				return;
			}

			MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.UNSUBSCRIBE, false, MqttQoS.AT_LEAST_ONCE, false, 0);
			MqttMessageIdVariableHeader variableHeader = getNewMessageId();
			token.setMessageId(variableHeader.messageId());
			message = new MqttUnsubscribeMessage(fixedHeader, variableHeader,
					new MqttUnsubscribePayload(topicsNeedingUnsubscribe));
			pending = new MqttPendingUnsubscription(token, topicsNeedingUnsubscribe, message);
			this.pendingServerUnsubscribes.put(variableHeader.messageId(), pending);
		}

		// network work outside the lock
		this.sendAndFlushPacket(message);
		pending.startRetransmissionTimer(this.eventLoop.next(), this::sendAndFlushPacket, clientConfig.getRetransmissionInterval());
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "checkSubscriptionsForUnsubscribe()->End");
	}

	/**
	 * Checks whether an unacknowledged UNSUBSCRIBE includes the given topic.
	 *
	 * @param topic the topic filter
	 * @return {@code true} if an UNSUBSCRIBE for the topic is pending
	 */
	private boolean isUnsubscribeInFlight(String topic) {
		for (MqttPendingUnsubscription u : pendingServerUnsubscribes.values()) {
			if (u.getTopics().contains(topic)) return true;
		}
		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IMqttDeliveryToken publish(String topic, ByteBuf payload) throws MqttException {
		return publish(topic, payload, MqttQoS.AT_MOST_ONCE, false);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IMqttDeliveryToken publish(String topic, ByteBuf payload, MqttQoS qos) throws MqttException {
		return publish(topic, payload, qos, false);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IMqttDeliveryToken publish(String topic, ByteBuf payload, boolean retain) throws MqttException {
		return publish(topic, payload, MqttQoS.AT_MOST_ONCE, retain);
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * A QoS outside 0 to 2 is treated as 0.
	 */
	@Override
	public IMqttDeliveryToken publish(String topic, ByteBuf payload, int qos, boolean retain) throws MqttException {
		return publish(topic, payload, qosFromInt(qos), retain);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IMqttDeliveryToken publish(String topic, ByteBuf payload, MqttQoS qos, boolean retain) throws MqttException {
		MqttMessage message = new MqttMessage(byteBufToBytes(payload));
		message.setQos(qos.value());
		message.setRetained(retain);
		return publish(topic, message, null, null);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IMqttDeliveryToken publish(String topic, String payload) throws MqttException {
		return publish(topic, payload.getBytes(StandardCharsets.UTF_8), 0, false);
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * A QoS outside 0 to 2 is treated as 0.
	 */
	@Override
	public IMqttDeliveryToken publish(String topic, String payload, int qos) throws MqttException {
		return publish(topic, payload.getBytes(StandardCharsets.UTF_8), qos, false);
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * A QoS outside 0 to 2 is treated as 0.
	 */
	@Override
	public IMqttDeliveryToken publish(String topic, String payload, int qos, boolean retain) throws MqttException {
		return publish(topic, payload.getBytes(StandardCharsets.UTF_8), qos, retain);
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public IMqttDeliveryToken publish(String topic, byte[] payload) throws MqttException {
		return publish(topic, payload, 0, false);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IMqttDeliveryToken publish(String topic, byte[] payload, boolean retain) throws MqttException {
		return publish(topic, payload, 0, retain);
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * A QoS outside 0 to 2 is treated as 0.
	 */
	@Override
	public IMqttDeliveryToken publish(String topic, byte[] payload, int qos) throws MqttException {
		return publish(topic, payload, qos, false);
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * A QoS outside 0 to 2 is treated as 0.
	 */
	@Override
	public IMqttDeliveryToken publish(String topic, byte[] payload, int qos, boolean retain) throws MqttException {
		return publish(topic, payload, qos, retain, null, null);
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * A QoS outside 0 to 2 is treated as 0.
	 */
	@Override
	public IMqttDeliveryToken publish(String topic, byte[] payload, int qos, boolean retained, Object userContext,
									  IMqttActionListener callback) throws MqttException {
		MqttMessage message = new MqttMessage(payload);
		if ((qos < 0) || (qos > 2)) {
			qos = 0;
		}
		message.setQos(qos);
		message.setRetained(retained);
		return publish(topic, message, userContext, callback);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IMqttDeliveryToken publish(String topic, byte[] payload, MqttQoS qos, boolean retain) throws MqttException {
		MqttMessage message = new MqttMessage(payload);
		message.setQos(qos.value());
		message.setRetained(retain);
		return publish(topic, message, null, null);
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public IMqttDeliveryToken publish(String topic, MqttMessage message) throws MqttException {
		return publish(topic, message, null, null);
	}

	// canonical — everything above funnels here

	/**
	 * {@inheritDoc}
	 * <p>
	 * If the client is not connected, a QoS 0 publish fails immediately with
	 * {@link MqttException#REASON_CODE_CLIENT_NOT_CONNECTED}, while a QoS 1 or 2 publish is held
	 * as pending. A sent QoS 0 publish completes once it has been written to the socket, and
	 * then {@link MqttCallback#deliveryComplete(IMqttDeliveryToken)} is called; at most
	 * {@link MqttConnectOptions#setMaxUnwrittenQos0(int)} QoS 0 publishes may be waiting to be
	 * written. QoS 1 and 2 publishes complete when the broker acknowledges
	 * them. If the connection closes before a QoS 1 or 2 publish
	 * is acknowledged, then with {@link MqttConnectOptions#setCleanSession(boolean) cleanSession}
	 * {@code false} the publish is kept and sent again after the next CONNACK, with the DUP flag
	 * set and the same packet identifier (or its PUBREL is, if PUBREC had arrived), and its token
	 * completes when the broker acknowledges it; with a clean session its token fails with
	 * {@link MqttException#REASON_CODE_CONNECTION_LOST}, or
	 * {@link MqttException#REASON_CODE_CLIENT_DISCONNECTING} after a disconnect. Publishes
	 * still pending when the client is closed fail with
	 * {@link MqttException#REASON_CODE_CLIENT_CLOSED}.
	 *
	 * @throws IllegalArgumentException if the topic is empty, encodes to more than 65535 bytes
	 *                                  of UTF-8 (the most MQTT can carry), or contains a
	 *                                  wildcard or U+0000
	 * @throws MqttException with {@link MqttException#REASON_CODE_MAX_INFLIGHT} if a QoS 1 or 2
	 *                       publish would exceed {@link MqttConnectOptions#setMaxInflight(int)},
	 *                       or a QoS 0 publish {@link MqttConnectOptions#setMaxUnwrittenQos0(int)}, or
	 *                       {@link MqttException#REASON_CODE_NO_MESSAGE_IDS_AVAILABLE} if no
	 *                       packet identifier is free
	 */
	@Override
	public IMqttDeliveryToken publish(String topic, MqttMessage message,
									  Object userContext, IMqttActionListener callback) throws MqttException {
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "publish()->Start");
		if (topic == null || topic.isEmpty()) {
			throw new IllegalArgumentException("Topic must not be empty");
		}
		MqttStrings.checkTopicName(topic);
		checkNotQuiescing();
		MqttQoS qos = qosFromInt(message.getQos());
		int messageId = -1;
		if (qos != MqttQoS.AT_MOST_ONCE) {
			// Check the window and take an identifier together, and count the identifier as
			// reserved until the publish is recorded in pendingPublishes, so that concurrent
			// publishes cannot exceed the window.
			synchronized (inFlightLock) {
				if (pendingPublishes.size() + reservedPublishIds >= clientConfig.getMaxInflight()) {
					throw new MqttException(MqttException.REASON_CODE_MAX_INFLIGHT);
				}
				messageId = getNewMessageId().messageId();
				reservedPublishIds++;
			}
			try {
				return publish(topic, message, qos, messageId, userContext, callback);
			} finally {
				synchronized (inFlightLock) {
					reservedPublishIds--;
				}
			}
		}
		// QoS 0 has a window of its own: publishes not yet written to the socket. Without it a
		// fast publisher could queue messages without limit.
		if (unwrittenQos0.incrementAndGet() > clientConfig.getMaxUnwrittenQos0()) {
			unwrittenQos0.decrementAndGet();
			throw new MqttException(MqttException.REASON_CODE_MAX_INFLIGHT);
		}
		IMqttDeliveryToken token = null;
		try {
			token = publish(topic, message, qos, messageId, userContext, callback);
		} finally {
			if (token instanceof MqttDeliveryToken) {
				// The token completes once the PUBLISH is written, or fails; either frees the slot.
				((MqttDeliveryToken) token).getPromise().addListener(f -> unwrittenQos0.decrementAndGet());
			} else {
				unwrittenQos0.decrementAndGet();
			}
		}
		return token;
	}

	/**
	 * Builds and sends a publish whose packet identifier has already been allocated.
	 *
	 * @param topic       the topic to publish to
	 * @param message     the message to publish
	 * @param qos         the QoS of the message
	 * @param messageId   the packet identifier, or {@code -1} for QoS 0
	 * @param userContext an object attached to the returned token, or {@code null}
	 * @param callback    a listener notified when delivery completes or fails, or {@code null}
	 * @return a token which will be completed when delivery completes for the QoS
	 */
	private IMqttDeliveryToken publish(String topic, MqttMessage message, MqttQoS qos, int messageId,
									   Object userContext, IMqttActionListener callback) {
		MqttPendingPublish pendingPublish = null;
		MqttPublishMessage publishMsg = null;
		MqttDeliveryToken token = null;
		boolean connected = false;
		try {
			ByteBuf payloadBuf = Unpooled.wrappedBuffer(message.getPayload());
			if (BuildConfig.LOGGING && enableLogging) log(TAG + "publish()->messageid=" + messageId);

			publishMsg = MqttMessageBuilders.publish()
					.topicName(topic)
					// build() copies the payload with writeBytes, which would drain payloadBuf;
					// give it a duplicate so payloadBuf stays readable for retransmissions.
					.payload(payloadBuf.duplicate())
					.qos(qos)
					.retained(message.isRetained())
					.messageId(messageId)
					.build();


			// The token completes on the channel's event loop, so its promise uses that loop:
			// listeners are then notified at once rather than handed to another thread.
			Channel current = this.channel;
			Promise<Void> promise = new DefaultPromise<>(current != null ? current.eventLoop() : eventLoop.next());
			token = new MqttDeliveryToken(this, messageId, new String[]{topic}, promise, this::runCallback);
			token.setUserContext(userContext);
			token.setActionCallback(callback);
			token.setMessage(message);

			pendingPublish = new MqttPendingPublish(
					messageId, token,
					(qos == MqttQoS.AT_MOST_ONCE) ? payloadBuf : payloadBuf.retain(),
					publishMsg, qos, clientConfig.getMaxInflightRetries());

			if (qos != MqttQoS.AT_MOST_ONCE) {
				// Record the publish before sending it, so that from here on it is either sent
				// on a connection, and failed if that connection is lost, or held until the
				// next CONNACK sends it.
				pendingPublishes.put(messageId, pendingPublish);
				storeOutgoing(pendingPublish);
			}
			connected = isConnected();
			if (!connected) {
				if (qos == MqttQoS.AT_MOST_ONCE) {
					token.markFailed(new MqttException(MqttException.REASON_CODE_CLIENT_NOT_CONNECTED));
					payloadBuf.release();
					return token;
				}
				if (BuildConfig.LOGGING && enableLogging) log(TAG + "publish()->not connected, held until the next CONNACK.");
			}
		} catch (Exception e) {
			log(TAG + "Exception caught while creating message publish" + e.getStackTrace().toString());
		}

		// Send on the channel's event loop, where a lost connection and CONNACK are also handled,
		// so the publish cannot slip between them. If the client connected while the publish
		// was being held, this also sends it.
		Channel ch = this.channel;
		if (ch != null && pendingPublish != null) {
			final MqttPendingPublish toSend = pendingPublish;
			final boolean connectedWhenMade = connected;
			if (ch.eventLoop().inEventLoop()) {
				sendPublish(ch, toSend, connectedWhenMade);
			} else {
				ch.eventLoop().execute(() -> sendPublish(ch, toSend, connectedWhenMade));
			}
		}
		return token;
	}

	/**
	 * Sends a publish on a channel, on that channel's event loop. A QoS 0 publish completes once
	 * written, or fails if the channel can no longer be used. A QoS 1 or 2 publish is sent and
	 * retransmitted until acknowledged; if the channel cannot be used, or the publish has already
	 * been sent or has ended, it is left for the next CONNACK.
	 * <p>
	 * A publish made while connected may use its channel while it is open, even if a disconnect
	 * has begun since: the publish was queued on the event loop first, so it is written before
	 * DISCONNECT. A publish made while disconnected may only use the client's current channel
	 * once CONNACK has been received.
	 *
	 * @param ch                the channel the publish was made on
	 * @param publish           the publish
	 * @param connectedWhenMade whether the client was connected when the publish was made
	 */
	private void sendPublish(Channel ch, MqttPendingPublish publish, boolean connectedWhenMade) {
		boolean usable = ch.isActive() && (connectedWhenMade || (ch == this.channel && mqttConnected.get()));
		MqttDeliveryToken token = publish.getToken();
		if (publish.getQos() == MqttQoS.AT_MOST_ONCE) {
			if (!usable) {
				token.markFailed(new MqttException(MqttException.REASON_CODE_CLIENT_NOT_CONNECTED));
				publish.getPayload().release();
				return;
			}
			if (BuildConfig.LOGGING && enableLogging) log(TAG + "publish()->sending QoS 0 PUBLISH");
			publish.setSent(true);
			// Complete once written to the socket, as Paho does, so the caller sees back-pressure.
			ch.writeAndFlush(publish.getMessage()).addListener((ChannelFutureListener) f -> {
				if (!f.isSuccess()) {
					token.markFailed(f.cause());
					return;
				}
				token.markComplete();
				MqttCallback handler = this.getDefaultCallbackHandler(); // single read, null-checked
				if (handler != null) {
					runMessageCallback(() -> {
						try {
							MqttCallbackEvents.deliveryComplete(handler, token);
						} catch (Exception ex) {
							log(TAG + "invokeDeliveryCompleteForOutgoingPublish()->exception in user callback: " + ex);
						}
					});
				}
			});
			return;
		}
		if (!usable || publish.isSent() || pendingPublishes.get(publish.getMessageId()) != publish) {
			return;
		}
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "publish()->sending PUBLISH, QoS=" + publish.getQos());
		publish.setSent(true);
		ch.writeAndFlush(publish.getMessage()).addListener((ChannelFutureListener) f -> {
			if (!f.isSuccess() && ch.isActive()) {
				// Not caused by a lost connection, which fails the publish itself.
				if (BuildConfig.LOGGING && enableLogging) log(TAG + "publish()->failure.cause->" + f.cause());
				endPublish(publish, f.cause());
			}
		});
		publish.startPublishRetransmissionTimer(this.eventLoop.next(), this::sendAndFlushPacket, clientConfig.getRetransmissionInterval());
	}

	/**
	 * Ends a QoS 1 or 2 publish that has failed: forgets it, stops its retransmission, releases
	 * its payload and fails its token. Does nothing if the publish has already ended.
	 *
	 * @param publish the publish
	 * @param cause   why it failed
	 */
	private void endPublish(MqttPendingPublish publish, Throwable cause) {
		endPublish(publish, cause, true);
	}

	/**
	 * Ends a QoS 1 or 2 publish that has failed, as {@link #endPublish(MqttPendingPublish, Throwable)}
	 * does, optionally keeping its stored copy for a later client.
	 *
	 * @param publish the publish
	 * @param cause   why it failed
	 * @param forget  whether to remove it from the session store too
	 */
	private void endPublish(MqttPendingPublish publish, Throwable cause, boolean forget) {
		if (!pendingPublishes.remove(publish.getMessageId(), publish)) {
			return;
		}
		if (forget) {
			forgetOutgoing(publish.getMessageId());
		}
		publish.onPubackReceived();
		publish.onPubcompReceived();
		publish.getPayload().release();
		publish.getToken().markFailed(cause);
	}

	/**
	 * Ends the work in flight on a connection that has closed. With
	 * {@link MqttConnectOptions#setCleanSession(boolean) cleanSession} {@code false}, the QoS 1
	 * and 2 publishes sent on it belong to the session and are kept, with retransmission
	 * stopped, to be sent again after the next CONNACK (see
	 * {@link #sendSessionPublishes(Channel)}), and so are incoming QoS 2 publishes awaiting
	 * PUBREL. With a clean session they fail with {@code cause}, and incoming QoS 2 state is
	 * dropped. Every pending subscribe and unsubscribe fails with {@code cause}, and QoS 1 and 2
	 * publishes that were never sent stay held either way.
	 *
	 * @param cause the reason: {@link MqttException#REASON_CODE_CONNECTION_LOST}, or
	 *              {@link MqttException#REASON_CODE_CLIENT_DISCONNECTING} after a disconnect,
	 *              or {@link MqttException#REASON_CODE_CLIENT_CLOSED} once the client is closing
	 */
	void failInFlight(Throwable cause) {
		boolean keepSession = !clientConfig.isCleanSession();
		for (MqttPendingPublish publish : new ArrayList<>(pendingPublishes.values())) {
			if (publish.isSent()) {
				if (keepSession) {
					publish.suspend();
				} else {
					endPublish(publish, cause);
				}
			}
		}
		for (MqttIncomingQos2Publish incoming : qos2PendingIncomingPublishes.values()) {
			if (keepSession) {
				incoming.onPubrelReceived();   // stops PUBREC retransmission; kept for the PUBREL
			} else {
				incoming.discard();
				forgetIncoming(incoming.getIncomingPublish().variableHeader().packetId());
			}
		}
		if (!keepSession) {
			qos2PendingIncomingPublishes.clear();
		}
		for (Integer id : new ArrayList<>(pendingSubscriptions.keySet())) {
			MqttPendingSubscription subscription = pendingSubscriptions.remove(id);
			if (subscription != null) {
				subscription.onSubackReceived();
				subscription.getToken().tryFailure(cause);
			}
		}
		for (Integer id : new ArrayList<>(pendingServerUnsubscribes.keySet())) {
			MqttPendingUnsubscription unsubscription = pendingServerUnsubscribes.remove(id);
			if (unsubscription != null) {
				unsubscription.onUnsubackReceived();
				unsubscription.getToken().tryFailure(cause);
			}
		}
	}

	/**
	 * Sends the QoS 1 and 2 publishes waiting for a connection, after CONNACK, in the order they
	 * were made, and starts their retransmission. Publishes held while disconnected are sent as
	 * they are. Publishes kept from a previous connection of the session are sent again with
	 * their original packet identifiers, as MQTT requires: a PUBLISH with the DUP flag set, or
	 * the PUBREL if the broker had already answered with PUBREC. If this connection uses a clean
	 * session, publishes kept from a previous one fail with
	 * {@link MqttException#REASON_CODE_CONNECTION_LOST}, as the broker no longer has their
	 * session. Runs on the channel's event loop.
	 *
	 * @param channel the newly connected channel
	 */
	void sendSessionPublishes(Channel channel) {
		boolean cleanSession = clientConfig.isCleanSession();
		List<MqttPendingPublish> publishes = new ArrayList<>(pendingPublishes.values());
		publishes.sort(Comparator.comparingLong(MqttPendingPublish::getSequence));
		for (MqttPendingPublish publish : publishes) {
			if (publish.isSent() || pendingPublishes.get(publish.getMessageId()) != publish) {
				continue;
			}
			if (publish.wasEverSent() && cleanSession) {
				endPublish(publish, new MqttException(MqttException.REASON_CODE_CONNECTION_LOST));
				continue;
			}
			io.netty.handler.codec.mqtt.MqttMessage pubrel = publish.getPubrelMessage();
			if (pubrel != null) {
				if (BuildConfig.LOGGING && enableLogging) log(TAG + "sendSessionPublishes()->resending PUBREL id=" + publish.getMessageId());
				channel.write(new io.netty.handler.codec.mqtt.MqttMessage(pubrel.fixedHeader(), pubrel.variableHeader()));
				publish.setSent(true);
				publish.startPubrelRetransmissionTimer(this.eventLoop.next(), this::sendAndFlushPacket, clientConfig.getRetransmissionInterval());
			} else {
				if (BuildConfig.LOGGING && enableLogging) log(TAG + "sendSessionPublishes()->sending PUBLISH id=" + publish.getMessageId()
						+ (publish.wasEverSent() ? " again" : ""));
				channel.write(publish.wasEverSent() ? publish.newDuplicate() : publish.getMessage());
				publish.setSent(true);
				publish.startPublishRetransmissionTimer(this.eventLoop.next(), this::sendAndFlushPacket, clientConfig.getRetransmissionInterval());
			}
		}
		channel.flush();
	}

	/**
	 * Fails every QoS 1 and 2 publish still pending, sent or held, with {@code cause}. Used
	 * when the client is closed, after which nothing will send them.
	 *
	 * @param cause the reason
	 */
	private void failAllPublishes(Throwable cause) {
		for (MqttPendingPublish publish : new ArrayList<>(pendingPublishes.values())) {
			endPublish(publish, cause, false);   // stored copies are kept for a later client
		}
	}

	/**
	 * Opens the session store, if there is one, when connecting. A clean session clears it;
	 * otherwise, the first time, the publishes and incoming QoS 2 messages it holds from an
	 * earlier client are loaded, in the order they were made, to be sent again after CONNACK
	 * (see {@link #sendSessionPublishes(Channel)}).
	 */
	private void openSession() {
		if (store == null) {
			return;
		}
		store.open(clientConfig.getClientId(), getServerURI());
		if (clientConfig.isCleanSession()) {
			store.clear();
			return;
		}
		if (sessionRestored) {
			return;
		}
		sessionRestored = true;
		for (SessionStore.Outgoing o : store.outgoing()) {
			if (!pendingPublishes.containsKey(o.packetId)) {
				restoreOutgoing(o);
			}
		}
		for (SessionStore.Incoming i : store.incoming()) {
			if (!qos2PendingIncomingPublishes.containsKey(i.packetId)) {
				qos2PendingIncomingPublishes.put(i.packetId, new MqttIncomingQos2Publish(
						new MqttPublishMessage(
								new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.EXACTLY_ONCE, i.retained, 0),
								new MqttPublishVariableHeader(i.topic, i.packetId),
								Unpooled.wrappedBuffer(i.payload)),
						new io.netty.handler.codec.mqtt.MqttMessage(
								new MqttFixedHeader(MqttMessageType.PUBREC, false, MqttQoS.AT_MOST_ONCE, false, 0),
								MqttMessageIdVariableHeader.from(i.packetId)),
						true));
			}
		}
	}

	/**
	 * Recreates a publish stored by an earlier client, with a new token, as one sent in an
	 * earlier connection of the session, so it is sent again with DUP set, or its PUBREL is.
	 *
	 * @param o the stored publish
	 */
	private void restoreOutgoing(SessionStore.Outgoing o) {
		MqttQoS qos = qosFromInt(o.qos);
		ByteBuf payloadBuf = Unpooled.wrappedBuffer(o.payload);
		MqttPublishMessage publishMsg = MqttMessageBuilders.publish()
				.topicName(o.topic)
				.payload(payloadBuf.duplicate())
				.qos(qos)
				.retained(o.retained)
				.messageId(o.packetId)
				.build();
		publishMsg.payload().release();   // only the headers are used: a resend copies payloadBuf
		MqttMessage message = new MqttMessage(o.payload);
		message.setQos(o.qos);
		message.setRetained(o.retained);
		MqttDeliveryToken token = new MqttDeliveryToken(this, o.packetId, new String[]{o.topic},
				new DefaultPromise<>(eventLoop.next()), this::runCallback);
		token.setMessage(message);
		MqttPendingPublish publish = new MqttPendingPublish(o.packetId, token, payloadBuf.retain(), publishMsg, qos,
				clientConfig.getMaxInflightRetries());
		publish.setSent(true);   // in an earlier connection
		publish.suspend();
		if (o.pubrel) {
			publish.setPubrelMessage(new io.netty.handler.codec.mqtt.MqttMessage(
					MqttPubRelFixedHeader.getFixedHeader(), MqttMessageIdVariableHeader.from(o.packetId)));
		}
		pendingPublishes.put(o.packetId, publish);
	}

	/**
	 * Stores, or updates, an outgoing QoS 1 or 2 publish in the session store, if any.
	 *
	 * @param publish the publish
	 */
	void storeOutgoing(MqttPendingPublish publish) {
		if (store != null) {
			store.putOutgoing(publish);
		}
	}

	/**
	 * Removes an outgoing publish from the session store, if any.
	 *
	 * @param packetId its packet identifier
	 */
	void forgetOutgoing(int packetId) {
		if (store != null) {
			store.removeOutgoing(packetId);
		}
	}

	/**
	 * Stores an incoming QoS 2 message awaiting PUBREL in the session store, if any.
	 *
	 * @param message the PUBLISH
	 */
	void storeIncoming(MqttPublishMessage message) {
		if (store != null) {
			store.putIncoming(message.variableHeader().packetId(), message.variableHeader().topicName(),
					message.fixedHeader().isRetain(), ByteBufUtil.getBytes(message.payload()));
		}
	}

	/**
	 * Removes an incoming QoS 2 message from the session store, if any.
	 *
	 * @param packetId its packet identifier
	 */
	void forgetIncoming(int packetId) {
		if (store != null) {
			store.removeIncoming(packetId);
		}
	}


	/**
	 * Sends a PINGREQ to the broker, if the channel is active.
	 */
	public void sendPing() {
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "sendPing()->Start");
		MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PINGREQ, false, MqttQoS.AT_MOST_ONCE, false, 0);
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "sendPing()->Send PINGREQ for " + getClientID());
		sendAndFlushPacket(new io.netty.handler.codec.mqtt.MqttMessage(fixedHeader));
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "sendPing()->End");
	}


	/**
	 * Converts a numeric QoS to {@link MqttQoS}. Wherever the client takes a QoS as an
	 * {@code int}, a value MQTT does not have means QoS 0.
	 *
	 * @param qos the QoS level
	 * @return the matching QoS; values other than 1 and 2 map to {@link MqttQoS#AT_MOST_ONCE}
	 */
	private static MqttQoS qosFromInt(int qos) {
		switch (qos) {
			case 1:
				return MqttQoS.AT_LEAST_ONCE;
			case 2:
				return MqttQoS.EXACTLY_ONCE;
			default:
				return MqttQoS.AT_MOST_ONCE;
		}
	}

	// Caller-supplied ByteBuf is consumed here — copied out and released,
// consistent with "method takes ownership" for a public API taking a ByteBuf.

	/**
	 * Copies the readable bytes of a buffer and releases it.
	 *
	 * @param payload the buffer, which is released by this call
	 * @return a copy of the readable bytes
	 */
	private static byte[] byteBufToBytes(ByteBuf payload) {
		try {
			byte[] bytes = new byte[payload.readableBytes()];
			payload.getBytes(payload.readerIndex(), bytes);
			return bytes;
		} finally {
			payload.release();
		}
	}


	/**
	 * {@inheritDoc}
	 * <p>
	 * While a connection attempt is in progress, a new default options object is returned
	 * instead, so changes made to it have no effect.
	 */
	@Override
	public MqttConnectOptions getOptions() {
		if (isConnecting()) {
			// We don't want the user to modify the connection options while a connection attempt is in flight.
			return new MqttConnectOptions(); // dummy object to avoid NPE
			//but settings are ignored.
		}
		return clientConfig;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setOptions(MqttConnectOptions options) {
		if (options != null) {
			this.clientConfig = options;
		}
	}

	/**
	 * Returns the connection options, even while a connection attempt is in progress.
	 *
	 * @return the client config
	 */
	MqttConnectOptions getClientConfig() {
		// Internal method which bypasses the isConnecting guard.
		return clientConfig;
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Also cancels any scheduled reconnect or connection attempt in progress.
	 */
	@Override
	public IMqttToken disconnect() {
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "disconnect()->");
		userdisconnected = true;

		cancelScheduledReconnect();
		// cancelConnectionAttempt() closes the channel, so on an established connection it
		// would stop DISCONNECT from being sent, and the broker would publish the will.
		// disconnect(long, ...) cancels an attempt itself when there is no active channel.
		Channel currentChannel = this.channel;
		if (!(this.mqttConnected.get() && currentChannel != null && currentChannel.isActive())) {
			cancelConnectionAttempt();
		}
		return disconnect(defaultQuiesceTimeout(), null, null);
	}

	/**
	 * Returns the quiesce timeout used by the disconnect methods that take none: Paho's in
	 * {@link MqttConnectOptions#enablePahoMode(boolean) Paho mode}, otherwise none.
	 *
	 * @return the timeout in milliseconds, or 0 not to quiesce
	 */
	long defaultQuiesceTimeout() {
		MqttConnectOptions config = this.clientConfig;
		return config != null && config.isPahoMode() ? MqttConnectOptions.PAHO_QUIESCE_TIMEOUT : 0;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IMqttToken disconnect(long quiesceTimeout) {
		return disconnect(quiesceTimeout, null, null);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IMqttToken disconnect(Object userContext, IMqttActionListener callback) {
		return disconnect(defaultQuiesceTimeout(), userContext, callback);
	}


	/**
	 * Disconnects, waiting up to the configured time to wait, and then optionally releases
	 * client resources.
	 *
	 * @param deletecallback    whether to clear the callback after disconnecting
	 * @param deleteoptions     whether to reset the connection options after disconnecting
	 * @param shutdowneventloop whether to let go of the event loop after disconnecting: the
	 *                          shared group is shut down once no client uses it, and a group set
	 *                          with {@link #setEventLoop(EventLoopGroup)} is left running
	 * @throws MqttException if the disconnect fails or times out
	 */
	public void disconnect(boolean deletecallback, boolean deleteoptions, boolean shutdowneventloop) throws MqttException {
		disconnect(0, null, null).waitForCompletion(timeToWait); // was: disconnect() with no wait
		if (deletecallback) {
			this.callback = null;
		}
		if (deleteoptions) {
			this.clientConfig.reset();
		}
		if (shutdowneventloop) {
			releaseEventLoop();
		}
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * With a positive {@code quiesceTimeout} the client quiesces first, as Paho does: new
	 * publishes, subscribes and unsubscribes are refused with
	 * {@link MqttException#REASON_CODE_CLIENT_DISCONNECTING}, and DISCONNECT is sent once every
	 * sent QoS 1 and 2 publish has been acknowledged, every incoming QoS 2 message has completed
	 * and the callbacks already queued have run, or once {@code quiesceTimeout} milliseconds have
	 * passed, whichever is first. With {@code 0} or less, DISCONNECT is sent at once. This method
	 * does not block either way.
	 * <p>
	 * If there is no active channel, any connection attempt in progress is cancelled and the
	 * token completes immediately.
	 */
	@Override
	public IMqttToken disconnect(long quiesceTimeout,
								 Object userContext,
								 IMqttActionListener callback) {

		if (BuildConfig.LOGGING && enableLogging) log(TAG + "disconnect()->quiesceTimeout=" + quiesceTimeout);

		/*
		 * This is an explicit user/application disconnect.
		 * Set this BEFORE cancelling anything so that any concurrently
		 * executing reconnect task sees the user's intent.
		 */
		userdisconnected = true;

		/*
		 * Stop automatic reconnect machinery first.
		 */
		cancelScheduledReconnect();

		Promise<Void> promise =
				new DefaultPromise<>(this.eventLoop.next());

		MqttToken<Void> token =
				newToken(promise);

		token.setUserContext(userContext);
		token.setActionCallback(callback);

		hasEverConnected = false;

		Channel currentChannel = this.channel;

		if (currentChannel == null || !currentChannel.isActive()) {

			/*
			 * There isn't an active MQTT connection.
			 *
			 * This is also the case where connect() is currently in
			 * progress but TCP hasn't produced a channel yet.
			 */
			cancelConnectionAttempt();

			isConnected(false);
			token.trySuccess(null);

			return token;
		}

		quiesceThen(currentChannel, quiesceTimeout, () -> sendDisconnectAndClose(currentChannel, token));

		return token;
	}

	/**
	 * Quiesces for up to {@code quiesceTimeout} milliseconds, then runs {@code then}; with a
	 * timeout of 0 or less, runs it at once. While quiescing, new work is refused (see
	 * {@link #checkNotQuiescing()}); {@code then} must clear the flag once the channel closes.
	 *
	 * @param channel        the channel being disconnected
	 * @param quiesceTimeout the longest time to wait, in milliseconds
	 * @param then           what to do once quiesced or out of time
	 */
	private void quiesceThen(Channel channel, long quiesceTimeout, Runnable then) {
		if (quiesceTimeout <= 0) {
			then.run();
			return;
		}
		quiescing = true;
		AtomicBoolean callbacksDrained = new AtomicBoolean(false);
		runCallback(() -> callbacksDrained.set(true));   // runs after callbacks already queued
		pollQuiesced(channel, callbacksDrained, System.currentTimeMillis() + quiesceTimeout, then);
	}

	/**
	 * Waits, without blocking, until the client has quiesced or the deadline has passed, then
	 * runs {@code then}. Quiesced means no sent QoS 1 or 2 publish awaits acknowledgement, no
	 * incoming QoS 2 message awaits PUBREL, and the callbacks queued when the disconnect began
	 * have run. Checks every {@value #QUIESCE_POLL_MILLIS} ms on the channel's event loop.
	 *
	 * @param channel          the channel being disconnected
	 * @param callbacksDrained set once the callbacks queued before the disconnect have run
	 * @param deadline         when to stop waiting, in milliseconds since the epoch
	 * @param then             what to do once quiesced or out of time
	 */
	private void pollQuiesced(Channel channel, AtomicBoolean callbacksDrained, long deadline, Runnable then) {
		boolean quiesced = callbacksDrained.get()
				&& qos2PendingIncomingPublishes.isEmpty()
				&& pendingPublishes.values().stream().noneMatch(MqttPendingPublish::isSent);
		if (quiesced || !channel.isActive() || System.currentTimeMillis() >= deadline) {
			if (BuildConfig.LOGGING && enableLogging) log(TAG + "pollQuiesced()->quiesced=" + quiesced);
			then.run();
			return;
		}
		channel.eventLoop().schedule(() -> pollQuiesced(channel, callbacksDrained, deadline, then),
				QUIESCE_POLL_MILLIS, TimeUnit.MILLISECONDS);
	}

	/**
	 * Sends DISCONNECT and closes the channel, whether or not the send succeeds, then completes
	 * the disconnect token.
	 *
	 * @param currentChannel the channel to disconnect
	 * @param token          the disconnect token
	 */
	private void sendDisconnectAndClose(Channel currentChannel, MqttToken<Void> token) {
		io.netty.handler.codec.mqtt.MqttMessage message =
				new io.netty.handler.codec.mqtt.MqttMessage(
						new MqttFixedHeader(
								MqttMessageType.DISCONNECT,
								false,
								MqttQoS.AT_MOST_ONCE,
								false,
								0));

		ChannelFuture sendFuture =
				sendAndFlushPacket(message);
		if (sendFuture == null) {
			// The channel closed meanwhile, for example while quiescing; nothing to send.
			sendFuture = currentChannel.newSucceededFuture();
		}

		sendFuture.addListener(f -> {

			/*
			 * Regardless of whether sending DISCONNECT succeeded,
			 * close the channel.
			 */
			currentChannel.close().addListener(closeFuture -> {

				if (this.channel == currentChannel) {
					this.channel = null;
				}

				isConnected(false);
				quiescing = false;
				token.trySuccess(null);
			});
		});
	}

	/**
	 * Throws if a disconnect is quiescing the client, which refuses new work as Paho does.
	 *
	 * @throws MqttException with {@link MqttException#REASON_CODE_CLIENT_DISCONNECTING}
	 */
	private void checkNotQuiescing() throws MqttException {
		if (quiescing) {
			throw new MqttException(MqttException.REASON_CODE_CLIENT_DISCONNECTING);
		}
	}

	/**
	 * Closes the connection immediately without sending DISCONNECT, and disables automatic
	 * reconnection until the next explicit connect. Unlike Paho, which quiesces for up to 30 s
	 * and sends DISCONNECT first, this does neither; use {@link #disconnectForcibly(long, long)}
	 * for that. In {@link MqttConnectOptions#enablePahoMode(boolean) Paho mode} it does what Paho
	 * does: {@code disconnectForcibly(30000, 10000)}.
	 *
	 * @return outside Paho mode, a token which is already complete; in Paho mode, a token which
	 *         will be completed when the channel has closed
	 */
	@Override
	public IMqttToken disconnectForcibly() {
		if (defaultQuiesceTimeout() > 0) {
			return disconnectForcibly(defaultQuiesceTimeout(), MqttConnectOptions.PAHO_DISCONNECT_TIMEOUT);
		}
		return disconnectForcibly(0, 0, false);
	}

	/**
	 * Sends DISCONNECT, allowing up to {@code disconnectTimeout} for it to be written, then
	 * closes the connection whether or not it was, and disables automatic reconnection until
	 * the next explicit connect. As in Paho, this sends DISCONNECT; unlike Paho, it does not
	 * quiesce first (Paho waits up to 30 s), so use
	 * {@link #disconnectForcibly(long, long)} to quiesce. In
	 * {@link MqttConnectOptions#enablePahoMode(boolean) Paho mode} it quiesces for up to 30 s
	 * first, as Paho does.
	 *
	 * @param disconnectTimeout the time in milliseconds to allow for writing DISCONNECT; 0 or
	 *                          less waits for the write
	 * @return a token which will be completed when the channel has closed
	 */
	@Override
	public IMqttToken disconnectForcibly(long disconnectTimeout) {
		return disconnectForcibly(defaultQuiesceTimeout(), disconnectTimeout);
	}

	/**
	 * Quiesces for up to {@code quiesceTimeout}, as {@link #disconnect(long)} does, then sends
	 * DISCONNECT, allowing up to {@code disconnectTimeout} for it to be written, then closes the
	 * connection whether or not it was. Disables automatic reconnection until the next explicit
	 * connect. This matches Paho.
	 *
	 * @param quiesceTimeout    the time in milliseconds to allow in-flight work to finish; 0 or
	 *                          less does not quiesce
	 * @param disconnectTimeout the time in milliseconds to allow for writing DISCONNECT; 0 or
	 *                          less waits for the write
	 * @return a token which will be completed when the channel has closed
	 */
	@Override
	public IMqttToken disconnectForcibly(long quiesceTimeout,
										 long disconnectTimeout) {
		return disconnectForcibly(quiesceTimeout, disconnectTimeout, true);
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * On an established connection, a positive {@code quiesceTimeout} quiesces first, as
	 * {@link #disconnect(long)} does. Without {@code sendDisconnectPacket} or quiescing, the
	 * token is already complete when this returns; otherwise it completes once the channel has
	 * closed.
	 */
	@Override
	public IMqttToken disconnectForcibly(long quiesceTimeout,
										 long disconnectTimeout,
										 boolean sendDisconnectPacket) {

		if (BuildConfig.LOGGING && enableLogging) log(TAG + "disconnectForcibly()->quiesceTimeout=" + quiesceTimeout
				+ " disconnectTimeout=" + disconnectTimeout + " sendDisconnectPacket=" + sendDisconnectPacket);

		Channel currentChannel = this.channel;

		/*
		 * Quiescing and sending DISCONNECT only apply to an established MQTT connection;
		 * otherwise the channel, if any, is simply closed.
		 */
		boolean established = currentChannel != null && currentChannel.isActive() && this.mqttConnected.get();
		boolean sendFirst = sendDisconnectPacket && established;
		boolean quiesceFirst = quiesceTimeout > 0 && established;

		/*
		 * Explicit user/application disconnect.
		 */
		userdisconnected = true;

		/*
		 * Stop future automatic reconnects.
		 */
		cancelScheduledReconnect();

		/*
		 * Stop an outstanding connect attempt. This also closes the channel, so it is skipped
		 * when the channel is kept open to quiesce or to write DISCONNECT.
		 */
		if (!sendFirst && !quiesceFirst) {
			cancelConnectionAttempt();
		}

		Promise<Void> promise =
				new DefaultPromise<>(this.eventLoop.next());

		MqttToken<Void> token =
				newToken(promise);

		isConnected(false);
		hasEverConnected = false;

		if (sendFirst || quiesceFirst) {
			currentChannel.closeFuture().addListener(f -> {
				if (this.channel == currentChannel) {
					this.channel = null;
				}
				quiescing = false;
				token.trySuccess(null);
			});
			quiesceThen(currentChannel, quiesceFirst ? quiesceTimeout : 0, () -> {
				if (!sendFirst || !currentChannel.isActive()) {
					currentChannel.close();
					return;
				}
				/*
				 * Close once DISCONNECT has been written, or after disconnectTimeout if the write
				 * has not finished by then, whichever comes first.
				 */
				io.netty.handler.codec.mqtt.MqttMessage message =
						new io.netty.handler.codec.mqtt.MqttMessage(
								new MqttFixedHeader(MqttMessageType.DISCONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0));
				ScheduledFuture<?> timeout = (disconnectTimeout > 0)
						? currentChannel.eventLoop().schedule(() -> {
							currentChannel.close();
						}, disconnectTimeout, TimeUnit.MILLISECONDS)
						: null;
				currentChannel.writeAndFlush(message).addListener(f -> {
					if (timeout != null) {
						timeout.cancel(false);
					}
					currentChannel.close();
				});
			});
			return token;
		}

		if (currentChannel != null) {
			try {
				currentChannel.close();
			} catch (Exception ex) {
				log(TAG
						+ "disconnectForcibly()->exception closing channel: "
						+ ex);
			}
		}

		token.trySuccess(null);

		return token;
	}


	/**
	 * Cancel an in-progress connection attempt.
	 * <p>
	 * This does not cancel a scheduled reconnect. It only deals with a
	 * connection attempt which has already reached connect().
	 */
	private void cancelConnectionAttempt() {
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "cancelConnectionAttempt()->start");

		Promise<MqttConnectResult> future = currentConnectFuture;
		Channel currentChannel = this.channel;

		/*
		 * If the connection future hasn't completed, this represents an
		 * active connection attempt (possibly TCP connected but waiting
		 * for CONNACK).
		 */
		if (future != null && !future.isDone()) {

			if (BuildConfig.LOGGING && enableLogging) log(TAG + "cancelConnectionAttempt()->failing connect future");

			ChannelClosedException e =
					new ChannelClosedException(
							"Connection attempt cancelled",
							true);

			future.tryFailure(e);
		}

		/*
		 * Closing the channel also handles the case where TCP has connected
		 * but CONNACK hasn't arrived yet.
		 */
		if (currentChannel != null && currentChannel.isOpen()) {
			if (BuildConfig.LOGGING && enableLogging) log(TAG + "cancelConnectionAttempt()->closing channel");

			try {
				currentChannel.close();
			} catch (Exception ex) {
				log(TAG + "cancelConnectionAttempt()->exception closing channel: "
						+ ex);
			}
		}

		/*
		 * Don't leave a reference to a channel which we're deliberately
		 * killing.
		 *
		 * The identity check prevents an old cancellation from clearing
		 * a newer channel.
		 */
		if (this.channel == currentChannel) {
			this.channel = null;
		}

		isConnected(false);

		if (BuildConfig.LOGGING && enableLogging) log(TAG + "cancelConnectionAttempt()->end");
	}


	/**
	 * Cancel a scheduled automatic reconnect.
	 */
	private void cancelScheduledReconnect() {
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "cancelScheduledReconnect()->start");
		reconnecting = false;

		// A connect moving through several brokers stops too, and fails.
		Promise<MqttConnectResult> walk = serverWalk;
		serverWalk = null;
		ScheduledFuture<?> next = serverWalkFuture;
		if (next != null) {
			next.cancel(false);
		}
		if (walk != null) {
			walk.tryFailure(new MqttException(closing
					? MqttException.REASON_CODE_CLIENT_CLOSED
					: MqttException.REASON_CODE_CLIENT_DISCONNECTING));
		}

		Future<?> future = rescheduleFuture;

		if (future != null) {
			if (BuildConfig.LOGGING && enableLogging) log(TAG + "cancelScheduledReconnect()->cancelling scheduled reconnect");

			future.cancel(false);

			/*
			 * Only clear our reference if it is still the same Future.
			 */
			if (rescheduleFuture == future) {
				rescheduleFuture = null;
			}
		}

		/*
		 * A cancellation of the scheduled Future does not necessarily
		 * prevent run() if the Runnable has already started. The
		 * userdisconnected check in run() therefore remains necessary.
		 */

		if (BuildConfig.LOGGING && enableLogging) log(TAG + "cancelScheduledReconnect()->end");
	}


	/**
	 * Resets the connection options to their defaults.
	 *
	 * @throws MqttException if the options cannot be reset
	 */
	public void resetConnectionoptions() throws MqttException {
		this.clientConfig.reset();
	}

	/**
	 * Disconnect from the broker and destroy the MqttClient: waits up to 30 seconds for the
	 * disconnect, clears the callback, resets the options, shuts down the callback executor and
	 * lets go of the event loop: the shared group is shut down once no client uses it, and a
	 * group set with {@link #setEventLoop(EventLoopGroup)} is left running. The client cannot
	 * be used afterwards.
	 *
	 * @throws MqttException if the disconnect fails or times out
	 */
	public void shutdown() throws MqttException {
		shutdown(false);
	}

	/**
	 * Destroys the client, as {@link #shutdown()} does, optionally without a clean disconnect.
	 * With {@code force}, the connection is closed at once without DISCONNECT, even in
	 * {@link MqttConnectOptions#enablePahoMode(boolean) Paho mode}, and the client's resources
	 * are released even if closing fails. Without it, this is {@link #shutdown()}: a clean
	 * disconnect with {@link #disconnect()}, waiting up to 30 seconds (plus the quiesce time in
	 * Paho mode), then the release.
	 *
	 * @param force whether to close the connection forcibly
	 * @throws MqttException if a clean disconnect fails or times out
	 */
	public void shutdown(boolean force) throws MqttException {
		closing = true;
		if (force) {
			try {
				disconnectForcibly(0, 0, false);
			} finally {
				releaseResources();
			}
			return;
		}
		long timeToWait = 30_000 + defaultQuiesceTimeout();
		disconnect().waitForCompletion(timeToWait);
		releaseResources();
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * An alias of {@link #shutdown()}, for Paho compatibility.
	 */
	@Override
	public void close() throws MqttException {
		shutdown();
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * An alias of {@link #shutdown(boolean)}, for Paho compatibility.
	 */
	@Override
	public void close(boolean force) throws MqttException {
		shutdown(force);
	}

	/**
	 * Fails the QoS 1 and 2 publishes still pending, kept for the session or held while
	 * disconnected, with {@link MqttException#REASON_CODE_CLIENT_CLOSED}, clears the callback,
	 * resets the options, shuts down the callback executor and lets go of the event loop (see
	 * {@link #releaseEventLoop()}). The client cannot be used afterwards.
	 */
	private void releaseResources() {
		failAllPublishes(new MqttException(MqttException.REASON_CODE_CLIENT_CLOSED));
		qos2PendingIncomingPublishes.values().forEach(MqttIncomingQos2Publish::discard);
		qos2PendingIncomingPublishes.clear();
		if (store != null) {
			store.close();   // what it holds is kept for a later client
		}
		this.callback = null;
		this.clientConfig.reset();
		// Let queued callbacks finish; later ones run on the after-close thread.
		callbackExecutor.shutdown();
		multithreadedcallbackExecutor.shutdown();
		releaseEventLoop();
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Ignored while a connection attempt is in progress.
	 */
	@Override
	public void setCallback(MqttCallback callback) {
		if (isConnecting()) {
			return;
		}
		this.callback = callback;
	}

	/**
	 * Sets the broker host. Ignored while a connection attempt is in progress.
	 *
	 * @param host the host name or IP address
	 */
	public void setHost(String host) {
		if ((this.clientConfig != null) && (!isConnecting())) {
			this.clientConfig.setHost(host);
		}
	}


	/**
	 * Sets the broker port. Ignored while a connection attempt is in progress.
	 *
	 * @param port the TCP port
	 */
	public void setPort(int port) {
		if ((this.clientConfig != null) && (!isConnecting())) {
			this.clientConfig.setPort(port);
		}
	}

	/**
	 * Sets the user name sent in CONNECT. Ignored while a connection attempt is in progress.
	 *
	 * @param username the user name
	 */
	public void setUserName(String username) {
		if ((this.clientConfig != null) && (!isConnecting())) {
			this.clientConfig.setUserName(username);
		}
	}

	/**
	 * Sets the password sent in CONNECT. Ignored while a connection attempt is in progress.
	 *
	 * @param password the password
	 */
	public void setPassword(String password) {
		if ((this.clientConfig != null) && (!isConnecting())) {
			this.clientConfig.setPassword(password);
		}
	}

	/**
	 * Sets the password sent in CONNECT. Ignored while a connection attempt is in progress.
	 *
	 * @param password the password
	 */
	public void setPassword(char[] password) {
		if ((this.clientConfig != null) && (!isConnecting())) {
			this.clientConfig.setPassword(password);
		}
	}

	/**
	 * Sets the client identifier. Ignored while a connection attempt is in progress.
	 *
	 * @param clientid the client identifier
	 */
	public void setClientID(String clientid) {
		if ((this.clientConfig != null) && (!isConnecting())) {
			this.clientConfig.setClientId(clientid);
		}
	}

	/**
	 * Sets whether to start a clean session. Ignored while a connection attempt is in progress.
	 *
	 * @param cleansession {@code true} to discard any previous session
	 */
	public void setCleanSession(boolean cleansession) {
		if ((this.clientConfig != null) && (!isConnecting())) {
			this.clientConfig.setCleanSession(cleansession);
		}
	}

	/**
	 * Sets the MQTT protocol version. Ignored while a connection attempt is in progress.
	 *
	 * @param version the protocol version
	 */
	public void setMqttVersion(MqttVersion version) {
		if ((this.clientConfig != null) && (!isConnecting())) {
			this.clientConfig.setMqttVersion(version);
		}
	}

	/**
	 * Sets the MQTT protocol version from Paho's numeric values; see
	 * {@link MqttConnectOptions#setMqttVersion(int)}. Ignored while a connection attempt is in
	 * progress.
	 *
	 * @param version the protocol level: 0, 3 or 4
	 * @throws IllegalArgumentException if the value is not one of those
	 */
	public void setMqttVersion(int version) {
		if ((this.clientConfig != null) && (!isConnecting())) {
			this.clientConfig.setMqttVersion(version);
		}
	}

	/**
	 * Sets the keep-alive interval. Ignored while a connection attempt is in progress.
	 *
	 * @param keepaliveinterval the interval in seconds
	 */
	public void setKeepAliveInterval(int keepaliveinterval) {
		if ((this.clientConfig != null) && (!isConnecting())) {
			this.clientConfig.setKeepAliveInterval(keepaliveinterval);
		}
	}

	/**
	 * Sets the connection timeout. Ignored while a connection attempt is in progress.
	 *
	 * @param connectiontimeout the timeout in seconds
	 */
	public void setConnectionTimeout(int connectiontimeout) {
		if ((this.clientConfig != null) && (!isConnecting())) {
			this.clientConfig.setConnectionTimeout(connectiontimeout);
		}
	}

	/**
	 * Sets whether to reconnect automatically after the connection is lost. Ignored while a connection attempt is in progress.
	 *
	 * @param autoreconnect {@code true} to enable automatic reconnect
	 */
	public void setAutomaticReconnect(boolean autoreconnect) {
		if ((this.clientConfig != null) && (!isConnecting())) {
			this.clientConfig.setAutomaticReconnect(autoreconnect);
		}
	}

	/**
	 * Sets the wait before the first automatic reconnect attempt; later waits double, up to the
	 * maximum reconnect delay. Ignored while a connection attempt is in progress.
	 *
	 * @param retryinterval the initial delay in seconds
	 */
	public void setRetryInterval(int retryinterval) {
		if ((this.clientConfig != null) && (!isConnecting())) {
			this.clientConfig.setRetryInterval(retryinterval);
		}
	}

	/**
	 * Sets the maximum number of automatic reconnect attempts. Ignored while a connection attempt is in progress.
	 *
	 * @param maxretries the maximum, or {@code -1} for no limit
	 */
	public void setMaxRetries(int maxretries) {
		if ((this.clientConfig != null) && (!isConnecting())) {
			this.clientConfig.setMaxRetries(maxretries);
		}
	}

	/**
	 * Returns whether the current or most recent connection attempt is an automatic reconnect.
	 *
	 * @return {@code true} for an automatic reconnect
	 */
	public boolean isReconnect() {
		return reconnect;
	}


	/**
	 * Handles an exception thrown from a message callback. If
	 * {@link MqttConnectOptions#setMessageArrivedErrorsTriggerDisconnect(boolean)} is enabled, the
	 * connection is closed without sending DISCONNECT, as Paho does; the close is then reported
	 * through {@link MqttCallback#connectionLost(Throwable)} with an {@link MqttException} whose
	 * cause is the callback's exception. Otherwise this does nothing.
	 *
	 * @param error the exception the callback threw
	 */
	void messageArrivedFailed(Throwable error) {
		MqttConnectOptions config = this.clientConfig;
		if (config == null || !config.isMessageArrivedErrorsTriggerDisconnect()) {
			return;
		}
		Channel current = this.channel;
		if (current == null || !current.isActive()) {
			return;   // already closing, for example after an earlier callback error
		}
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "messageArrivedFailed()->closing connection after callback error: " + error);
		closeError = new MqttException(error);
		current.close();
	}

	/**
	 * Closes a connection on which a packet from the broker could not be decoded: a malformed
	 * or not allowed packet (for example a topic containing U+0000, a QoS of 3, a QoS 1 or 2
	 * packet identifier of 0, an unknown packet type), or one larger than
	 * {@link MqttConnectOptions#setMaxBytesInMessage(int)}. Netty's decoder discards everything
	 * after such a packet, so the connection could not be used any more; without this it would
	 * stay open, deaf, until keep-alive gave up. The loss (or, before CONNACK, the failed
	 * connect) is reported with {@link MqttException#REASON_CODE_INVALID_MESSAGE}, whose cause
	 * is the decoder's exception.
	 *
	 * @param channel the channel the packet came on
	 * @param cause   the decoder's exception
	 */
	void decodeFailed(Channel channel, Throwable cause) {
		// Recorded even if the channel is already closing: Netty's WebSocket decoder closes it
		// itself before the exception reaches the pipeline's end.
		if (channel == this.channel && closeError == null && !channel.closeFuture().isDone()) {
			closeError = new MqttException(MqttException.REASON_CODE_INVALID_MESSAGE, cause);
		}
		if (channel.isActive()) {
			log(TAG + "decodeFailed()->closing the connection: " + cause);
			channel.close();
		}
	}

	/**
	 * Notifies the callback, on the calling thread, that the connection was lost.
	 *
	 * @param cause the reason the connection was lost
	 */
	protected void connectionLost(Throwable cause) {
		if (callback != null) {
			MqttCallbackEvents.connectionLost(callback, cause, getServerURI(), classuuid);
		}
	}

	/**
	 * Notifies the callback, on the calling thread, that a connection attempt failed.
	 *
	 * @param reason a description of the failure
	 */
	protected void connectionFailed(String reason) {
		if (callback != null) {
			MqttCallbackEvents.connectFailed(callback, getServerURI(), reason, classuuid);
		}
	}

	/**
	 * Resets the retry count and notifies the callback, on the callback executor, that the
	 * connection is complete.
	 *
	 * @param reconnect whether this was an automatic reconnect
	 */
	protected void onConnectComplete(boolean reconnect) {
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "onConnectComplete()->reconnect=" + reconnect);
		MqttCallback cb;
		cb = this.callback; // snapshot — avoid a race if setCallback() runs concurrently
		if (cb == null) {
			if (BuildConfig.LOGGING && enableLogging) log(TAG + "onConnectComplete()->returning due to missing callback handler");
			return;
		}
		connectRetryCount.set(0);
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "onConnectComplete()->sending to callback handler");
		boolean cleanSession = clientConfig.isCleanSession();
		String serverURI = getServerURI();
		runCallback(() -> {
			try {
				MqttCallbackEvents.connectComplete(cb, reconnect, cleanSession, serverURI, classuuid);
			} catch (Exception ex) {
				log(TAG + "onConnectComplete()->exception in callback: " + ex);
			}
		});
	}


	/**
	 * Notifies the callback, on the callback executor, that a connection attempt failed.
	 *
	 * @param reason a description of the failure
	 */
	protected void onConnectFailed(String reason) {
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "onConnectFailed()");
		MqttCallback cb;
		cb = this.callback; // snapshot — avoid a race if setCallback() runs concurrently
		if (cb == null) {
			if (BuildConfig.LOGGING && enableLogging) log(TAG + "onConnectFailed()->returning due to missing callback handler");
			return;
		}
		String serverURI = getServerURI();
		runCallback(() -> {
			try {
				MqttCallbackEvents.connectFailed(cb, serverURI, reason, classuuid);
			} catch (Exception ex) {
				log(TAG + "onConnectFailed()->exception in callback: " + ex);
			}
		});
	}


	/**
	 * Removes the given topics from the broker-side subscription set after an UNSUBACK, and
	 * returns topics that still have local handlers even though the broker no longer has them.
	 *
	 * @param topics the topics acknowledged as unsubscribed
	 * @return the topics that still have local handlers
	 */
	List<String> onTopicsUnsubscribed(List<String> topics) {
		List<String> orphaned = new ArrayList<>();
		synchronized (subscribeLock) {
			for (String topic : topics) {
				serverSubscriptions.remove(topic);   // broker has dropped it, whatever the local state
				if (!subscriptions.get(topic).isEmpty()) {
					orphaned.add(topic);
				}
			}
		}
		return orphaned;
	}

	/**
	 * Resets the retry count and notifies the callback, on the callback executor, that
	 * automatic reconnection has failed.
	 *
	 * @param serverURI the URI of the broker
	 * @param reason    a description of the failure
	 */
	protected void onReconnectFailed(String serverURI, String reason) {
		MqttCallback cb = this.callback;
		if (cb == null) {
			return;
		}
		connectRetryCount.set(0);
		runCallback(() -> {
			try {
				MqttCallbackEvents.reconnectFailed(cb, serverURI, reason, classuuid);
			} catch (Exception ex) {
				log(TAG + "onReconnectFailed()->exception in callback: " + ex);
			}
		});
	}

	/**
	 * Records that an automatic reconnect has succeeded, so {@link #isReconnecting()} is
	 * {@code false} again, and with several brokers that the current one has connected, so its
	 * failures are forgotten. Called on CONNACK, whether or not a callback is set.
	 */
	void reconnected() {
		reconnecting = false;
		serverWalk = null;
		ServerFailover servers = failover;
		if (servers != null) {
			servers.connected();
		}
	}

	/**
	 * Resets the retry count and notifies the callback, on the callback executor, that the
	 * connection has been recovered.
	 */
	protected void onSuccessfulReconnect() {
		MqttCallback cb = this.callback;
		if (cb != null) {
			connectRetryCount.set(0);
			boolean cleanSession = clientConfig.isCleanSession();
			String serverURI = getServerURI();
			runCallback(() -> {
				try {
					MqttCallbackEvents.successfulReconnect(cb, cleanSession, serverURI, classuuid);
				} catch (Exception ex) {
					log(TAG + "onSuccessfulReconnect()->exception in callback: " + ex);
				}
			});
		}
		this.rescheduleFuture = null;
	}

	/**
	 * Sets whether callbacks run on a single thread, preserving message order (the default),
	 * or on a cached thread pool.
	 *
	 * @param preserve {@code true} to deliver callbacks in order on one thread
	 */
	public void preserveMessageOrdering(boolean preserve) {
		this.preserveMessageOrdering = preserve;
	}


	/**
	 * Sends a SUBSCRIBE unless it has been sent already. If there is no usable channel yet, it is
	 * left for the CONNACK handler to send. The claim on the pending subscription makes sure the
	 * subscribing thread and the CONNACK handler, which may run at the same moment right after a
	 * connect, do not both send it.
	 *
	 * @param pending the pending subscription
	 * @param message its SUBSCRIBE
	 */
	private void sendSubscribe(MqttPendingSubscription pending, Object message) {
		if (pending.claimSend() && this.sendAndFlushPacket(message) == null) {
			pending.setSent(false);   // not connected: CONNACK sends it
		}
	}

	/**
	 * Writes and flushes a packet to the channel.
	 *
	 * @param message the packet to send
	 * @return the write future, or {@code null} if there is no active channel
	 */
	ChannelFuture sendAndFlushPacket(Object message) {
		if (this.channel == null || !channel.isActive()) {
			return null;
		}

		if (this.channel.isActive()) {
			if (BuildConfig.LOGGING && enableLogging) log(TAG + "sendAndFlushPacket()-> about to write Message");
			return this.channel.writeAndFlush(message);
		}
		return this.channel.newFailedFuture(new ChannelClosedException(TAG + "sendAndFlushPacket()->Channel is closed!"));
	}

	/**
	 * Allocates the next packet identifier, wrapping from 65535 back to 1 and skipping any
	 * identifier still awaiting acknowledgement.
	 *
	 * @return a variable header carrying the identifier
	 * @throws MqttException with {@link MqttException#REASON_CODE_NO_MESSAGE_IDS_AVAILABLE} if
	 *                       all 65535 identifiers are in use
	 */
	private MqttMessageIdVariableHeader getNewMessageId() throws MqttException {
		synchronized (nextMessageId) {
			for (int tried = 0; tried < MqttConnectOptions.MAX_INFLIGHT_LIMIT; tried++) {
				int id = nextMessageId.get();
				nextMessageId.set(id >= MqttConnectOptions.MAX_INFLIGHT_LIMIT ? 1 : id + 1);
				if (!isMessageIdInUse(id)) {
					return MqttMessageIdVariableHeader.from(id);
				}
			}
		}
		throw new MqttException(MqttException.REASON_CODE_NO_MESSAGE_IDS_AVAILABLE);
	}

	/**
	 * Returns whether a packet identifier is held by a publish, subscribe or unsubscribe that is
	 * still awaiting acknowledgement, so that it is not reused when the identifiers wrap round.
	 *
	 * @param id the packet identifier
	 * @return {@code true} if the identifier is in use
	 */
	private boolean isMessageIdInUse(int id) {
		return pendingPublishes.containsKey(id)
				|| pendingSubscriptions.containsKey(id)
				|| pendingServerUnsubscribes.containsKey(id);
	}


	/**
	 * Forgets the state of a connection that has ended. Subscribes in progress have been failed
	 * (see {@link #failInFlight(Throwable)}), so their topics are forgotten. The subscriptions
	 * themselves, and the listeners given with them, are kept when
	 * {@link MqttConnectOptions#setCleanSession(boolean) cleanSession} is {@code false}: the
	 * broker keeps the subscriptions with the session and sends their messages after the next
	 * connect, as Paho keeps its listeners. With a clean session the broker drops them, so they
	 * are forgotten here, and the application subscribes again.
	 */
	void connectionEnded() {
		pendingSubscribeTopics.clear();
		MqttConnectOptions config = clientConfig;
		if (config == null || config.isCleanSession()) {
			clearSubscriptions();
		}
	}

	/**
	 * Forgets every subscription and the listeners given with them: on a clean session, or when
	 * CONNACK reports that the broker no longer has the session.
	 */
	void clearSubscriptions() {
		serverSubscriptions.clear();
		subscriptions.clear();
		handlerToSubscription.clear();
	}

	/**
	 * Re-sends a SUBSCRIBE for every local subscription, if a resubscribe has been requested.
	 * The request flag is cleared so this runs only once per request.
	 */
	protected void resubscribe() {

		if (BuildConfig.LOGGING && enableLogging) log(TAG + "resubscribe()->");
		if (!resubscribe) {
			return;
		}
		resubscribe = false;

		for (Map.Entry<String, MqttSubscription> subiter : subscriptions.entries()) {

			String topic = subiter.getKey();
			MqttSubscription sub = subiter.getValue();
			boolean once = sub.isOnce();
			MqttQoS qos = sub.getQOS();
			MqttCallback handler = sub.getHandler();


			if (BuildConfig.LOGGING && enableLogging) log(TAG + "Resubscribe->topic=" + topic);

			Promise<Void> promise = new DefaultPromise<>(this.eventLoop.next());
			MqttToken<Void> token = newToken(promise);

			MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.SUBSCRIBE, false, MqttQoS.AT_LEAST_ONCE, false, 0);
			MqttTopicSubscription subscription = new MqttTopicSubscription(topic, qos);
			MqttMessageIdVariableHeader variableHeader;
			try {
				variableHeader = getNewMessageId();
			} catch (MqttException e) {
				log(TAG + "Resubscribe->no packet identifier free for " + topic + ": " + e);
				continue;   // every identifier is awaiting acknowledgement; try the remaining topics
			}
			token.setMessageId(variableHeader.messageId());
			MqttSubscribePayload payload = new MqttSubscribePayload(Collections.singletonList(subscription));
			MqttSubscribeMessage message = new MqttSubscribeMessage(fixedHeader, variableHeader, payload);


			final MqttPendingSubscription pendingSubscription = new MqttPendingSubscription(token, topic, message, qos);
			pendingSubscription.addHandler(handler, once);
			this.pendingSubscriptions.put(variableHeader.messageId(), pendingSubscription);
			this.pendingSubscribeTopics.put(topic, qos.value());
			sendSubscribe(pendingSubscription, message);   // if not sent now, CONNACK sends it

			pendingSubscription.startRetransmitTimer(this.eventLoop.next(), this::sendAndFlushPacket, clientConfig.getRetransmissionInterval());

		}
	}

	/**
	 * Returns the unacknowledged SUBSCRIBEs, keyed by packet identifier.
	 *
	 * @return the pending subscriptions
	 */
	Map<Integer, MqttPendingSubscription> getPendingSubscriptions() {
		return pendingSubscriptions;
	}

	/**
	 * Returns the local subscriptions, keyed by topic filter.
	 *
	 * @return the subscriptions
	 */
	Multimap<String, MqttSubscription> getSubscriptions() {
		return subscriptions;
	}

	/**
	 * Returns the topic filters with an unacknowledged SUBSCRIBE.
	 *
	 * @return the pending subscribe topics
	 */
	Set<String> getPendingSubscribeTopicsSet() {
		return pendingSubscribeTopics.keySet();
	}


	/**
	 * Returns the topic filters with an unacknowledged SUBSCRIBE, mapped to the requested QoS.
	 *
	 * @return the pending subscribe topics and their QoS
	 */
	Map<String, Integer> getPendingSubscribeTopics() {
		return pendingSubscribeTopics;
	}

	/**
	 * Returns how long to wait before the next automatic reconnect attempt: the retry interval,
	 * doubled after each failed attempt since the last successful connection, and capped at
	 * {@link MqttConnectOptions#getMaxReconnectDelay()}.
	 *
	 * @return the delay in milliseconds
	 */
	long reconnectDelayMillis() {
		ServerFailover servers = failover;
		if (servers != null) {
			return servers.delayMillis();
		}
		return reconnectDelayMillis(clientConfig.getRetryInterval(), connectRetryCount.get(),
				clientConfig.getMaxReconnectDelay());
	}

	/**
	 * Returns how long to wait before an automatic reconnect attempt.
	 *
	 * @param retryIntervalSeconds the wait before the first attempt, in seconds
	 * @param attempt              the attempt number since the last successful connection,
	 *                             starting at 1; 0 is treated as 1
	 * @param maxDelayMillis       the longest wait, in milliseconds
	 * @return the retry interval doubled {@code attempt - 1} times, capped at
	 *         {@code maxDelayMillis}, in milliseconds
	 */
	static long reconnectDelayMillis(long retryIntervalSeconds, int attempt, long maxDelayMillis) {
		long initial = Math.max(0L, retryIntervalSeconds) * 1000L;
		long delay = Math.min(initial, maxDelayMillis);
		for (int i = 1; i < attempt && delay < maxDelayMillis; i++) {
			delay = (delay > maxDelayMillis / 2) ? maxDelayMillis : delay * 2;   // never overflows
		}
		return delay;
	}


	/**
	 * Creates a token for an action of this client, whose callbacks run on the callback executor.
	 *
	 * @param promise the promise tracking the action
	 * @param <V>     the type of result the promise completes with
	 * @return a new token whose {@link MqttToken#getClient()} is this client
	 */
	private <V> MqttToken<V> newToken(Promise<V> promise) {
		MqttToken<V> token = new MqttToken<>(promise, this::runCallback);
		token.setClient(this);
		return token;
	}

	/**
	 * Returns this client's keep-alive ping tracker.
	 *
	 * @return the tracker
	 */
	KeepAliveTracker getKeepAliveTracker() {
		return keepAliveTracker;
	}

	/**
	 * Returns the local subscriptions, keyed by callback.
	 *
	 * @return the handler to subscription
	 */
	Multimap<MqttCallback, MqttSubscription> getHandlerToSubscription() {
		return handlerToSubscription;
	}

	/**
	 * Returns the topic filters the broker has acknowledged.
	 *
	 * @return the server subscriptions
	 */
	Set<String> getServerSubscriptionsSet() {
		return serverSubscriptions.keySet();
	}

	/**
	 * Returns the topic filters the broker has acknowledged, mapped to the granted QoS.
	 *
	 * @return the server subscriptions and their QoS
	 */
	Map<String, Integer> getServerSubscriptions() {
		return serverSubscriptions;

	}


	/**
	 * Returns the unacknowledged UNSUBSCRIBEs, keyed by packet identifier.
	 *
	 * @return the pending server unsubscribes
	 */
	Map<Integer, MqttPendingUnsubscription> getPendingServerUnsubscribes() {
		return pendingServerUnsubscribes;
	}

	/**
	 * Returns the unacknowledged QoS 1 and 2 publishes, keyed by packet identifier.
	 *
	 * @return the pending publishes
	 */
	Map<Integer, MqttPendingPublish> getPendingPublishes() {
		return pendingPublishes;
	}


	// on MqttAsyncClient — this is where it belongs, since pendingPublishes lives here

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getInFlightMessageCount() {
		return pendingPublishes.size();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IMqttDeliveryToken[] getPendingDeliveryTokens() {
		return pendingPublishes.values().stream()
				.map(MqttPendingPublish::getToken)   // <-- pull the token out of the wrapper
				.toArray(IMqttDeliveryToken[]::new);
	}

	/**
	 * Returns incoming QoS 2 publishes awaiting PUBREL, keyed by packet identifier.
	 *
	 * @return the qos 2 pending incoming publishes
	 */
	Map<Integer, MqttIncomingQos2Publish> getQos2PendingIncomingPublishes() {
		return qos2PendingIncomingPublishes;
	}


	/**
	 * Returns the callback set on this client.
	 *
	 * @return the default callback handler, or {@code null}
	 */
	public MqttCallback getDefaultCallbackHandler() {
		return callback;
	}

	/**
	 * Promise for one connection attempt. Completing it, by any path, first clears the
	 * client's connecting flag, so the flag is already false when waiters and listeners are
	 * notified. A stale attempt that completes after a newer one has started leaves the flag
	 * alone.
	 */
	private final class ConnectPromise extends DefaultPromise<MqttConnectResult> {

		private final AtomicBoolean finished = new AtomicBoolean(false);

		/**
		 * Creates a connect promise.
		 *
		 * @param executor the executor used to notify listeners
		 */
		ConnectPromise(EventExecutor executor) {
			super(executor);
		}

		/**
		 * Clears the connecting flag the first time the attempt finishes, if this is still
		 * the current attempt.
		 */
		private void finish() {
			if (finished.compareAndSet(false, true) && currentConnectFuture == this) {
				if (BuildConfig.LOGGING && enableLogging) log(TAG + "connect()->connectFuture completing");
				isConnecting(false);
			}
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public boolean trySuccess(MqttConnectResult result) {
			if (isDone()) {
				return false;
			}
			finish();
			return super.trySuccess(result);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public boolean tryFailure(Throwable cause) {
			if (isDone()) {
				return false;
			}
			finish();
			return super.tryFailure(cause);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public Promise<MqttConnectResult> setSuccess(MqttConnectResult result) {
			finish();
			return super.setSuccess(result);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public Promise<MqttConnectResult> setFailure(Throwable cause) {
			finish();
			return super.setFailure(cause);
		}
	}

	/**
	 * Builds the channel pipeline for a connection: optional proxy and TLS handlers, optional
	 * WebSocket handlers, the MQTT codec, idle-state and ping handlers, and the
	 * {@link MqttChannelHandler}.
	 */
	private class MqttChannelInitializer extends ChannelInitializer<SocketChannel> {

		private final MqttAsyncClient client;

		private final Promise<MqttConnectResult> connectFuture;
		private final String host;
		private final int port;
		private final URI uri;
		private final SslContext sslContext;

		private final ProxyHandler proxyHandler;

		private final Consumer<SSLEngine> sslEngineConsumer;

		private final boolean httpsHostnameVerification;

		private final HostnameVerifier hostnameVerifier;

		private boolean proxyAdded = false;
		private final boolean websocket;
		private final boolean secure;

		private final String subprotocol = "mqtt";


		/**
		 * Creates a channel initializer from the client's connection options.
		 *
		 * @param client        the client that owns the connection
		 * @param connectFuture the future completed when CONNACK is received
		 * @param config        the connection options
		 */
		public MqttChannelInitializer(MqttAsyncClient client, Promise<MqttConnectResult> connectFuture,
									  MqttConnectOptions config) {
			this.client = client;
			this.connectFuture = connectFuture;
			this.host = config.getHost();
			this.port = config.getPort();
			this.uri = config.getServerURI();
			this.proxyHandler = config.getProxy();
			this.sslContext = config.getSslContext();
			this.sslEngineConsumer = config.getSslEngineConsumer();
			this.httpsHostnameVerification = config.isHttpsHostnameVerificationEnabled();
			this.hostnameVerifier = config.getSSLHostnameVerifier();
			this.websocket = config.isWebSocket();
			this.secure = config.isSecure();
		}


		/**
		 * Runs the verifier set with {@link MqttConnectOptions#setSSLHostnameVerifier(HostnameVerifier)}
		 * on a completed handshake, unless HTTPS hostname verification is on.
		 *
		 * @param engine the engine whose handshake has completed
		 * @return the reason to reject the connection, or {@code null} to accept it
		 */
		private SSLPeerUnverifiedException verifyHostname(SSLEngine engine) {
			if (hostnameVerifier == null || httpsHostnameVerification) {
				return null;
			}
			SSLSession session = engine.getSession();
			try {
				if (hostnameVerifier.verify(host, session)) {
					return null;
				}
			} catch (RuntimeException e) {
				SSLPeerUnverifiedException unverified = new SSLPeerUnverifiedException(
						"Host: " + host + ", Peer Host: " + session.getPeerHost());
				unverified.initCause(e);
				session.invalidate();
				return unverified;
			}
			session.invalidate();
			return new SSLPeerUnverifiedException("Host: " + host + ", Peer Host: " + session.getPeerHost());
		}

		/**
		 * Adds the handlers to a new channel's pipeline.
		 *
		 * @param ch the new channel
		 * @throws Exception if a handler cannot be created
		 */
		@Override
		protected void initChannel(SocketChannel ch) throws Exception {
			// Set the channel here, on the event loop and before CONNECT can be sent. The connect
			// listener also sets it, but when the TCP connect completes at once (loopback on
			// Linux) Netty runs that listener later as a task, and CONNACK can complete the
			// connect token first, leaving channel null and isConnected() false.
			client.channel = ch;
			if ((!proxyAdded) && (proxyHandler != null)) {
				if (BuildConfig.LOGGING && enableLogging) log(TAG + "MqttChannelInitializer()->initChannel() added proxyhandler");
				ch.pipeline().addFirst("proxy", proxyHandler);
				proxyAdded = true;
			} else {
				if (BuildConfig.LOGGING && enableLogging) log(TAG + "MqttChannelInitializer()->initChannel() proxyhandler is null");
			}

			SslContext context = sslContext;
			if (context == null && secure) {
				// A TLS scheme with no context set: use the JVM's default, as Paho does, so the
				// javax.net.ssl.* system properties apply rather than connecting without TLS.
				try {
					context = MqttConnectOptions.wrapSslContext(SSLContext.getDefault());
				} catch (NoSuchAlgorithmException e) {
					connectFuture.tryFailure(new ChannelClosedException("Default TLS context unavailable", e, true));
					throw e;
				}
			}
			if (context != null) {
				if (BuildConfig.LOGGING && enableLogging) log(TAG + "MqttChannelInitializer()->initChannel() added sslcontext");
				SSLEngine engine = context.newEngine(ch.alloc(), host, port);
				if (this.httpsHostnameVerification) {
					SSLParameters params = engine.getSSLParameters();
					params.setEndpointIdentificationAlgorithm("HTTPS");
					engine.setSSLParameters(params);
				}
				if (this.sslEngineConsumer != null) {
					this.sslEngineConsumer.accept(engine);
				}
				SslHandler sslHandler = new SslHandler(engine);
				sslHandler.setHandshakeTimeoutMillis(40000);
				// Fail the connect attempt with the real TLS error (for example a hostname
				// verification failure). Without this the caller only sees "Channel closed before
				// CONNACK received", and over WebSockets HttpResponseHandler swallows the exception.
				sslHandler.handshakeFuture().addListener(f -> {
					if (!f.isSuccess() && !connectFuture.isDone()) {
						ChannelClosedException e = new ChannelClosedException("TLS handshake failed", f.cause(), true);
						if (connectFuture.tryFailure(e)) {
							MqttAsyncClient.this.connectionLost(e);
						}
					} else if (f.isSuccess()) {
						// As in Paho, the custom verifier only runs when the HTTPS check is off.
						SSLPeerUnverifiedException unverified = verifyHostname(engine);
						if (unverified != null) {
							// Fail the connect before closing: closing fails the queued CONNECT
							// (it is never sent), which would otherwise be reported instead.
							ChannelClosedException e = new ChannelClosedException("TLS hostname verification failed", unverified, true);
							boolean failed = connectFuture.tryFailure(e);
							ch.close();
							if (failed) {
								MqttAsyncClient.this.connectionLost(e);
							}
						}
					}
				});
				if (proxyAdded) {
					// The proxy handler must stay nearest the socket: its handshake goes out in
					// the clear, and TLS then runs inside the tunnel to the broker. The proxy
					// handler holds the ClientHello until the tunnel is up.
					ch.pipeline().addAfter("proxy", "ssl", sslHandler);
				} else {
					ch.pipeline().addFirst("ssl", sslHandler);
				}
				if (BuildConfig.LOGGING && enableLogging) log("Adding ssl support to pipeline ");
			} else {
				if (BuildConfig.LOGGING && enableLogging) log(TAG + "MqttChannelInitializer()->initChannel() sslcontext is null");
			}

			if ((!proxyAdded) && (proxyHandler != null)) {
				ch.pipeline().addFirst("proxy", proxyHandler);
				proxyAdded = true;
				if (BuildConfig.LOGGING && enableLogging) log("Adding proxy support to pipeline " + clientConfig.getProxyHost());
			}

			if (websocket) {
				if (BuildConfig.LOGGING && enableLogging) log(TAG + "MqttChannelInitializer()->Adding websocket handlers to pipeline");
				if (BuildConfig.LOGGING && enableLogging) log(TAG + "MqttChannelInitializer()->URI=" + this.uri.toString());
				ch.pipeline().addLast(new HttpClientCodec(512, 512, 512));
				ch.pipeline().addLast(new HttpObjectAggregator(8192));
				ch.pipeline().addLast(new WebSocketClientCompressionHandler(0));
				DefaultHttpHeaders upgradeHeaders = new DefaultHttpHeaders();
				for (Map.Entry<String, String> header : client.clientConfig.getCustomWebSocketHeaders().entrySet()) {
					upgradeHeaders.set(header.getKey(), header.getValue());
				}
				// A WebSocket frame may carry a whole MQTT packet, so frames up to MQTT's largest
				// packet (plus five bytes of fixed header) are accepted, and the MQTT decoder
				// applies setMaxBytesInMessage, as on TCP. Netty's frame decoder would close the
				// connection on a larger frame without the client learning why.
				ch.pipeline().addLast(new WebSocketClientProtocolHandler(WebSocketClientHandshakerFactory.newHandshaker(
						this.uri, WebSocketVersion.V13, subprotocol, true, upgradeHeaders,
						MqttConnectOptions.MAX_PACKET_SIZE + 5)));
				ch.pipeline().addLast(new MqttWebSocketCodec());
				ch.pipeline().addLast(new HttpResponseHandler());
			}

			if (client.clientConfig.isFlushConsolidationEnabled()) {
				// Placed after TLS and WebSocket, which then see fewer flushes, and before the MQTT
				// handlers, whose flushes it combines.
				ch.pipeline().addLast("flushConsolidation", new FlushConsolidationHandler(
						FlushConsolidationHandler.DEFAULT_EXPLICIT_FLUSH_AFTER_FLUSHES, true));
			}
			ch.pipeline().addLast("mqttDecoder", new MqttDecoder(client.clientConfig.getMaxBytesInMessage())); //https://github.com/thingsboard/thingsboard/issues/2565
			ch.pipeline().addLast("mqttEncoder", MqttEncoder.INSTANCE);
			ch.pipeline().addLast("idleStateHandler", new IdleStateHandler(client.clientConfig.getKeepAliveInterval(), client.clientConfig.getKeepAliveInterval(), client.clientConfig.getKeepAliveMax()));
			ch.pipeline().addLast("mqttPingHandler", new MqttPingHandler(client, client.clientConfig.getKeepAliveInterval()));
			channelHandler = new MqttChannelHandler(client, connectFuture);
			ch.pipeline().addLast("mqttHandler", channelHandler);

		}
	}//MqttChannelInitializer

}
