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

import io.netty.channel.Channel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttVersion;
import io.netty.handler.proxy.ProxyHandler;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

/**
 * Connection options for the client: broker address, credentials, session and keep-alive
 * settings, TLS, proxy, WebSocket and automatic reconnect behaviour.
 * <p>
 * The broker address can be given as a URL with {@link #setServerURI(String)} or
 * {@link #setHost(String)}; it is parsed when the client connects. See
 * {@link #parseBrokerUrl()} for the supported schemes.
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public class MqttConnectOptions {
	private final String TAG = "MqttConnectOptions()->";
	private boolean enableLogging = false;

	private SimpleLogger logger = new SimpleLogger();

	private SslContext sslContext;

	private String randomClientId;

	private String clientId;
	private int timeoutSeconds = 5;

	private int keepaliveSeconds = 60;

	private int keepAliveMax = 0;

	private int readTimeoutSeconds = 70;

	private int maxBytesInMessage = MAX_PACKET_SIZE;

	private int maxReconnectDelay = 128000;

	private int maxInflightRetries = -1; // -1 = infinite, matches current behavior
	private int retransmissionInterval = 0; // seconds; 0 = resend only after a reconnect, as Paho

	private boolean skipPortDuringHandshake = false;

	private Map<String, String> customWebSocketHeaders = null;


	/** The default protocol version, MQTT 3.1.1. */
	public static final MqttVersion MQTT_VERSION_DEFAULT = MqttVersion.MQTT_3_1_1;

	/** MQTT 3.1. */
	public static final MqttVersion MQTT_VERSION_3_1 = MqttVersion.MQTT_3_1;

	/** MQTT 3.1.1. */
	public static final MqttVersion MQTT_VERSION_3_1_1 = MqttVersion.MQTT_3_1_1;

	/** MQTT 5. */
	public static final MqttVersion MQTT_VERSION_5 = MqttVersion.MQTT_5;

	/** The longest client identifier MQTT 3.1 allows, in characters. */
	public static final int MQTT_3_1_MAX_CLIENT_ID_LENGTH = 23;

	/**
	 * The largest in-flight window: the number of packet identifiers MQTT allows (1 to 65535).
	 * It is also the default for {@link #setMaxUnwrittenQos0(int)}.
	 */
	public static final int MAX_INFLIGHT_LIMIT = 65535;

	/**
	 * The default in-flight window for {@link #setMaxInflight(int)}. Paho's is 10, which Paho
	 * mode uses. Mosquitto slows down sharply with tens of thousands of QoS 2 messages in
	 * progress, so the default stays well below {@value #MAX_INFLIGHT_LIMIT}.
	 */
	public static final int DEFAULT_MAX_INFLIGHT = 1000;

	private int maxInflight = DEFAULT_MAX_INFLIGHT;

	private int maxUnwrittenQos0 = MAX_INFLIGHT_LIMIT;

	private MqttVersion protocolVersion = MQTT_VERSION_DEFAULT;

	/** The default clean session setting, {@code true}. */
	public static final boolean CLEAN_SESSION_DEFAULT = true;
	/** Resume any existing session. */
	public static final boolean CLEAN_SESSION_FALSE = false;
	/** Start a clean session. */
	public static final boolean CLEAN_SESSION_TRUE = true;


	private boolean tcp_nodelay = true;
	private boolean so_reuseaddr = true;
	private boolean so_keepalive = true;

	private String username = null;
	private String password = null;
	private boolean cleanSession = true;
	private MqttLastWill lastWill;

	private Class<? extends Channel> channelClass = NioSocketChannel.class;

	private SocketAddress bindAddress;

	private Consumer<SSLEngine> sslEngineConsumer = (engine) -> {
	};

	private boolean httpsHostnameVerificationEnabled = false;

	private HostnameVerifier hostnameVerifier;

	private ProxyHandler proxyHandler;

	private ProxyHandlerFactory proxyHandlerFactory;

	private boolean reconnect = false;
	private long retryInterval = 1L;

	private int maxRetries = 2;

	private boolean secure = false;

	private boolean isManualAcks = false;

	private boolean messageArrivedErrorsTriggerDisconnect = false;

	private boolean settingsParsed = false;

	private boolean pahoMode = false;

	private boolean flushConsolidation = false;

	/** Paho's connection timeout, in seconds. */
	public static final int PAHO_CONNECTION_TIMEOUT = 30;

	/** Paho's default in-flight window. */
	public static final int PAHO_MAX_INFLIGHT = 10;

	/** Paho's quiesce timeout for {@code disconnect()} and {@code disconnectForcibly()}, in milliseconds. */
	public static final long PAHO_QUIESCE_TIMEOUT = 30000;

	/** Paho's timeout for sending DISCONNECT in {@code disconnectForcibly()}, in milliseconds. */
	public static final long PAHO_DISCONNECT_TIMEOUT = 10000;

	private URI uri;

	private boolean websocket = false;
	private String webSocketBasePath = "mqtt";


	private boolean channellogging = false;
	private boolean pinglogging = false;

	private String host;

	/** The broker address as the user gave it; parsing always starts from this. */
	private String brokerUrl;

	/** A port set explicitly with {@link #setPort(int)}, overriding the URL's; -1 if none. */
	private int portOverride = -1;

	private int port = -1;

	private String[] serverURIs = null;


	/**
	 * Constructs a new <code>MqttConnectOptions</code> object using the default
	 * values.
	 * <p>
	 * The defaults are:
	 * <ul>
	 * <li>The keepalive interval is 60 seconds</li>
	 * <li>Clean Session is true</li>
	 * <li>The connection timeout period is 5 seconds</li>
	 * <li>The MQTT version is 3.1.1</li>
	 * <li>Automatic reconnect is off; when enabled, it retries every second, up to 2 times</li>
	 * <li>No Will message is set</li>
	 * <li>A random client ID of the form {@code netty-mqtt/XXXXXXXX} is generated</li>
	 * </ul>
	 * More information about these values can be found in the setter methods.
	 */
	public MqttConnectOptions() {
		generateRandomClientID();
	}

	/**
	 * Creates a copy of other options, for example to describe a second broker for an {@link
	 * MqttServerList} that differs from the first only in its address:
	 * <pre>{@code
	 * MqttConnectOptions b = new MqttConnectOptions(a);
	 * b.setServerURI("tcp://broker-b");
	 * b.setPort(1885);
	 * }</pre>
	 * Every setting is copied, including the client ID, credentials, Will, TLS, proxy and
	 * reconnect settings and Paho mode. Objects held by the options, such as the TLS context,
	 * Will, hostname verifier and proxy factory, are shared rather than copied; the WebSocket
	 * headers and server URIs are copied, so changing them on one does not change the other.
	 *
	 * @param other the options to copy
	 * @throws IllegalArgumentException if {@code other} is {@code null}
	 */
	public MqttConnectOptions(MqttConnectOptions other) {
		if (other == null) {
			throw new IllegalArgumentException("options to copy must not be null");
		}
		this.enableLogging = other.enableLogging;
		this.sslContext = other.sslContext;
		this.randomClientId = other.randomClientId;
		this.clientId = other.clientId;
		this.timeoutSeconds = other.timeoutSeconds;
		this.keepaliveSeconds = other.keepaliveSeconds;
		this.keepAliveMax = other.keepAliveMax;
		this.readTimeoutSeconds = other.readTimeoutSeconds;
		this.maxBytesInMessage = other.maxBytesInMessage;
		this.maxReconnectDelay = other.maxReconnectDelay;
		this.maxInflightRetries = other.maxInflightRetries;
		this.retransmissionInterval = other.retransmissionInterval;
		this.skipPortDuringHandshake = other.skipPortDuringHandshake;
		this.customWebSocketHeaders = other.customWebSocketHeaders == null
				? null
				: new HashMap<>(other.customWebSocketHeaders);
		this.maxInflight = other.maxInflight;
		this.maxUnwrittenQos0 = other.maxUnwrittenQos0;
		this.protocolVersion = other.protocolVersion;
		this.tcp_nodelay = other.tcp_nodelay;
		this.so_reuseaddr = other.so_reuseaddr;
		this.so_keepalive = other.so_keepalive;
		this.username = other.username;
		this.password = other.password;
		this.cleanSession = other.cleanSession;
		this.lastWill = other.lastWill;
		this.channelClass = other.channelClass;
		this.bindAddress = other.bindAddress;
		this.sslEngineConsumer = other.sslEngineConsumer;
		this.httpsHostnameVerificationEnabled = other.httpsHostnameVerificationEnabled;
		this.hostnameVerifier = other.hostnameVerifier;
		this.proxyHandler = other.proxyHandler;
		this.proxyHandlerFactory = other.proxyHandlerFactory;
		this.reconnect = other.reconnect;
		this.retryInterval = other.retryInterval;
		this.maxRetries = other.maxRetries;
		this.secure = other.secure;
		this.isManualAcks = other.isManualAcks;
		this.messageArrivedErrorsTriggerDisconnect = other.messageArrivedErrorsTriggerDisconnect;
		this.settingsParsed = other.settingsParsed;
		this.pahoMode = other.pahoMode;
		this.flushConsolidation = other.flushConsolidation;
		this.uri = other.uri;
		this.websocket = other.websocket;
		this.webSocketBasePath = other.webSocketBasePath;
		this.channellogging = other.channellogging;
		this.pinglogging = other.pinglogging;
		this.host = other.host;
		this.brokerUrl = other.brokerUrl;
		this.portOverride = other.portOverride;
		this.port = other.port;
		this.serverURIs = other.serverURIs == null ? null : other.serverURIs.clone();
	}

	/**
	 * Logs a message if logging is enabled for these options.
	 *
	 * @param data the message to log
	 */
	private void log(String data) {
		if (enableLogging) {
			logger.log(data);
		}
	}

	/**
	 * Logs a message with the given tag if logging is enabled for these options.
	 *
	 * @param TAG  the prefix written before the message
	 * @param info the message to log
	 */
	private void log(String TAG, String info) {
		if (!enableLogging) {
			return;
		}
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "->" + info);
	}

	/**
	 * Returns the password to use for the connection.
	 *
	 * @return the password to use for the connection.
	 */
	public char[] getPassword() {
		if (password != null) {
			return password.toCharArray();
		}
		return null;
	}

	/**
	 * Sets the password to use for the connection. A {@code null} password is ignored.
	 *
	 * @param password A Char Array of the password
	 * @throws IllegalArgumentException if the password encodes to more than 65535 bytes of
	 *                                  UTF-8, the most MQTT can carry
	 */
	public void setPassword(char[] password) {
		if (password != null) {
			String value = new String(password);
			MqttStrings.checkLength(value, "The password");
			this.password = value;
		}

	}


	/**
	 * Returns the password to use for the connection.
	 *
	 * @return the password, or {@code null} if none is set
	 */
	public String getPasswordString() {
		if (password != null) {
			return password;
		}
		return null;
	}

	/**
	 * Sets the password to use for the connection. A {@code null} or empty password is ignored.
	 *
	 * @param password the password
	 * @throws IllegalArgumentException if the password encodes to more than 65535 bytes of
	 *                                  UTF-8, the most MQTT can carry
	 */
	public void setPassword(String password) {
		if ((password != null) && (!password.isEmpty())) {
			MqttStrings.checkLength(password, "The password");
			this.password = password;
		}
	}

	/**
	 * Sets a header to send in the WebSocket upgrade request.
	 *
	 * @param header the header
	 * @param value  the header value
	 * @throws IllegalArgumentException if {@code header} is {@code null} or {@code value} is blank
	 */
	public void setCustomWebSocketHeader(WebSocketHeader header, String value) {
		if (header == null) {
			throw new IllegalArgumentException("header must not be null");
		}
		if ((value == null) || (value.trim().isEmpty())) {
			throw new IllegalArgumentException("value must not be null or empty");
		}
		if (customWebSocketHeaders == null) {
			customWebSocketHeaders = new HashMap<String, String>();
		}
		customWebSocketHeaders.put(header.headerName(), value);
	}

	/**
	 * Sets a header, by name, to send in the WebSocket upgrade request.
	 *
	 * @param header the header name; surrounding whitespace is trimmed
	 * @param value  the header value
	 * @throws IllegalArgumentException if {@code header} or {@code value} is blank
	 */
	public void setCustomWebSocketHeader(String header, String value) {
		if ((header == null) || (header.trim().isEmpty())) {
			throw new IllegalArgumentException("header must not be null or empty");
		}
		if ((value == null) || (value.trim().isEmpty())) {
			throw new IllegalArgumentException("value must not be null or empty");
		}
		if (customWebSocketHeaders == null) {
			customWebSocketHeaders = new HashMap<String, String>();
		}
		customWebSocketHeaders.put(header.trim(), value);
	}

	/**
	 * Sets every header to send in the WebSocket upgrade request, replacing any set before, as
	 * Paho's {@code setCustomWebSocketHeaders} does. The map is copied, so later changes to it
	 * have no effect. Headers can still be added afterwards with
	 * {@link #setCustomWebSocketHeader(String, String)}.
	 *
	 * @param headers header names to values; {@code null} or empty removes all custom headers.
	 *                Surrounding whitespace is trimmed from names
	 * @throws IllegalArgumentException if any name or value is blank; the headers are then left
	 *                                  unchanged
	 */
	public void setCustomWebSocketHeaders(Map<String, String> headers) {
		if (headers == null || headers.isEmpty()) {
			customWebSocketHeaders = null;
			return;
		}
		Map<String, String> copy = new HashMap<String, String>();
		for (Map.Entry<String, String> header : headers.entrySet()) {
			String name = header.getKey();
			String value = header.getValue();
			if ((name == null) || (name.trim().isEmpty())) {
				throw new IllegalArgumentException("header must not be null or empty");
			}
			if ((value == null) || (value.trim().isEmpty())) {
				throw new IllegalArgumentException("value for header " + name.trim() + " must not be null or empty");
			}
			copy.put(name.trim(), value);
		}
		customWebSocketHeaders = copy;
	}

	/**
	 * Removes a header set with {@link #setCustomWebSocketHeader(WebSocketHeader, String)}.
	 *
	 * @param header the header to remove; {@code null} is ignored
	 */
	public void removeCustomWebSocketHeader(WebSocketHeader header) {
		if (customWebSocketHeaders != null && header != null) {
			customWebSocketHeaders.remove(header.headerName());
		}
	}

	/**
	 * Returns the custom WebSocket headers.
	 *
	 * @return an unmodifiable map of header names to values; empty if none are set
	 */
	public Map<String, String> getCustomWebSocketHeaders() {
		return customWebSocketHeaders == null
				? Collections.<String, String>emptyMap()
				: Collections.unmodifiableMap(customWebSocketHeaders);
	}


	/**
	 * Resets the options. Not yet implemented; currently does nothing.
	 */
	protected void reset() {

	}

	/**
	 * Turns logging of keep-alive pings on or off.
	 *
	 * @param pingLogging whether to log pings
	 */
	public void enablePingLogging(boolean pingLogging) {
		this.pinglogging = pingLogging;
	}

	/**
	 * Returns whether keep-alive pings are logged.
	 *
	 * @return {@code true} if ping logging is enabled
	 */
	protected boolean getPingLogging() {
		return this.pinglogging;
	}

	/**
	 * Turns logging of MQTT protocol handling on or off.
	 *
	 * @param channelLogging whether to log channel activity
	 */
	public void enableChannelLogging(boolean channelLogging) {
		this.channellogging = channelLogging;
	}

	/**
	 * Returns whether MQTT protocol handling is logged.
	 *
	 * @return {@code true} if channel logging is enabled
	 */
	protected boolean getChannelLogging() {
		return this.channellogging;
	}


	/**
	 * Creates options with default values that connect over TLS.
	 *
	 * @param sslContext the Netty TLS context; {@code null} leaves TLS off
	 */
	public MqttConnectOptions(SslContext sslContext) {
		if (sslContext != null) {
			this.sslContext = sslContext;
			secure = true;
		}
		generateRandomClientID();
	}

	/**
	 * Creates options with default values that connect over TLS.
	 *
	 * @param sslContext the JDK TLS context, wrapped in a Netty context; {@code null} leaves TLS off
	 */
	public MqttConnectOptions(SSLContext sslContext) {
		if (sslContext != null) {
			this.sslContext = new JdkSslContext(sslContext, true, ClientAuth.NONE);
			secure = true;
		}
		generateRandomClientID();
	}

	/**
	 * Routes the connection through a SOCKS proxy.
	 *
	 * @param proxyHost the proxy host; surrounding whitespace is trimmed
	 * @param proxyPort the proxy port
	 * @param username  the proxy username, or {@code null}
	 * @param password  the proxy password, or {@code null}; ignored for SOCKS4
	 * @param socks5    {@code true} for SOCKS5, {@code false} for SOCKS4
	 */
	public void setProxy(String proxyHost, int proxyPort, String username, String password, boolean socks5) {

		if (socks5) {
			this.proxyHandlerFactory = new Socks5ProxyHandlerFactory(new InetSocketAddress(proxyHost.trim(), proxyPort), username, password).createProxyHandlerFactory();
		} else {
			this.proxyHandlerFactory = new Socks4ProxyHandlerFactory(new InetSocketAddress(proxyHost.trim(), proxyPort), username).createProxyHandlerFactory();
		}

	}

	/**
	 * Routes the connection through an HTTP {@code CONNECT} proxy.
	 *
	 * @param proxyHost the proxy host
	 * @param proxyPort the proxy port
	 * @param username  the proxy username, or {@code null}
	 * @param password  the proxy password, or {@code null}
	 * @param secure    currently has no effect
	 */
	public void setHTTPProxy(String proxyHost, int proxyPort, String username, String password, boolean secure) {

		if (secure) {
			this.proxyHandlerFactory = new HttpProxyHandlerFactory(new InetSocketAddress(proxyHost, proxyPort), username, password).createProxyHandlerFactory();
		} else {
			this.proxyHandlerFactory = new HttpProxyHandlerFactory(new InetSocketAddress(proxyHost, proxyPort), username, password).createProxyHandlerFactory();
		}
	}


	/**
	 * Sets the credentials for the proxy configured with {@link #setProxy} or {@link #setHTTPProxy}.
	 * <p>
	 * SOCKS4 has no password authentication, so supplying a password for a SOCKS4 proxy
	 * replaces it with a SOCKS5 proxy at the same address using these credentials.
	 *
	 * @param username the proxy username
	 * @param password the proxy password
	 * @throws IllegalStateException if no proxy has been configured
	 */
	public void setProxyAuth(String username, String password) {
		if (this.proxyHandlerFactory == null) {
			throw new IllegalStateException("setProxyAuth()->no proxy configured");
		}

		if ((this.proxyHandlerFactory.getProxyType() == ProxyType.SOCKS4) && (password != null)) {
			SocketAddress sa = this.proxyHandlerFactory.getSocketAddress();
			this.proxyHandlerFactory = new Socks5ProxyHandlerFactory(sa, username, password).createProxyHandlerFactory();
		} else {
			this.proxyHandlerFactory.setProxyAuth(username, password);
		}
	}

	/**
	 * Creates a proxy handler for a new connection. If the proxy cannot be reached, the
	 * connection attempt fails; it is never made without the proxy.
	 *
	 * @return a new proxy handler, or {@code null} if no proxy is configured
	 */
	public ProxyHandler getProxy() {
		if (proxyHandlerFactory == null) {
			if (BuildConfig.LOGGING && enableLogging) log(TAG + "getProxy() is null");
			return null;
		}
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "getProxy()->creating ProxyHandler");
		return this.proxyHandlerFactory.createProxyHandler();
	}

	/**
	 * Returns the broker host and port, as used in log messages. Despite the name, this is not
	 * the proxy address.
	 *
	 * @return the broker address in {@code host:port} form
	 */
	public String getProxyHost() {
		return this.host + ":" + this.port;
	}

	/**
	 * Generates a random client ID of the form {@code netty-mqtt/XXXXXXXX} and remembers it.
	 *
	 * @return the generated client ID
	 */
	private String generateRandomClientID() {
		String id = newRandomClientId();
		this.randomClientId = id;
		return id;
	}

	/**
	 * Returns a new random client ID of the form {@code netty-mqtt/XXXXXXXX}, where each
	 * {@code X} is a letter or digit. At 19 characters it also fits MQTT 3.1's limit of
	 * {@value #MQTT_3_1_MAX_CLIENT_ID_LENGTH}.
	 *
	 * @return the generated client ID
	 */
	static String newRandomClientId() {
		String chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
		StringBuilder id = new StringBuilder("netty-mqtt/");
		for (int i = 0; i < 8; i++) {
			id.append(chars.charAt(ThreadLocalRandom.current().nextInt(chars.length())));
		}
		return id.toString();
	}

	/**
	 * Returns the client identifier. If none is set, a new random ID is generated on every call.
	 *
	 * @return the client identifier
	 */
	public String getClientId() {
		if (clientId == null || clientId.isEmpty()) {
			return generateRandomClientID();
		}
		return clientId;
	}

	/**
	 * Returns the client identifier sent in CONNECT. This is {@link #getClientId()}, except that
	 * MQTT 3.1 allows at most {@value #MQTT_3_1_MAX_CLIENT_ID_LENGTH} characters, so with
	 * {@link #MQTT_VERSION_3_1} a longer identifier is truncated to its first
	 * {@value #MQTT_3_1_MAX_CLIENT_ID_LENGTH} (one fewer if the cut would split a surrogate
	 * pair). MQTT 3.1.1 identifiers are sent unchanged.
	 *
	 * @return the client identifier to send
	 */
	public String getConnectClientId() {
		String id = getClientId();
		if (protocolVersion == MqttVersion.MQTT_3_1 && id.length() > MQTT_3_1_MAX_CLIENT_ID_LENGTH) {
			int end = MQTT_3_1_MAX_CLIENT_ID_LENGTH;
			if (Character.isHighSurrogate(id.charAt(end - 1))) {
				end--;
			}
			id = id.substring(0, end);
		}
		return id;
	}

	/**
	 * Sets the client identifier. With {@link #MQTT_VERSION_3_1}, an identifier longer than
	 * {@value #MQTT_3_1_MAX_CLIENT_ID_LENGTH} characters is truncated when it is sent; see
	 * {@link #getConnectClientId()}.
	 *
	 * @param clientId the client identifier; {@code null} or empty means generate a random one
	 */
	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	/**
	 * Sets the broker host, or a full broker URL. The value is parsed again on the next connect.
	 * <p>
	 * If the value includes a port, that port replaces one set earlier with
	 * {@link #setPort(int)}; whichever was set last wins. Without a port, an earlier
	 * {@code setPort} still applies.
	 *
	 * @param host the host name, IP address or broker URL
	 */
	public void setHost(String host) {
		this.host = host;
		this.brokerUrl = host;
		if (explicitPort(host) != -1) {
			this.portOverride = -1;
		}
		settingsParsed = false;
	}

	/**
	 * Returns the port written in a broker address, if it has one. A missing scheme is allowed,
	 * as are host names with underscores (see {@link #parseBrokerUrl()}).
	 *
	 * @param address a host name, {@code host:port} or broker URL, or {@code null}
	 * @return the port, or {@code -1} if the address has none or cannot be parsed
	 */
	static int explicitPort(String address) {
		if (address == null || address.trim().isEmpty()) {
			return -1;
		}
		String normalized = address.trim();
		if (!normalized.contains("://")) {
			normalized = "mqtt://" + normalized;   // the scheme does not affect the port
		}
		URI uri;
		try {
			uri = new URI(normalized);
		} catch (URISyntaxException e) {
			return -1;   // reported when the address is parsed
		}
		if (uri.getPort() != -1) {
			return uri.getPort();
		}
		// As in parseBrokerUrl(): a host with '_' leaves getHost()/getPort() unset, so read the
		// port from the raw authority.
		String authority = uri.getRawAuthority();
		if (uri.getHost() == null && authority != null) {
			int at = authority.lastIndexOf('@');
			String hostPort = (at >= 0) ? authority.substring(at + 1) : authority;
			int colon = hostPort.lastIndexOf(':');
			if (colon > 0 && hostPort.indexOf(']') < colon) {
				String portText = hostPort.substring(colon + 1);
				if (!portText.isEmpty() && portText.chars().allMatch(Character::isDigit)) {
					return Integer.parseInt(portText);
				}
			}
		}
		return -1;
	}

	/**
	 * Returns the broker host. After the options have been parsed this is the bare host name.
	 *
	 * @return the broker host, or {@code null} if none is set
	 */
	public String getHost() {
		return this.host;
	}

	/**
	 * Sets the broker port, overriding any port in the broker URL set so far. A later
	 * {@link #setServerURI(String)} or {@link #setHost(String)} whose value includes a port
	 * replaces it; one without a port keeps it. The address is parsed again on the next connect.
	 *
	 * @param port the TCP port, or {@code -1} to use the URL's port or the default for the scheme
	 */
	public void setPort(int port) {
		this.port = port;
		this.portOverride = port;
		settingsParsed = false;
	}

	/**
	 * Returns the broker port.
	 *
	 * @return the port, or {@code -1} if not set and not yet parsed
	 */
	public int getPort() {
		return this.port;
	}

	/**
	 * Sets whether incoming messages must be acknowledged explicitly with
	 * {@link IMqttAsyncClient#messageArrivedComplete(int, int)}.
	 *
	 * @param manualAcks {@code true} to enable manual acknowledgements
	 */
	public void setManualAcks(boolean manualAcks) {
		isManualAcks = manualAcks;
	}

	/**
	 * Returns whether manual acknowledgements are enabled.
	 *
	 * @return {@code true} if manual acknowledgements are enabled
	 */
	protected boolean isManualAcks() {
		return isManualAcks;
	}

	/**
	 * Sets whether an exception thrown from {@link MqttCallback#messageArrived(String, MqttMessage)}
	 * (or from a per-subscription {@link IMqttMessageListener}) drops the connection.
	 * <p>
	 * By default such exceptions are logged and the client stays connected. Setting this to
	 * {@code true} gives Paho's behaviour: the connection is closed without sending DISCONNECT,
	 * and {@link MqttCallback#connectionLost(Throwable)} is called with an exception whose cause
	 * is the one the callback threw. Automatic reconnect, if enabled, then applies as for any
	 * other lost connection.
	 *
	 * @param messageArrivedErrorsTriggerDisconnect {@code true} to disconnect when a message
	 *                                              callback throws; the default is {@code false}
	 */
	public void setMessageArrivedErrorsTriggerDisconnect(boolean messageArrivedErrorsTriggerDisconnect) {
		this.messageArrivedErrorsTriggerDisconnect = messageArrivedErrorsTriggerDisconnect;
	}

	/**
	 * Returns whether an exception thrown from a message callback drops the connection.
	 *
	 * @return {@code true} if message callback errors disconnect the client
	 * @see #setMessageArrivedErrorsTriggerDisconnect(boolean)
	 */
	public boolean isMessageArrivedErrorsTriggerDisconnect() {
		return messageArrivedErrorsTriggerDisconnect;
	}


	/**
	 * Returns the maximum number of retransmissions of an unacknowledged publish.
	 *
	 * @return the limit, or {@code -1} for no limit
	 */
	public int getMaxInflightRetries() {
		return maxInflightRetries;
	}

	/**
	 * Sets the maximum number of retransmissions of an unacknowledged publish. Applies only when
	 * {@link #setRetransmissionInterval(int)} turns retransmission on.
	 *
	 * @param maxInflightRetries the limit, or {@code -1} for no limit
	 */
	public void setMaxInflightRetries(int maxInflightRetries) {
		this.maxInflightRetries = maxInflightRetries;
	}

	/**
	 * Returns the delay before an unacknowledged packet is first sent again on the same
	 * connection.
	 *
	 * @return the delay in seconds, or {@code 0} if packets are only sent again after a reconnect
	 */
	public int getRetransmissionInterval() {
		return retransmissionInterval;
	}

	/**
	 * Sets whether, and after how long, an unacknowledged PUBLISH, PUBREL, PUBREC, SUBSCRIBE or
	 * UNSUBSCRIBE is sent again on the same connection. With {@code 0}, the default, it is not:
	 * TCP delivers what was sent unless the connection fails, and after a reconnect with
	 * {@link #setCleanSession(boolean) cleanSession} {@code false} unacknowledged publishes are
	 * sent again anyway. This matches Paho, and MQTT 5, which forbids resending on a live
	 * connection. Resends add load to a broker that is slow to answer: with tens of thousands of
	 * QoS 2 publishes outstanding, Mosquitto fell further behind and finally closed the connection.
	 * <p>
	 * A positive value sends the packet again after that many seconds, then after 5 seconds
	 * more each time, up to {@link #setMaxInflightRetries(int)} times. Before this option the
	 * client always did so, with an interval of 10 seconds.
	 *
	 * @param seconds the delay before the first resend, or {@code 0} for none
	 * @throws IllegalArgumentException if {@code seconds} is negative
	 */
	public void setRetransmissionInterval(int seconds) {
		if (seconds < 0) {
			throw new IllegalArgumentException("Retransmission interval must be 0 or more: " + seconds);
		}
		this.retransmissionInterval = seconds;
	}


	/**
	 * The largest packet MQTT allows: a remaining length of 268,435,455 bytes (256 MB), the
	 * most its four-byte length encoding can hold.
	 */
	public static final int MAX_PACKET_SIZE = 268_435_455;

	/**
	 * Sets the largest incoming packet the client accepts, over every transport. A larger
	 * packet closes the connection, reported with {@link MqttException#REASON_CODE_INVALID_MESSAGE}.
	 * The default, {@link #MAX_PACKET_SIZE}, accepts anything MQTT allows, as Paho does; lower
	 * it to bound the memory a broker can make the client use for one message. (Over WebSocket a
	 * frame is read whole before the packet in it is checked, so there the bound is the frame
	 * the broker sends.)
	 *
	 * @param maxBytesInMessage the maximum remaining length in bytes, 1 to {@link #MAX_PACKET_SIZE}
	 * @throws IllegalArgumentException if the value is outside that range
	 */
	public void setMaxBytesInMessage(int maxBytesInMessage) {
		if (maxBytesInMessage < 1 || maxBytesInMessage > MAX_PACKET_SIZE) {
			throw new IllegalArgumentException("setMaxBytesInMessage()->must be 1 to " + MAX_PACKET_SIZE + ": " + maxBytesInMessage);
		}
		this.maxBytesInMessage = maxBytesInMessage;
	}

	/**
	 * Returns the largest incoming packet the decoder accepts.
	 *
	 * @return the maximum size in bytes
	 */
	public int getMaxBytesInMessage() {
		return this.maxBytesInMessage;
	}


	/**
	 * Returns the connection timeout: how long a connect may take, from the start of the TCP
	 * connect to CONNACK.
	 *
	 * @return the timeout in seconds; values of 0 or less other than {@code -1} are reported as 30
	 */
	public int getConnectionTimeout() {
		if (timeoutSeconds != -1 && timeoutSeconds <= 0) {
			return 30;
		}
		return timeoutSeconds;
	}

	/**
	 * Sets the connection timeout: how long a connect may take, from the start of the TCP
	 * connect, through any TLS or WebSocket handshake, to CONNACK. When it runs out the connect
	 * fails with {@link MqttException#REASON_CODE_CLIENT_TIMEOUT} if TCP had connected (with
	 * {@link MqttException#REASON_CODE_SERVER_CONNECT_ERROR} if not). With no timeout, a broker
	 * that accepts the connection and never answers is given up on by keep-alive.
	 *
	 * @param timeoutSeconds the timeout in seconds, or {@code -1} for no timeout of the client's
	 *                       own (the operating system still gives up eventually), which is what
	 *                       Paho's {@code 0} means; in {@link #enablePahoMode(boolean) Paho
	 *                       mode}, {@code 0} is accepted and stored as {@code -1}
	 * @throws IllegalArgumentException if the value is 0 or less and not {@code -1} (or, in Paho
	 *                                  mode, {@code 0})
	 */
	public void setConnectionTimeout(int timeoutSeconds) {
		if (pahoMode && timeoutSeconds == 0) {
			timeoutSeconds = -1;
		}
		if (timeoutSeconds != -1 && timeoutSeconds <= 0) {
			throw new IllegalArgumentException("setConnectionTimeout()->timeoutSeconds must be > 0 or -1");
		}
		this.timeoutSeconds = timeoutSeconds;
	}

	/**
	 * Returns the TCP connection timeout as Netty's {@code CONNECT_TIMEOUT_MILLIS} option
	 * expects it: {@code 0}, meaning none, for {@code -1}.
	 *
	 * @return the timeout in milliseconds, or 0 for no timeout
	 */
	int getConnectTimeoutMillis() {
		int seconds = getConnectionTimeout();
		return seconds == -1 ? 0 : seconds * 1000;
	}

	/**
	 * Returns the read timeout. Not currently applied by the client.
	 *
	 * @return the timeout in seconds, or the keep-alive interval plus 10 if none is set
	 */
	protected int getReadTimeout() {
		if (this.readTimeoutSeconds <= 0) {
			return this.keepaliveSeconds + 10;
		} else {
			return readTimeoutSeconds;
		}
	}

	/**
	 * Returns the keep-alive interval.
	 *
	 * @return the interval in seconds
	 */
	public int getKeepAliveInterval() {
		return this.keepaliveSeconds;
	}

	/**
	 * Returns the keep-alive to send in CONNECT: the interval, or {@code 0}, which turns
	 * keep-alive off in MQTT, for {@code -1}.
	 *
	 * @return the keep-alive in seconds, 0 to 65535
	 */
	int getConnectKeepAlive() {
		return Math.max(0, Math.min(this.keepaliveSeconds, 65535));
	}

	/**
	 * Returns the all-idle timeout used by the pipeline's idle-state handler: one and a half
	 * times the keep-alive interval, less one second.
	 *
	 * @return the timeout in seconds, or 0 if keep-alive is disabled
	 */
	public int getKeepAliveMax() {
		int retval = 0;
		if (this.keepaliveSeconds > 0) {
			int max = (int) (this.keepaliveSeconds * 1.5) - 1;
			if (max > 0) {
				retval = max;
			}
		}
		return retval;
	}

	/**
	 * Sets the keep-alive interval. A PINGREQ is sent when nothing has been written for this long.
	 * With {@code -1}, which is what Paho's {@code 0} means, keep-alive is off: CONNECT asks for a
	 * keep-alive of 0, so the broker never drops the client for being idle, and no PINGREQ is
	 * sent.
	 *
	 * @param keepaliveSeconds the interval in seconds, or {@code -1} to turn keep-alive off; in
	 *                         {@link #enablePahoMode(boolean) Paho mode}, {@code 0} is accepted
	 *                         and stored as {@code -1}
	 * @throws IllegalArgumentException if the value is 0 or less and not {@code -1} (or, in Paho
	 *                                  mode, {@code 0})
	 */
	public void setKeepAliveInterval(int keepaliveSeconds) {
		if (pahoMode && keepaliveSeconds == 0) {
			keepaliveSeconds = -1;
		}
		if (keepaliveSeconds != -1 && keepaliveSeconds <= 0) {
			throw new IllegalArgumentException("setKeepAliveInterval()->keepaliveSeconds must be > 0 or -1");
		}
		this.keepaliveSeconds = keepaliveSeconds;
	}

	/**
	 * Returns the MQTT protocol version.
	 *
	 * @return the protocol version
	 */
	public MqttVersion getMqttVersion() {
		return protocolVersion;
	}

	/**
	 * Sets the MQTT protocol version.
	 *
	 * @param protocolVersion the protocol version; {@code null} selects {@link #MQTT_VERSION_DEFAULT}
	 */
	public void setMqttVersion(MqttVersion protocolVersion) {
		if (protocolVersion == null) {
			this.protocolVersion = this.MQTT_VERSION_DEFAULT;
		} else {
			this.protocolVersion = protocolVersion;
		}
	}

	/**
	 * Sets the MQTT protocol version from Paho's numeric values, so code that passes Paho's
	 * {@code int} constants, or a number read from configuration, keeps working.
	 * <ul>
	 * <li>{@code 0}: Paho's {@code MQTT_VERSION_DEFAULT}; selects {@link #MQTT_VERSION_DEFAULT}
	 * (3.1.1). Unlike Paho, there is no fallback to 3.1 if the broker refuses 3.1.1.</li>
	 * <li>{@code 3}: MQTT 3.1</li>
	 * <li>{@code 4}: MQTT 3.1.1</li>
	 * </ul>
	 * Any other value is rejected, as in Paho's MQTT 3 client; MQTT 5 can only be selected with
	 * {@link #setMqttVersion(MqttVersion)} and {@link #MQTT_VERSION_5}.
	 *
	 * @param protocolVersion the protocol level: 0, 3 or 4
	 * @throws IllegalArgumentException if the value is not one of those
	 */
	public void setMqttVersion(int protocolVersion) {
		switch (protocolVersion) {
			case 0:
				setMqttVersion(MQTT_VERSION_DEFAULT);
				break;
			case 3:
				setMqttVersion(MqttVersion.MQTT_3_1);
				break;
			case 4:
				setMqttVersion(MqttVersion.MQTT_3_1_1);
				break;
			default:
				throw new IllegalArgumentException("An incorrect version was used \"" + protocolVersion
						+ "\". Acceptable version options are 0, 3 and 4.");
		}
	}


	/**
	 * Returns the {@code SO_KEEPALIVE} socket option.
	 *
	 * @return the option value
	 */
	protected boolean getSOKeepAlive() {
		return so_keepalive;
	}

	/**
	 * Returns the {@code SO_REUSEADDR} socket option.
	 *
	 * @return the option value
	 */
	protected boolean getSOReuseAddr() {
		return so_reuseaddr;
	}

	/**
	 * Returns the {@code TCP_NODELAY} socket option.
	 *
	 * @return the option value
	 */
	protected boolean isTCPNoDelayEnabled() {
		return tcp_nodelay;
	}


	/**
	 * Sets the {@code SO_KEEPALIVE} socket option.
	 *
	 * @param keepAlive the option value
	 */
	protected void setSOKeepAlive(boolean keepAlive) {
		this.so_keepalive = keepAlive;
	}

	/**
	 * Sets the {@code SO_REUSEADDR} socket option, despite the name.
	 *
	 * @param reuseaddr the option value
	 */
	protected void getSOReuseAddr(boolean reuseaddr) {
		this.so_reuseaddr = reuseaddr;
	}

	/**
	 * Sets the {@code TCP_NODELAY} socket option.
	 *
	 * @param nodelay the option value
	 */
	protected void TCPNoDelayEnabled(boolean nodelay) {
		this.tcp_nodelay = nodelay;
	}

	/**
	 * Checks whether the code is running on Android.
	 *
	 * @return {@code true} on Android
	 */
	protected boolean isAndroid() {
		boolean retval = false;
		String op_sys = System.getProperty("os.name");
		if (op_sys.equalsIgnoreCase("Android")) {
			retval = true;
		} else {
			String jre_name = System.getProperty("java.runtime.name");
			if (jre_name.toLowerCase().startsWith("android")) {
				retval = true;
			}
		}

		return retval;
	}

	/**
	 * Sets whether to connect over WebSockets when the broker URL has no scheme.
	 *
	 * @param value {@code true} to use WebSockets
	 */
	protected void isWebSocket(boolean value) {
		websocket = value;
	}

	/**
	 * Returns whether the connection uses WebSockets.
	 *
	 * @return {@code true} for a WebSocket connection
	 */
	protected boolean isWebSocket() {
		return websocket;
	}

	/**
	 * Returns the user name sent in CONNECT.
	 *
	 * @return the user name, or {@code null} if none is set
	 */
	public String getUserName() {
		return username;
	}

	/**
	 * Returns the longest wait between automatic reconnect attempts.
	 *
	 * @return the maximum delay in milliseconds
	 */
	public int getMaxReconnectDelay() {
		return maxReconnectDelay;
	}

	/**
	 * Sets the longest wait between automatic reconnect attempts. As in Paho, the wait starts
	 * at {@link #setRetryInterval(long) the retry interval} and doubles after each failed
	 * attempt until it reaches this limit. The default is 128000 (128 seconds).
	 *
	 * @param maxReconnectDelay the maximum delay in milliseconds
	 * @throws IllegalArgumentException if the value is less than 1
	 */
	public void setMaxReconnectDelay(int maxReconnectDelay) {
		if (maxReconnectDelay < 1) {
			throw new IllegalArgumentException("setMaxReconnectDelay()->maxReconnectDelay must be > 0");
		}
		this.maxReconnectDelay = maxReconnectDelay;
	}

	/**
	 * Sets the user name sent in CONNECT. A {@code null} or empty name is ignored.
	 *
	 * @param username the user name
	 * @throws IllegalArgumentException if the name encodes to more than 65535 bytes of UTF-8,
	 *                                  the most MQTT can carry
	 */
	public void setUserName(String username) {
		if ((username != null) && (username.length() > 0)) {
			MqttStrings.checkLength(username, "The user name");
			this.username = username;
		}
	}


	/**
	 * Returns the maximum number of QoS 1 and 2 publishes that may await acknowledgement at
	 * once.
	 *
	 * @return the maximum; {@value #DEFAULT_MAX_INFLIGHT} unless set
	 */
	public int getMaxInflight() {
		return maxInflight;
	}

	/**
	 * Sets the maximum number of QoS 1 and 2 publishes that may await acknowledgement at once,
	 * including those held while disconnected. A publish beyond the limit throws an
	 * {@link MqttException} with {@link MqttException#REASON_CODE_MAX_INFLIGHT}, as in Paho.
	 * <p>
	 * The limit cannot exceed {@value #MAX_INFLIGHT_LIMIT}, the number of packet identifiers MQTT
	 * allows (1 to 65535), so a packet identifier is never reused while still awaiting an
	 * acknowledgement. The default is {@value #DEFAULT_MAX_INFLIGHT}; Paho's is
	 * {@value #PAHO_MAX_INFLIGHT}. For QoS 2 against Mosquitto keep it to about 1000, as Mosquitto
	 * slows down sharply with tens of thousands of QoS 2 messages in progress.
	 * QoS 0 publishes have a window of their own: see {@link #setMaxUnwrittenQos0(int)}.
	 *
	 * @param maxInflight the maximum, from 1 to {@value #MAX_INFLIGHT_LIMIT}
	 * @throws IllegalArgumentException if the value is outside that range
	 */
	public void setMaxInflight(int maxInflight) {
		if (maxInflight < 1 || maxInflight > MAX_INFLIGHT_LIMIT) {
			throw new IllegalArgumentException("setMaxInflight()->maxInflight must be between 1 and "
					+ MAX_INFLIGHT_LIMIT + ", was " + maxInflight);
		}
		this.maxInflight = maxInflight;
	}

	/**
	 * Returns the maximum number of QoS 0 publishes that may wait to be written to the socket.
	 *
	 * @return the maximum; {@value #MAX_INFLIGHT_LIMIT} unless set
	 */
	public int getMaxUnwrittenQos0() {
		return maxUnwrittenQos0;
	}

	/**
	 * Sets the maximum number of QoS 0 publishes that may wait to be written to the socket. A
	 * QoS 0 token completes once its PUBLISH is written, and a publish beyond the limit throws an
	 * {@link MqttException} with {@link MqttException#REASON_CODE_MAX_INFLIGHT}, so a publisher
	 * faster than the network gets back-pressure rather than an ever-growing queue. Paho does not
	 * limit QoS 0 publishes.
	 *
	 * @param maxUnwrittenQos0 the maximum, 1 or more; {@value #MAX_INFLIGHT_LIMIT} by default
	 * @throws IllegalArgumentException if the value is less than 1
	 */
	public void setMaxUnwrittenQos0(int maxUnwrittenQos0) {
		if (maxUnwrittenQos0 < 1) {
			throw new IllegalArgumentException("setMaxUnwrittenQos0()->maxUnwrittenQos0 must be 1 or more, was "
					+ maxUnwrittenQos0);
		}
		this.maxUnwrittenQos0 = maxUnwrittenQos0;
	}

	/**
	 * Returns whether to start a clean session.
	 *
	 * @return {@code true} to discard any previous session
	 */
	public boolean isCleanSession() {
		return cleanSession;
	}

	/**
	 * Sets whether to start a clean session.
	 *
	 * @param cleanSession {@code true} to discard any previous session
	 */
	public void setCleanSession(boolean cleanSession) {
		this.cleanSession = cleanSession;
	}


	/**
	 * Returns the last will message.
	 *
	 * @return the last will, or {@code null} if none is set
	 */
	public MqttLastWill getLastWill() {
		return lastWill;
	}

	/**
	 * Sets the last will message.
	 *
	 * @param lastWill the last will, or {@code null} for none
	 */
	public void setLastWill(MqttLastWill lastWill) {
		this.lastWill = lastWill;
	}

	/**
	 * Returns the topic the last will is published to, as Paho does.
	 *
	 * @return the will topic, or {@code null} if no will is set
	 */
	public String getWillDestination() {
		MqttLastWill will = this.lastWill;
		return (will == null) ? null : will.getTopic();
	}

	/**
	 * Returns the last will as a message, with its payload, QoS and retained flag, as Paho
	 * does. Each call returns a new copy, so changing it does not change the will.
	 *
	 * @return the will message, or {@code null} if no will is set
	 */
	public MqttMessage getWillMessage() {
		MqttLastWill will = this.lastWill;
		if (will == null) {
			return null;
		}
		MqttMessage message = new MqttMessage(will.getPayload());
		message.setQos(will.getQos().value());
		message.setRetained(will.isRetain());
		return message;
	}


	/**
	 * Sets the "Last Will and Testament" (LWT) for the connection. In the event
	 * that this client unexpectedly loses its connection to the server, the server
	 * will publish a message to itself using the supplied details.
	 *
	 * @param topic    the topic to publish to.
	 * @param payload  the byte payload for the message.
	 * @param qos      the quality of service to publish the message at (0, 1 or 2).
	 * @param retained whether or not the message should be retained.
	 */
	public void setWill(MqttTopic topic, byte[] payload, int qos, boolean retained) {
		String topicS = topic.getName();
		setWill(topicS, payload, qos, retained);
	}

	/**
	 * Sets the "Last Will and Testament" (LWT) for the connection. The payload is sent exactly
	 * as given, and a QoS outside 0 to 2 is treated as 0.
	 *
	 * @param topic    the topic to publish to.
	 * @param payload  the byte payload for the message.
	 * @param qos      the quality of service to publish the message at (0, 1 or 2).
	 * @param retained whether or not the message should be retained.
	 * @throws IllegalArgumentException if the topic encodes to, or the payload is, more than
	 *                                  65535 bytes, the most MQTT can carry
	 */
	public void setWill(String topic, byte[] payload, int qos, boolean retained) {
		MqttQoS mqttqos;
		switch (qos) {
			case 0:
				mqttqos = MqttQoS.AT_MOST_ONCE;
				break;
			case 1:
				mqttqos = MqttQoS.AT_LEAST_ONCE;
				break;
			case 2:
				mqttqos = MqttQoS.EXACTLY_ONCE;
				break;
			default:
				mqttqos = MqttQoS.AT_MOST_ONCE;
		}

		this.lastWill = new MqttLastWill(topic, payload, retained, mqttqos);

	}

	/**
	 * Returns the Netty channel class used for connections.
	 *
	 * @return the channel class
	 */
	public Class<? extends Channel> getChannelClass() {
		return channelClass;
	}

	/**
	 * Sets the broker URL. Equivalent to {@link #setHost(String)}: a port in the URL replaces
	 * one set earlier with {@link #setPort(int)}.
	 *
	 * @param serverURI the broker URL, for example {@code tcp://host:1883}
	 */
	public void setServerURI(String serverURI) {
		setHost(serverURI);
	}


	/**
	 * Returns the normalized broker URI, parsing the address first if needed.
	 *
	 * @return the broker URI, or {@code null} if no host is set
	 */
	public URI getServerURI() {
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "getServerURI()->");
		if (!settingsParsed) {
			parseBrokerUrl();
		}
		return this.uri;
	}

	/**
	 * Sets a list of brokers to connect to, as Paho does for high availability. With two or more,
	 * {@code connect} with these options behaves as {@code connect} with an {@link MqttServerList}
	 * holding a copy of these options for each address, in next-server mode with round robin. As
	 * in Paho, the first connect tries each address once, in turn, and fails if none accepts the
	 * connection; after a lost connection, automatic reconnect moves on through the list after
	 * each failure, up to {@link #getMaxRetries()} attempts. With one address it is used as {@link
	 * #setServerURI(String)}; {@code null} or an empty array clears the list. For more control,
	 * such as random choice or different settings per broker, use {@link MqttServerList}.
	 *
	 * @param serverURIs the broker addresses, for example {@code tcp://broker-a:1883}
	 * @throws IllegalArgumentException if an address is {@code null} or empty
	 */
	public void setServerURIs(String[] serverURIs) {
		if (serverURIs == null || serverURIs.length == 0) {
			this.serverURIs = null;
			return;
		}
		for (String uri : serverURIs) {
			if (uri == null || uri.trim().isEmpty()) {
				throw new IllegalArgumentException("Server URIs must not be null or empty");
			}
		}
		this.serverURIs = serverURIs.clone();
		if (serverURIs.length == 1) {
			setServerURI(serverURIs[0]);
		}
	}

	/**
	 * Returns the brokers set with {@link #setServerURIs(String[])}.
	 *
	 * @return a copy of the addresses, or {@code null} if none are set
	 */
	public String[] getServerURIs() {
		return serverURIs == null ? null : serverURIs.clone();
	}

	/**
	 * Sets the Netty channel class used for connections. It must match the event loop group type.
	 *
	 * @param channelClass the channel class
	 */
	public void setChannelClass(Class<? extends Channel> channelClass) {
		this.channelClass = channelClass;
	}

	/**
	 * Returns the TLS context.
	 *
	 * @return the TLS context, or {@code null} if TLS is not configured
	 */
	public SslContext getSslContext() {
		return sslContext;
	}

	/**
	 * Clears the TLS context. This does not change whether the connection is considered secure.
	 */
	public void setSslContext() {
		this.sslContext = null;
		settingsParsed = false;
	}

	/**
	 * Sets the TLS context and enables TLS.
	 *
	 * @param sslContext the Netty TLS context; {@code null} leaves the current context unchanged
	 */
	public void setSslContext(SslContext sslContext) {
		if (sslContext != null) {
			this.sslContext = sslContext;
			secure = true;
		}
		settingsParsed = false;
	}

	/**
	 * Sets the TLS context and enables TLS.
	 *
	 * @param sslContext the JDK TLS context, wrapped in a Netty context; {@code null} leaves the current context unchanged
	 */
	public void setSslContext(SSLContext sslContext) {
		if (sslContext != null) {
			this.sslContext = new JdkSslContext(sslContext, true, ClientAuth.NONE);
			secure = true;
		}
		settingsParsed = false;
	}


	/**
	 * Returns whether automatic reconnect is enabled. Equivalent to {@link #getAutomaticReconnect()}.
	 *
	 * @return {@code true} if automatic reconnect is enabled
	 */
	public boolean isReconnect() {
		return reconnect;
	}

	/**
	 * Sets whether to reconnect automatically after the connection is lost. An initial
	 * connection failure is not retried.
	 *
	 * @param reconnect {@code true} to enable automatic reconnect
	 */
	public void setAutomaticReconnect(boolean reconnect) {
		this.reconnect = reconnect;
	}


	/**
	 * Returns whether automatic reconnect is enabled.
	 *
	 * @return {@code true} if automatic reconnect is enabled
	 */
	public boolean getAutomaticReconnect() {
		return this.reconnect;
	}

	/**
	 * Returns the wait before the first automatic reconnect attempt; later waits double, up to
	 * {@link #getMaxReconnectDelay()}.
	 *
	 * @return the initial delay in seconds
	 */
	public long getRetryInterval() {
		return retryInterval;
	}

	/**
	 * Sets the wait before the first automatic reconnect attempt. After each failed attempt the
	 * wait doubles, up to {@link #setMaxReconnectDelay(int) the maximum reconnect delay}.
	 *
	 * @param retryInterval the initial delay in seconds
	 */
	public void setRetryInterval(long retryInterval) {
		this.retryInterval = retryInterval;
	}

	/**
	 * Returns the maximum number of automatic reconnect attempts.
	 *
	 * @return the maximum, or {@code -1} for no limit
	 */
	public int getMaxRetries() {
		return this.maxRetries;
	}

	/**
	 * Sets the maximum number of automatic reconnect attempts.
	 *
	 * @param maxRetries the maximum, or {@code -1} for no limit
	 */
	public void setMaxRetries(int maxRetries) {
		this.maxRetries = maxRetries;
	}

	/**
	 * Returns whether the connection uses TLS.
	 *
	 * @return {@code true} for a TLS connection
	 */
	public boolean isSecure() {
		return secure;
	}

	/**
	 * Returns the local address to bind the connection to.
	 *
	 * @return the local address, or {@code null} to let the system choose
	 */
	public SocketAddress getBindAddress() {
		return bindAddress;
	}

	/**
	 * Sets the local address to bind the connection to.
	 *
	 * @param bindAddress the local address, or {@code null} to let the system choose
	 */
	public void setBindAddress(SocketAddress bindAddress) {
		this.bindAddress = bindAddress;
	}

	/**
	 * Returns the hook used to customize each new TLS engine.
	 *
	 * @return the TLS engine consumer
	 */
	public Consumer<SSLEngine> getSslEngineConsumer() {
		return sslEngineConsumer;
	}

	/**
	 * Sets a hook to customize each new TLS engine, for example to set protocols or cipher suites.
	 *
	 * @param sslEngineConsumer the TLS engine consumer; must not be {@code null}
	 */
	public void setSslEngineConsumer(Consumer<SSLEngine> sslEngineConsumer) {
		this.sslEngineConsumer = sslEngineConsumer;
	}

	/**
	 * Returns whether the broker's certificate is checked against the host name during the TLS
	 * handshake.
	 *
	 * @return {@code true} if HTTPS-style hostname verification is enabled
	 */
	public boolean isHttpsHostnameVerificationEnabled() {
		return httpsHostnameVerificationEnabled;
	}

	/**
	 * Enables or disables HTTPS-style hostname verification (RFC 2818): during the TLS
	 * handshake the broker's certificate must name the host the client connects to, or the
	 * handshake fails. It sets the {@code SSLEngine}'s endpoint identification algorithm to
	 * {@code HTTPS} before the hook set with {@link #setSslEngineConsumer(Consumer)} runs, so the
	 * hook can still override it.
	 * <p>
	 * As in Paho, except that it is off by default here; Paho 1.2.1 and later turn it on, and
	 * so does {@link #enablePahoMode(boolean)}.
	 *
	 * @param enabled {@code true} to verify the host name
	 */
	public void setHttpsHostnameVerificationEnabled(boolean enabled) {
		this.httpsHostnameVerificationEnabled = enabled;
	}

	/**
	 * Returns whether flush consolidation is on; see {@link #enableFlushConsolidation(boolean)}.
	 *
	 * @return {@code true} if on
	 */
	public boolean isFlushConsolidationEnabled() {
		return flushConsolidation;
	}

	/**
	 * Turns flush consolidation on or off for connections made from now on; it is off by
	 * default. When on, Netty's {@code FlushConsolidationHandler} combines flushes to the socket:
	 * acknowledgements written while a read is in progress go out in one flush when the read
	 * completes, and publishes queued on the same pass of the event loop share a flush. This
	 * saves a system call per packet under load, at the cost of each packet waiting for the rest
	 * of its pass of the event loop.
	 *
	 * @param enable {@code true} to combine flushes
	 */
	public void enableFlushConsolidation(boolean enable) {
		this.flushConsolidation = enable;
	}

	/**
	 * Returns whether {@link #enablePahoMode(boolean)} last turned Paho mode on.
	 *
	 * @return {@code true} in Paho mode
	 */
	public boolean isPahoMode() {
		return pahoMode;
	}

	/**
	 * Switches the settings whose defaults differ from Paho's to Paho's values, or back to this
	 * client's defaults. Paho mode is off by default. Turning it on sets:
	 * <ul>
	 * <li>{@link #setHttpsHostnameVerificationEnabled(boolean) HTTPS hostname verification}: on
	 * (default off);</li>
	 * <li>{@link #setConnectionTimeout(int) connection timeout}: {@value #PAHO_CONNECTION_TIMEOUT} s
	 * (default 5 s);</li>
	 * <li>{@link #setMaxInflight(int) in-flight window}: {@value #PAHO_MAX_INFLIGHT} (default
	 * {@value #DEFAULT_MAX_INFLIGHT}); the QoS 0 window, {@link #setMaxUnwrittenQos0(int)}, is
	 * unchanged, as Paho has none;</li>
	 * <li>{@link #setMaxRetries(int) reconnect attempts}: unlimited, as Paho retries for ever
	 * (default 2); reconnecting still needs {@link #setAutomaticReconnect(boolean)};</li>
	 * <li>{@link #setMessageArrivedErrorsTriggerDisconnect(boolean)}: {@code true}, so an
	 * exception from a message callback closes the connection (default {@code false});</li>
	 * <li>the disconnect methods without timeouts quiesce for up to
	 * {@value #PAHO_QUIESCE_TIMEOUT} ms, and {@code disconnectForcibly()} then allows
	 * {@value #PAHO_DISCONNECT_TIMEOUT} ms to send DISCONNECT (default: no quiescing, and
	 * {@code disconnectForcibly()} sends no DISCONNECT).</li>
	 * </ul>
	 * While Paho mode is on, {@link #setConnectionTimeout(int)} and
	 * {@link #setKeepAliveInterval(int)} also accept Paho's {@code 0}, for no timeout and no
	 * keep-alive, and store it as {@code -1}; outside Paho mode {@code 0} is rejected.
	 * Turning it off restores this client's defaults for all of them. Either way the values are
	 * set when this is called, so call it before changing any of these settings yourself.
	 *
	 * @param enable {@code true} for Paho's values, {@code false} for this client's defaults
	 */
	public void enablePahoMode(boolean enable) {
		this.pahoMode = enable;
		this.httpsHostnameVerificationEnabled = enable;
		this.timeoutSeconds = enable ? PAHO_CONNECTION_TIMEOUT : 5;
		this.maxInflight = enable ? PAHO_MAX_INFLIGHT : DEFAULT_MAX_INFLIGHT;
		this.maxRetries = enable ? -1 : 2;
		this.messageArrivedErrorsTriggerDisconnect = enable;
	}

	/**
	 * Returns the verifier set with {@link #setSSLHostnameVerifier(HostnameVerifier)}.
	 *
	 * @return the hostname verifier, or {@code null} if none is set
	 */
	public HostnameVerifier getSSLHostnameVerifier() {
		return hostnameVerifier;
	}

	/**
	 * Sets a verifier that decides, after each TLS handshake, whether the broker is the host
	 * the client meant to connect to. It is called with the host from the broker address and
	 * the handshake's {@code SSLSession}; if it returns {@code false} or throws, the connection
	 * is closed before CONNECT is sent and the connect fails with an
	 * {@code SSLPeerUnverifiedException} in its cause chain.
	 * <p>
	 * As in Paho, the verifier is only used while
	 * {@link #setHttpsHostnameVerificationEnabled(boolean) HTTPS hostname verification} is off,
	 * and there is no default verifier.
	 *
	 * @param hostnameVerifier the verifier, or {@code null} for none
	 */
	public void setSSLHostnameVerifier(HostnameVerifier hostnameVerifier) {
		this.hostnameVerifier = hostnameVerifier;
	}


	/**
	 * Parses the broker address into a host, port and connection type, once per change of
	 * address. Parsing always starts from the address given to {@link #setServerURI(String)} or
	 * {@link #setHost(String)}, so re-parsing keeps its scheme and path; a port set with
	 * {@link #setPort(int)} overrides the address's port. An address without a scheme gets {@code mqtt://}, {@code mqtts://}, {@code ws://} or
	 * {@code wss://} depending on the TLS and WebSocket settings. Supported schemes, with their
	 * default ports:
	 * <ul>
	 * <li>{@code mqtt}, {@code tcp}: plain TCP, port 1883</li>
	 * <li>{@code mqtts}, {@code ssl}: TLS, port 8883</li>
	 * <li>{@code ws}, {@code wss}: WebSockets, ports 8083 and 8084, with the path ending in {@code /mqtt}</li>
	 * <li>{@code http}, {@code https}: aliases of {@code ws} and {@code wss}</li>
	 * </ul>
	 * The scheme only selects the transport. Any of them can be routed through a SOCKS4,
	 * SOCKS5 or HTTP proxy, configured separately with {@link #setProxy} or
	 * {@link #setHTTPProxy}.
	 * <p>
	 * Host names containing underscores are accepted. A path is only allowed on WebSocket
	 * URIs; on any other scheme a path other than {@code /} is rejected. Does nothing if no
	 * host is set.
	 *
	 * @throws IllegalArgumentException if the address is malformed, the scheme is not supported,
	 *                                  or a non-WebSocket URI has a path
	 */
	protected void parseBrokerUrl() {
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "parseBrokerUrl()->start");
		if (settingsParsed) {
			if (BuildConfig.LOGGING && enableLogging) log(TAG + "parseBrokerUrl()->Settings already parsed.");
			if (BuildConfig.LOGGING && enableLogging) log(TAG + "parseBrokerUrl()->end");
			return;
		}

		// Always parse the address as the user gave it. this.host and this.port hold the parsed
		// results, so re-parsing from them (for example after setSslContext()) would lose the
		// scheme and the WebSocket path.
		if (this.brokerUrl == null || this.brokerUrl.trim().isEmpty()) {
			return;
		}

		String normalized = this.brokerUrl.trim();
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "parseBrokerUrl()->portOverride=" + this.portOverride);
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "parseBrokerUrl()->normalized=" + normalized);

		// No scheme provided -> default to plain mqtt
		if (!normalized.contains("://")) {
			if (!websocket) {
				if (!secure) {
					normalized = "mqtt://" + normalized;
				} else {
					normalized = "mqtts://" + normalized;
				}
			} else {
				if (!secure) {
					normalized = "ws://" + normalized;
				} else {
					normalized = "wss://" + normalized;
				}
			}
		}
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "parseBrokerUrl()->normalized=" + normalized);
		URI uri;
		try {
			uri = new URI(normalized);
		} catch (URISyntaxException e) {
			throw new IllegalArgumentException("Invalid broker URL: " + normalized, e);
		}

		String scheme = uri.getScheme() == null ? "mqtt" : uri.getScheme().toLowerCase();
		String host = uri.getHost();
		int uriPort = uri.getPort();
		if (host == null) {
			// java.net.URI only parses a server-based authority when the host is a valid
			// RFC 2396 host name, so names containing '_' (common for Docker containers)
			// leave getHost() null. Take the host and port from the raw authority instead.
			String authority = uri.getRawAuthority();
			if (authority != null) {
				int at = authority.lastIndexOf('@');
				String hostPort = (at >= 0) ? authority.substring(at + 1) : authority;
				int colon = hostPort.lastIndexOf(':');
				if (colon > 0 && hostPort.indexOf(']') < colon) {
					String portText = hostPort.substring(colon + 1);
					if (!portText.isEmpty() && portText.chars().allMatch(Character::isDigit)) {
						uriPort = Integer.parseInt(portText);
					}
					hostPort = hostPort.substring(0, colon);
				}
				if (!hostPort.isEmpty()) {
					host = hostPort;
				}
			}
		}
		if (host == null) {
			throw new IllegalArgumentException("Could not determine host from broker URL: " + normalized);
		}

		boolean secure;
		boolean webSocket;

		int defaultPort;

		switch (scheme) {
			case "mqtt":
			case "tcp":
				secure = false;
				webSocket = false;
				defaultPort = 1883;
				break;
			case "mqtts":
			case "ssl":
				// Aliases: TLS straight to the broker. A proxy, if any, comes from setProxy() or
				// setHTTPProxy(), never from the scheme.
				secure = true;
				webSocket = false;
				defaultPort = 8883;
				break;
			case "ws":
			case "http":
				secure = false;
				webSocket = true;
				defaultPort = 8083;
				break;
			case "wss":
			case "https":
				secure = true;
				webSocket = true;
				defaultPort = 8084;
				break;
			default:
				throw new IllegalArgumentException("Unsupported broker URL scheme: " + scheme);
		}

		int port = portOverride != -1 ? portOverride : (uriPort == -1 ? defaultPort : uriPort);
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "parseBrokerUrl()->parsed port=" + port);

		// Only WebSocket URIs carry a path (normalized below to end in /mqtt); as in Paho, a
		// path on any other scheme is an error rather than being silently ignored.
		if (!webSocket) {
			String rawPath = uri.getRawPath();
			if (rawPath != null && !rawPath.isEmpty() && !rawPath.equals("/")) {
				throw new IllegalArgumentException("URI path must be empty: " + normalized);
			}
		}

		String path = webSocket ? normalizeWebSocketPath(uri.getRawPath()) : null;

		String connectScheme = webSocket ? (secure ? "wss" : "ws") : (secure ? "ssl" : "tcp");
		String connectUri = connectScheme + "://" + host + ":" + port + (webSocket ? path : "");
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "parseBrokerUrl()->connectScheme=" + connectScheme);
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "parseBrokerUrl()->connectUri=" + connectUri);


		this.uri = uri;

		try {
			this.uri = new URI(connectUri);
		} catch (URISyntaxException e) {
			throw new IllegalArgumentException("Invalid broker URL: " + connectUri, e);
		}

		this.websocket = webSocket;
		this.secure = secure;
		this.port = port;
		this.host = host;

		settingsParsed = true;
		if (BuildConfig.LOGGING && enableLogging) log(TAG + "parseBrokerUrl()->end");
	}


	// Replaces parseURL(): only ever needed for websocket endpoints
	/**
	 * Ensures a WebSocket path ends in {@code /mqtt}, appending it if necessary and dropping a
	 * trailing slash after it.
	 *
	 * @param rawPath the path from the URL; {@code null} or blank is treated as {@code /}
	 * @return the normalized path, for example {@code /mqtt} or {@code /broker/mqtt}
	 */
	private String normalizeWebSocketPath(String rawPath) {
		String path = (rawPath == null || rawPath.trim().isEmpty()) ? "/" : rawPath;
		if (path.endsWith("/mqtt")) {
			return path;
		}
		if (path.endsWith("/mqtt/")) {
			return path.substring(0, path.length() - 1); // drop the trailing slash
		}
		if (path.endsWith("/")) {
			return path + "mqtt";
		}
		return path + "/mqtt";
	}
}
