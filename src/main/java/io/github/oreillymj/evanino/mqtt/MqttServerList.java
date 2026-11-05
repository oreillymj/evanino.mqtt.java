/*******************************************************************************
 * Copyright (c) 2026 Michael OReilly
 *
 * This program and the accompanying materials are made available under
 * the terms of the Apache License, Version 2.0, which accompanies this
 * distribution (see LICENSE).
 *
 * The Apache License is available at
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Contributors:
 *    Michael OReilly - initial API and implementation
 *******************************************************************************/

package io.github.oreillymj.evanino.mqtt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A list of brokers for high availability: one {@link MqttConnectOptions} per broker, and the
 * rules for moving between them. Pass it to {@link MqttAsyncClient#connect(MqttServerList)} or
 * {@link MqttClient#connect(MqttServerList)}.
 * <p>
 * The options for each broker are complete options of their own, so brokers can differ in more
 * than the address (TLS, credentials, WebSocket path). The easiest way to make them is from the
 * first: {@link #addServer(String)} adds a copy of the first options with another broker
 * address, and {@link MqttConnectOptions#MqttConnectOptions(MqttConnectOptions)} copies options
 * to change further:
 * <pre>{@code
 * MqttConnectOptions a = new MqttConnectOptions();
 * a.setServerURI("tcp://broker-a:1883");
 * a.setUserName("user");
 * a.setPassword("secret".toCharArray());
 * a.setAutomaticReconnect(true);
 *
 * MqttServerList servers = new MqttServerList(a);
 * servers.addServer("tcp://broker-b:1883");
 * MqttConnectOptions c = servers.addServer("tcp://broker-c");
 * c.setPort(1885);
 * servers.reconnectUsingNextServer(true);
 * servers.setStrategy(MqttServerList.Strategy.RANDOM);
 *
 * client.connect(servers);
 * }</pre>
 * <p>
 * With more than one broker, the first {@code connect} tries the first broker, and moves on
 * through the list as described below until one accepts the connection or the retries run out;
 * only then does the connect fail. After a connection is lost, {@link
 * MqttConnectOptions#setAutomaticReconnect(boolean) automatic reconnect} (still needed, as with
 * one broker) moves through the list in the same way. A lost connection counts as one failure of
 * the broker it was to. Each attempt uses that broker's options. The wait before an attempt is
 * that broker's {@link MqttConnectOptions#setRetryInterval(long) retry interval}, doubled as
 * described for each mode, up to its {@link MqttConnectOptions#setMaxReconnectDelay(int)
 * maximum reconnect delay}.
 * <p>
 * There are two modes, and exactly one is on:
 * <ul>
 * <li>{@link #reconnectUsingNextServer(boolean) Next server} (the default, as Paho does with
 * {@link MqttConnectOptions#setServerURIs(String[])}): after each failure the client moves to
 * another broker, chosen by the {@link #setStrategy(Strategy) strategy}. It gives up when the
 * failures since the last successful connection exceed {@link #setMaxRetries(int) the list's
 * retry limit}. The wait doubles after each pass through the list.</li>
 * <li>{@link #reconnectUntilRetryLimit(boolean) Until the retry limit}: the client stays with a
 * broker until it has failed more than that broker's own {@link
 * MqttConnectOptions#setMaxRetries(int) retry limit} times in a row, then moves to the next in
 * the list, wrapping round from the end. It gives up once every broker has used up its retries
 * since the last successful connection. The wait doubles after each failure, and starts again
 * at each new broker. A broker with no retry limit ({@code -1}) is never left.</li>
 * </ul>
 * When the client gives up, a first connect fails, and after a lost connection the callback's
 * {@link MqttCallback#onReconnectFailed} is called, as with one broker.
 * <p>
 * The client takes a copy of the list, and of its settings, when {@code connect} is called;
 * changes made afterwards apply from the next {@code connect}. The options objects themselves
 * are not copied. The session (messages in flight and the persistence) is the client's, not the
 * broker's: with {@link MqttConnectOptions#setCleanSession(boolean) clean session} off, messages
 * not yet acknowledged by one broker are sent again to the next, as Paho does.
 * <p>
 * With only one broker in the list, the client behaves exactly as with those options alone.
 */
public class MqttServerList {

	/**
	 * How the next broker is chosen in {@link #reconnectUsingNextServer(boolean) next-server}
	 * mode.
	 */
	public enum Strategy {
		/** The next broker in the list, wrapping round from the end to the start. */
		ROUND_ROBIN,
		/**
		 * A broker picked at random from those that have not failed since the last successful
		 * connection, so that clients leaving a failed broker spread over the others. When every
		 * other broker has failed, any broker but the one that has just failed.
		 */
		RANDOM
	}

	/**
	 * The value of {@link #getMaxRetries()} meaning "use the retry limit of the first broker's
	 * options"; the default.
	 */
	public static final int RETRIES_OF_FIRST_SERVER = -2;

	private final List<MqttConnectOptions> servers = new ArrayList<>();

	private boolean untilRetryLimit = false;

	private Strategy strategy = Strategy.ROUND_ROBIN;

	private int maxRetries = RETRIES_OF_FIRST_SERVER;

	/** Whether a first connect tries each broker once and then fails, as Paho's does. */
	private boolean onePassOnFirstConnect = false;

	/**
	 * Creates a list of brokers.
	 *
	 * @param servers the options for each broker, in order; more can be added later
	 * @throws IllegalArgumentException if any is {@code null}
	 */
	public MqttServerList(MqttConnectOptions... servers) {
		for (MqttConnectOptions options : servers) {
			add(options);
		}
	}

	/**
	 * Adds a broker at the end of the list.
	 *
	 * @param options the options for the broker
	 * @return this list
	 * @throws IllegalArgumentException if {@code options} is {@code null}
	 */
	public MqttServerList add(MqttConnectOptions options) {
		if (options == null) {
			throw new IllegalArgumentException("options must not be null");
		}
		servers.add(options);
		return this;
	}

	/**
	 * Adds a broker at the end of the list, with a copy of the first broker's options at another
	 * address. As with {@link MqttConnectOptions#setServerURI(String)}, a port set with {@link
	 * MqttConnectOptions#setPort(int)} on the first broker's options is kept unless the address
	 * has a port of its own.
	 *
	 * @param serverURI the broker address, for example {@code ssl://broker-b:8883}
	 * @return the new broker's options, to change further if needed
	 * @throws IllegalStateException    if the list is empty
	 * @throws IllegalArgumentException if {@code serverURI} is {@code null} or empty
	 */
	public MqttConnectOptions addServer(String serverURI) {
		if (servers.isEmpty()) {
			throw new IllegalStateException("Add the first broker's options before addServer()");
		}
		if (serverURI == null || serverURI.trim().isEmpty()) {
			throw new IllegalArgumentException("serverURI must not be empty");
		}
		MqttConnectOptions copy = new MqttConnectOptions(servers.get(0));
		copy.setServerURIs(null);
		copy.setServerURI(serverURI);
		servers.add(copy);
		return copy;
	}

	/**
	 * Returns the brokers' options, in order.
	 *
	 * @return an unmodifiable view of the list
	 */
	public List<MqttConnectOptions> getServers() {
		return Collections.unmodifiableList(servers);
	}

	/**
	 * Returns the number of brokers.
	 *
	 * @return the number of brokers
	 */
	public int size() {
		return servers.size();
	}

	/**
	 * Turns until-the-retry-limit mode on or off: stay with a broker until it has used up its own
	 * retries, then move to the next. The two modes are mutually exclusive, so turning this on
	 * turns {@link #reconnectUsingNextServer(boolean) next-server mode} off, and turning it off
	 * turns next-server mode on.
	 *
	 * @param enable {@code true} for until-the-retry-limit mode
	 * @return this list
	 */
	public MqttServerList reconnectUntilRetryLimit(boolean enable) {
		this.untilRetryLimit = enable;
		return this;
	}

	/**
	 * Returns whether until-the-retry-limit mode is on.
	 *
	 * @return {@code true} if it is on, {@code false} if next-server mode is
	 */
	public boolean isReconnectUntilRetryLimit() {
		return untilRetryLimit;
	}

	/**
	 * Turns next-server mode on or off: move to another broker after each failure. The two modes
	 * are mutually exclusive, so turning this on turns {@link #reconnectUntilRetryLimit(boolean)
	 * until-the-retry-limit mode} off, and turning it off turns that mode on. On by default.
	 *
	 * @param enable {@code true} for next-server mode
	 * @return this list
	 */
	public MqttServerList reconnectUsingNextServer(boolean enable) {
		this.untilRetryLimit = !enable;
		return this;
	}

	/**
	 * Returns whether next-server mode is on.
	 *
	 * @return {@code true} if it is on, {@code false} if until-the-retry-limit mode is
	 */
	public boolean isReconnectUsingNextServer() {
		return !untilRetryLimit;
	}

	/**
	 * Sets how the next broker is chosen in next-server mode. Ignored in until-the-retry-limit
	 * mode, which always takes the next in the list. The default is {@link Strategy#ROUND_ROBIN}.
	 *
	 * @param strategy the strategy
	 * @return this list
	 * @throws IllegalArgumentException if {@code strategy} is {@code null}
	 */
	public MqttServerList setStrategy(Strategy strategy) {
		if (strategy == null) {
			throw new IllegalArgumentException("strategy must not be null");
		}
		this.strategy = strategy;
		return this;
	}

	/**
	 * Returns how the next broker is chosen in next-server mode.
	 *
	 * @return the strategy
	 */
	public Strategy getStrategy() {
		return strategy;
	}

	/**
	 * Sets the retry limit for next-server mode: how many more attempts, across all the brokers,
	 * may follow the first failure since the last successful connection before the client gives
	 * up. Until-the-retry-limit mode uses each broker's own limit instead. The default, {@link
	 * #RETRIES_OF_FIRST_SERVER}, uses the first broker's {@link MqttConnectOptions#getMaxRetries()}.
	 *
	 * @param maxRetries the limit, {@code -1} for no limit, or {@link #RETRIES_OF_FIRST_SERVER}
	 * @return this list
	 * @throws IllegalArgumentException if {@code maxRetries} is below {@link #RETRIES_OF_FIRST_SERVER}
	 */
	public MqttServerList setMaxRetries(int maxRetries) {
		if (maxRetries < RETRIES_OF_FIRST_SERVER) {
			throw new IllegalArgumentException("maxRetries must be -1 or more, or RETRIES_OF_FIRST_SERVER");
		}
		this.maxRetries = maxRetries;
		return this;
	}

	/**
	 * Returns the retry limit for next-server mode, as set.
	 *
	 * @return the limit, {@code -1} for no limit, or {@link #RETRIES_OF_FIRST_SERVER}
	 */
	public int getMaxRetries() {
		return maxRetries;
	}

	/**
	 * Returns whether a first connect tries each broker once and then fails, whatever the retry
	 * limit, as Paho's connect with {@link MqttConnectOptions#setServerURIs(String[])} does.
	 *
	 * @return {@code true} for a list built from server URIs
	 */
	boolean isOnePassOnFirstConnect() {
		return onePassOnFirstConnect;
	}

	/**
	 * Builds the list Paho's {@link MqttConnectOptions#setServerURIs(String[])} describes: a copy
	 * of the options for each address, in next-server mode with round robin, as Paho moves through
	 * its list in order. As in Paho, a first connect tries each address once and then fails;
	 * after a lost connection the retry limit is the options' own.
	 *
	 * @param options options with two or more server URIs
	 * @return the list
	 */
	static MqttServerList fromServerURIs(MqttConnectOptions options) {
		String[] uris = options.getServerURIs();
		MqttServerList list = new MqttServerList();
		for (String uri : uris) {
			MqttConnectOptions copy = new MqttConnectOptions(options);
			copy.setServerURIs(null);
			copy.setServerURI(uri);
			list.add(copy);
		}
		list.onePassOnFirstConnect = true;
		return list;
	}
}
