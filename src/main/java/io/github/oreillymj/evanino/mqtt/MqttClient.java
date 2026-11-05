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
 * Contributors:
 *    Michael OReilly - initial API and implementation
 *******************************************************************************/

package io.github.oreillymj.evanino.mqtt;

import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.mqtt.MqttQoS;

import java.util.Arrays;

/**
 * Blocking MQTT client, modelled on Paho's {@code MqttClient}. Each operation delegates to
 * an internal {@link MqttAsyncClient} and waits for it to complete, for up to the time set
 * with {@link #setTimeToWait(long)} (30 seconds by default).
 */
public final class MqttClient implements IMqttClient {
	private final MqttAsyncClient aClient;
	private volatile long timeToWait = 30_000; // Paho's default


	/**
	 * Creates a client with the given connection options.
	 *
	 * @param options the connection options
	 */
	public MqttClient(MqttConnectOptions options) {
		this.aClient = new MqttAsyncClient(options);
	}

	/**
	 * Creates a client for the given broker and client ID, with otherwise default options;
	 * equivalent to Paho's {@code new MqttClient(serverURI, clientId)}. Unlike Paho, no message
	 * persistence is used: in-flight messages are kept in memory. To keep them across restarts,
	 * use {@link #MqttClient(String, String, MqttClientPersistence)} with a
	 * {@link MqttDefaultFilePersistence}.
	 *
	 * @param serverURI the URI of the broker, for example {@code tcp://host:1883}
	 * @param clientId  the client identifier; may be empty
	 * @throws MqttException declared for Paho compatibility; not currently thrown
	 * @throws IllegalArgumentException if {@code serverURI} is not a valid broker URI, or
	 *                                  {@code clientId} is {@code null} or longer than 65535
	 *                                  characters
	 */
	public MqttClient(String serverURI, String clientId) throws MqttException {
		this.aClient = new MqttAsyncClient(serverURI, clientId);
	}

	/**
	 * Creates a blocking client that keeps its session state in {@code persistence}, as Paho's
	 * constructor of the same form does; see
	 * {@link MqttAsyncClient#MqttAsyncClient(String, String, MqttClientPersistence)}.
	 *
	 * @param serverURI   the URI of the broker, for example {@code tcp://host:1883}
	 * @param clientId    the client identifier; may be empty
	 * @param persistence the store, or {@code null} to keep the state in memory only
	 * @throws MqttException declared for Paho compatibility; not currently thrown
	 */
	public MqttClient(String serverURI, String clientId, MqttClientPersistence persistence) throws MqttException {
		this.aClient = new MqttAsyncClient(serverURI, clientId, persistence);
	}

	/**
	 * Creates a client with default connection options.
	 */
	public MqttClient() {
		this.aClient = new MqttAsyncClient();
	}

	/**
	 * Sets the Netty event loop group used for connections. Call before connecting.
	 *
	 * @param eventLoop the event loop group to use
	 */
	public void setEventLoop(EventLoopGroup eventLoop) {
		this.aClient.setEventLoop(eventLoop);
		;
	}


	/**
	 * {@inheritDoc}
	 * <p>
	 * A non-null {@code options} replaces the client's current options, unless a connection
	 * attempt is already in progress.
	 */
	@Override
	public void connect(MqttConnectOptions options) throws MqttException {
		IMqttToken token = aClient.connect(options);
		if (token != null) {
			token.waitForCompletion(getTimeToWait());
		}
	}

	/**
	 * Connects to one of several brokers, for high availability, and waits until one accepts the
	 * connection or the client gives up; see {@link MqttAsyncClient#connect(MqttServerList)} and
	 * {@link MqttServerList}. The wait is limited by {@link #setTimeToWait(long)}, which covers the
	 * whole walk through the list, not each attempt; if it runs out, the walk carries on.
	 *
	 * @param servers the brokers
	 * @throws MqttException            if no broker accepted the connection, or the wait ran out
	 * @throws IllegalArgumentException if {@code servers} is {@code null} or empty
	 */
	public void connect(MqttServerList servers) throws MqttException {
		aClient.connect(servers).waitForCompletion(getTimeToWait());
	}

	/**
	 * {@inheritDoc}
	 *
	 * @throws IllegalStateException if no options or no host are configured
	 */
	@Override
	public void connect() throws MqttException {
		aClient.connect().waitForCompletion(getTimeToWait());
	}

	/**
	 * Connects to the broker and returns the completed token, so the connect result can be
	 * inspected. Blocks for up to the time set with {@link #setTimeToWait(long)}.
	 *
	 * @param options the connection options, or {@code null} to keep the current ones
	 * @return the completed connect token
	 * @throws MqttSecurityException if the broker rejects the credentials
	 * @throws MqttException if the connection fails or times out
	 */
	public IMqttToken connectWithResult(MqttConnectOptions options) throws MqttSecurityException, MqttException {
		IMqttToken token = aClient.connect(options, null, null);
		if (token != null) {
			token.waitForCompletion(getTimeToWait());
		}
		return token;
	}

	/**
	 * Disconnects and releases all client resources. The client cannot be used afterwards.
	 *
	 * @throws MqttException if the disconnect fails or times out
	 */
	public void shutdown() throws MqttException {
		aClient.shutdown();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void disconnect() throws MqttException {
		disconnect(aClient.defaultQuiesceTimeout());
	}

	/**
	 * Disconnects from the broker after quiescing for up to {@code quiesceTimeout} milliseconds:
	 * new work is refused, and in-flight messages are given that long to complete before
	 * DISCONNECT is sent; see {@link MqttAsyncClient#disconnect(long, Object, IMqttActionListener)}.
	 * Blocks for up to the quiesce timeout plus the time set with {@link #setTimeToWait(long)}.
	 *
	 * @param quiesceTimeout the longest time to wait for in-flight messages, in milliseconds
	 * @throws MqttException if the disconnect fails or times out
	 */
	public void disconnect(long quiesceTimeout) throws MqttException {
		aClient.disconnect(quiesceTimeout, null, null).waitForCompletion(Math.max(0, quiesceTimeout) + timeToWait);
	}

	/**
	 * Closes the connection immediately without sending DISCONNECT. Blocks for up to the time set with {@link #setTimeToWait(long)}.
	 * In {@link MqttConnectOptions#enablePahoMode(boolean) Paho mode} it does what Paho does:
	 * {@code disconnectForcibly(30000, 10000)}.
	 *
	 * @throws MqttException if the disconnect fails or times out
	 */
	public void disconnectForcibly() throws MqttException {
		long quiesceTimeout = aClient.defaultQuiesceTimeout();
		if (quiesceTimeout > 0) {
			disconnectForcibly(quiesceTimeout, MqttConnectOptions.PAHO_DISCONNECT_TIMEOUT);
		} else {
			aClient.disconnectForcibly().waitForCompletion(timeToWait);
		}
	}

	/**
	 * Sends DISCONNECT, then closes the connection whether or not it could be written; see
	 * {@link MqttAsyncClient#disconnectForcibly(long)}. Blocks for up to the disconnect timeout
	 * plus the time set with {@link #setTimeToWait(long)}. In
	 * {@link MqttConnectOptions#enablePahoMode(boolean) Paho mode} it quiesces for up to 30 s
	 * first, as Paho does, and blocks for that long too.
	 *
	 * @param disconnectTimeout the time in milliseconds to allow for writing DISCONNECT
	 * @throws MqttException if the disconnect fails or times out
	 */
	public void disconnectForcibly(long disconnectTimeout) throws MqttException {
		long quiesceTimeout = aClient.defaultQuiesceTimeout();
		aClient.disconnectForcibly(quiesceTimeout, disconnectTimeout).waitForCompletion(waitFor(quiesceTimeout, disconnectTimeout));
	}

	/**
	 * Quiesces, sends DISCONNECT, then closes the connection whether or not it could be
	 * written; see {@link MqttAsyncClient#disconnectForcibly(long, long)}. Blocks for up to both
	 * timeouts plus the time set with {@link #setTimeToWait(long)}.
	 *
	 * @param quiesceTimeout    the time in milliseconds to allow in-flight work to finish
	 * @param disconnectTimeout the time in milliseconds to allow for writing DISCONNECT
	 * @throws MqttException if the disconnect fails or times out
	 */
	public void disconnectForcibly(long quiesceTimeout, long disconnectTimeout) throws MqttException {
		aClient.disconnectForcibly(quiesceTimeout, disconnectTimeout).waitForCompletion(waitFor(quiesceTimeout, disconnectTimeout));
	}


	/**
	 * Disconnects from the server forcibly to reset all the states. Could be useful when disconnect attempt failed.
	 * <p>
	 * Because the client is able to establish the TCP/IP connection to a none MQTT server and it will certainly fail to
	 * send the disconnect packet.
	 * <p>
	 * Blocks until the channel has closed, for up to both timeouts plus the time set with
	 * {@link #setTimeToWait(long)}.
	 *
	 * @param quiesceTimeout       the amount of time in milliseconds to allow for existing work to finish before
	 *                             disconnecting; 0 or less does not quiesce
	 * @param disconnectTimeout    when {@code sendDisconnectPacket} is set, the time in milliseconds to allow for
	 *                             writing DISCONNECT before the channel is closed anyway; 0 or less waits for the write
	 * @param sendDisconnectPacket if true, will send the disconnect packet to the server
	 * @throws MqttException if any unexpected error
	 */
	public void disconnectForcibly(long quiesceTimeout, long disconnectTimeout, boolean sendDisconnectPacket) throws MqttException {
		aClient.disconnectForcibly(quiesceTimeout, disconnectTimeout, sendDisconnectPacket)
				.waitForCompletion(waitFor(quiesceTimeout, disconnectTimeout));
	}

	/**
	 * Returns how long to wait for a forced disconnect: its timeouts plus the time to wait.
	 *
	 * @param quiesceTimeout    the quiesce timeout, in milliseconds
	 * @param disconnectTimeout the disconnect timeout, in milliseconds
	 * @return the wait, in milliseconds
	 */
	private long waitFor(long quiesceTimeout, long disconnectTimeout) {
		return Math.max(0, quiesceTimeout) + Math.max(0, disconnectTimeout) + timeToWait;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void subscribe(String topic, int qos) throws MqttException {
		aClient.subscribe(topic, qos, null, null).waitForCompletion(timeToWait);
	}

	/**
	 * {@inheritDoc}
	 */
	public void subscribe(String topic, MqttQoS qos) throws MqttException {
		aClient.subscribe(topic, qos).waitForCompletion(timeToWait);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void subscribe(String topicFilter) throws MqttException {
		subscribe(topicFilter, 1);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void subscribe(String[] topicFilters) throws MqttException {
		subscribe(topicFilters, qosOne(topicFilters));
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void subscribe(String[] topicFilters, int[] qos) throws MqttException {
		aClient.subscribe(topicFilters, qos, null, null).waitForCompletion(timeToWait);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void subscribe(String topicFilter, IMqttMessageListener messageListener) throws MqttException {
		subscribe(topicFilter, 1, messageListener);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void subscribe(String topicFilter, int qos, IMqttMessageListener messageListener) throws MqttException {
		subscribeWithResponse(topicFilter, qos, messageListener);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void subscribe(String[] topicFilters, IMqttMessageListener[] messageListeners) throws MqttException {
		subscribe(topicFilters, qosOne(topicFilters), messageListeners);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void subscribe(String[] topicFilters, int[] qos, IMqttMessageListener[] messageListeners) throws MqttException {
		subscribeWithResponse(topicFilters, qos, messageListeners);
	}

	/**
	 * Subscribes to a topic filter, delivering messages to the given handler. Unlike the other
	 * subscribe methods, this does not wait for the broker to acknowledge the subscription.
	 *
	 * @param topic   the topic filter
	 * @param handler the callback for matching messages
	 * @param qos     the qos to request from the server: 0, 1 or 2
	 * @throws MqttException if the subscribe cannot be started
	 */
	public void subscribe(String topic, MqttCallback handler, int qos) throws MqttException {
		aClient.subscribe(topic, handler, qos);
	}

	/**
	 * Subscribes to several topic filters in a single SUBSCRIBE, each with its own listener,
	 * and returns the completed token so the granted QoS can be inspected. Blocks for up to the time set with {@link #setTimeToWait(long)}.
	 *
	 * @param topicFilters     the topic filters to subscribe to
	 * @param qos              the qos to request for each topic filter, in the same order
	 * @param messageListeners a listener per topic filter; a {@code null} array or entry uses the default callback
	 * @return the completed subscribe token
	 * @throws MqttException if the subscribe fails or times out
	 */
	public IMqttToken subscribeWithResponse(String[] topicFilters, int[] qos,
											IMqttMessageListener[] messageListeners) throws MqttException {
		IMqttToken tok = aClient.subscribe(topicFilters, qos, null, null, messageListeners);
		tok.waitForCompletion(timeToWait);   // throws if the SUBSCRIBE failed or timed out
		return tok;
	}

	/**
	 * Subscribes to a topic filter and returns the completed token so the granted QoS can be
	 * inspected. Blocks for up to the time set with {@link #setTimeToWait(long)}.
	 *
	 * @param topicFilter the topic filter
	 * @param qos         the qos to request from the server: 0, 1 or 2
	 * @return the completed subscribe token
	 * @throws MqttException if the subscribe fails or times out
	 */
	public IMqttToken subscribeWithResponse(String topicFilter, int qos) throws MqttException {
		IMqttToken tok = aClient.subscribe(topicFilter, qos, null, null);
		tok.waitForCompletion(timeToWait);
		return tok;
	}

	/**
	 * Subscribes to a topic filter at QoS 1 and returns the completed token so the granted QoS
	 * can be inspected. Blocks for up to the time set with {@link #setTimeToWait(long)}.
	 *
	 * @param topicFilter the topic filter
	 * @return the completed subscribe token
	 * @throws MqttException if the subscribe fails or times out
	 */
	public IMqttToken subscribeWithResponse(String topicFilter) throws MqttException {
		return subscribeWithResponse(topicFilter, 1);
	}

	/**
	 * Subscribes to a topic filter at QoS 1, delivering messages to the given listener, and
	 * returns the completed token. Blocks for up to the time set with
	 * {@link #setTimeToWait(long)}.
	 *
	 * @param topicFilter     the topic filter
	 * @param messageListener the listener for matching messages; {@code null} uses the default callback
	 * @return the completed subscribe token
	 * @throws MqttException if the subscribe fails or times out
	 */
	public IMqttToken subscribeWithResponse(String topicFilter, IMqttMessageListener messageListener) throws MqttException {
		return subscribeWithResponse(topicFilter, 1, messageListener);
	}

	/**
	 * Subscribes to a topic filter, delivering messages to the given listener, and returns the
	 * completed token. Blocks for up to the time set with {@link #setTimeToWait(long)}.
	 *
	 * @param topicFilter     the topic filter
	 * @param qos             the qos to request from the server: 0, 1 or 2
	 * @param messageListener the listener for matching messages; {@code null} uses the default callback
	 * @return the completed subscribe token
	 * @throws MqttException if the subscribe fails or times out
	 */
	public IMqttToken subscribeWithResponse(String topicFilter, int qos, IMqttMessageListener messageListener) throws MqttException {
		IMqttToken tok = aClient.subscribe(topicFilter, qos, messageListener);
		tok.waitForCompletion(timeToWait);
		return tok;
	}

	/**
	 * Subscribes to several topic filters at QoS 1 in a single SUBSCRIBE, and returns the
	 * completed token. Blocks for up to the time set with {@link #setTimeToWait(long)}.
	 *
	 * @param topicFilters the topic filters to subscribe to
	 * @return the completed subscribe token
	 * @throws MqttException if the subscribe fails or times out
	 */
	public IMqttToken subscribeWithResponse(String[] topicFilters) throws MqttException {
		return subscribeWithResponse(topicFilters, qosOne(topicFilters));
	}

	/**
	 * Subscribes to several topic filters in a single SUBSCRIBE, and returns the completed
	 * token. Blocks for up to the time set with {@link #setTimeToWait(long)}.
	 *
	 * @param topicFilters the topic filters to subscribe to
	 * @param qos          the qos to request for each topic filter, in the same order
	 * @return the completed subscribe token
	 * @throws MqttException if the subscribe fails or times out
	 */
	public IMqttToken subscribeWithResponse(String[] topicFilters, int[] qos) throws MqttException {
		IMqttToken tok = aClient.subscribe(topicFilters, qos, null, null);
		tok.waitForCompletion(timeToWait);
		return tok;
	}

	/**
	 * Subscribes to several topic filters at QoS 1 in a single SUBSCRIBE, each with its own
	 * listener, and returns the completed token. Blocks for up to the time set with
	 * {@link #setTimeToWait(long)}.
	 *
	 * @param topicFilters     the topic filters to subscribe to
	 * @param messageListeners a listener per topic filter; a {@code null} array or entry uses the default callback
	 * @return the completed subscribe token
	 * @throws MqttException if the subscribe fails or times out
	 */
	public IMqttToken subscribeWithResponse(String[] topicFilters, IMqttMessageListener[] messageListeners) throws MqttException {
		return subscribeWithResponse(topicFilters, qosOne(topicFilters), messageListeners);
	}

	/**
	 * Returns a QoS array of the same length as {@code topicFilters}, with every entry 1.
	 *
	 * @param topicFilters the topic filters
	 * @return the QoS array
	 */
	private static int[] qosOne(String[] topicFilters) {
		int[] qos = new int[topicFilters.length];
		Arrays.fill(qos, 1);
		return qos;
	}

	//subscribeOnce/unsubscribe(topic, handler) don't get blocking stubs —
	//they only make sense with a per-call handler/callback, which is fundamentally an async concept
	//(same reasoning as getTopic() living on IMqttAsyncClient only).
	//
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void unsubscribe(String topic) throws MqttException {
		aClient.unsubscribe(topic).waitForCompletion(timeToWait);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void unsubscribe(String[] topicFilters) throws MqttException {
		aClient.unsubscribe(topicFilters).waitForCompletion(timeToWait);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void publish(String topic, MqttMessage message) throws MqttException {
		aClient.publish(topic, message, null, null).waitForCompletion(timeToWait);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void publish(String topic, String payload) throws MqttException {
		aClient.publish(topic, payload).waitForCompletion(timeToWait);
	}

	/**
	 * {@inheritDoc}
	 */
	public void publish(String topic, String payload, int qos) throws MqttException {
		aClient.publish(topic, payload, qos).waitForCompletion(timeToWait);
	}

	/**
	 * {@inheritDoc}
	 */
	public void publish(String topic, String payload, int qos, boolean retain) throws MqttException {
		aClient.publish(topic, payload, qos, retain).waitForCompletion(timeToWait);
	}

	/**
	 * Publishes a binary payload at QoS 0, not retained. Blocks for up to the time set with {@link #setTimeToWait(long)}.
	 *
	 * @param topic   the topic to publish to
	 * @param payload the message payload
	 * @throws MqttException if the publish fails or times out
	 */
	public void publish(String topic, byte[] payload) throws MqttException {
		aClient.publish(topic, payload).waitForCompletion(timeToWait);
	}

	/**
	 * Publishes a binary payload at QoS 0. Blocks for up to the time set with {@link #setTimeToWait(long)}.
	 *
	 * @param topic   the topic to publish to
	 * @param payload the message payload
	 * @param retain  whether the broker should retain the message
	 * @throws MqttException if the publish fails or times out
	 */
	public void publish(String topic, byte[] payload, boolean retain) throws MqttException {
		aClient.publish(topic, payload, retain).waitForCompletion(timeToWait);
	}

	/**
	 * Publishes a binary payload, not retained. Blocks for up to the time set with {@link #setTimeToWait(long)}.
	 *
	 * @param topic   the topic to publish to
	 * @param payload the message payload
	 * @param qos     the QoS level (0, 1 or 2)
	 * @throws MqttException if the publish fails or times out
	 */
	public void publish(String topic, byte[] payload, int qos) throws MqttException {
		aClient.publish(topic, payload, qos).waitForCompletion(timeToWait);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void publish(String topic, byte[] payload, int qos, boolean retain) throws MqttException {
		aClient.publish(topic, payload, qos, retain).waitForCompletion(timeToWait);
	}

	/**
	 * Publishes a binary payload. Blocks for up to the time set with {@link #setTimeToWait(long)}.
	 *
	 * @param topic   the topic to publish to
	 * @param payload the message payload
	 * @param qos     the QoS level
	 * @param retain  whether the broker should retain the message
	 * @throws MqttException if the publish fails or times out
	 */
	public void publish(String topic, byte[] payload, MqttQoS qos, boolean retain) throws MqttException {
		aClient.publish(topic, payload, qos, retain).waitForCompletion(timeToWait);
	}

	/**
	 * Publishes a buffer payload at QoS 0, not retained. The buffer is released. Blocks for up to the time set with {@link #setTimeToWait(long)}.
	 *
	 * @param topic   the topic to publish to
	 * @param payload the message payload
	 * @throws MqttException if the publish fails or times out
	 */
	public void publish(String topic, ByteBuf payload) throws MqttException {
		aClient.publish(topic, payload).waitForCompletion(timeToWait);
	}

	/**
	 * Publishes a buffer payload, not retained. The buffer is released. Blocks for up to the time set with {@link #setTimeToWait(long)}.
	 *
	 * @param topic   the topic to publish to
	 * @param payload the message payload
	 * @param qos     the QoS level
	 * @throws MqttException if the publish fails or times out
	 */
	public void publish(String topic, ByteBuf payload, MqttQoS qos) throws MqttException {
		aClient.publish(topic, payload, qos).waitForCompletion(timeToWait);
	}

	/**
	 * Publishes a buffer payload at QoS 0. The buffer is released. Blocks for up to the time set with {@link #setTimeToWait(long)}.
	 *
	 * @param topic   the topic to publish to
	 * @param payload the message payload
	 * @param retain  whether the broker should retain the message
	 * @throws MqttException if the publish fails or times out
	 */
	public void publish(String topic, ByteBuf payload, boolean retain) throws MqttException {
		aClient.publish(topic, payload, retain).waitForCompletion(timeToWait);
	}

	/**
	 * Publishes a buffer payload. The buffer is released. Blocks for up to the time set with {@link #setTimeToWait(long)}.
	 *
	 * @param topic   the topic to publish to
	 * @param payload the message payload
	 * @param qos     the QoS level (0, 1 or 2)
	 * @param retain  whether the broker should retain the message
	 * @throws MqttException if the publish fails or times out
	 */
	public void publish(String topic, ByteBuf payload, int qos, boolean retain) throws MqttException {
		aClient.publish(topic, payload, qos, retain).waitForCompletion(timeToWait);
	}

	/**
	 * Publishes a buffer payload. The buffer is released. Blocks for up to the time set with {@link #setTimeToWait(long)}.
	 *
	 * @param topic   the topic to publish to
	 * @param payload the message payload
	 * @param qos     the QoS level
	 * @param retain  whether the broker should retain the message
	 * @throws MqttException if the publish fails or times out
	 */
	public void publish(String topic, ByteBuf payload, MqttQoS qos, boolean retain) throws MqttException {
		aClient.publish(topic, payload, qos, retain).waitForCompletion(timeToWait);
	}

	/**
	 * Sends a PINGREQ to the broker without waiting for the response.
	 *
	 * @throws MqttException declared for compatibility; not currently thrown
	 */
	public void sendPing() throws MqttException {
		aClient.sendPing();
	}


	/**
	 * {@inheritDoc}
	 * <p>
	 * The default is 30 seconds.
	 *
	 * @param timeToWaitInMillis the maximum wait in milliseconds, or {@code -1} to wait without a limit
	 * @throws IllegalArgumentException if the value is less than {@code -1}
	 */
	public void setTimeToWait(long timeToWaitInMillis) {
		if (timeToWaitInMillis < -1) {
			throw new IllegalArgumentException();
		}
		this.timeToWait = timeToWaitInMillis;
	}

	/**
	 * Return the maximum time to wait for an action to complete.
	 *
	 * @return the time to wait
	 * @see MqttClient#setTimeToWait(long)
	 */
	public long getTimeToWait() {
		return this.timeToWait;
	}

	/**
	 * Releases all client resources, disconnecting first. Equivalent to {@link #shutdown()}.
	 *
	 * @throws MqttException if the disconnect fails or times out
	 */
	public void close() throws MqttException {
		aClient.shutdown();
	}

	/**
	 * Releases all client resources. Without {@code force}, this is {@link #close()}: a clean
	 * disconnect first. With {@code force}, the connection is closed at once without
	 * DISCONNECT, and the resources are released even if closing fails, as Paho's
	 * {@code close(true)} closes the client whether or not it is connected. The client cannot be
	 * used afterwards.
	 *
	 * @param force whether to close the connection forcibly instead of disconnecting cleanly
	 * @throws MqttException if a clean disconnect fails or times out
	 */
	public void close(boolean force) throws MqttException {
		aClient.shutdown(force);
	}

	/**
	 * Returns the client library version.
	 *
	 * @return the version in {@code major.minor.build} form
	 */
	public String getVersion() {
		return aClient.getVersion();
	}

	/**
	 * Returns the major version of the client library.
	 *
	 * @return the major version
	 */
	public String getMajorVersion() {
		return aClient.getMajorVersion();
	}

	/**
	 * Returns the minor version of the client library.
	 *
	 * @return the minor version
	 */
	public String getMinorVersion() {
		return aClient.getMinorVersion();
	}

	/**
	 * Returns the build number of the client library.
	 *
	 * @return the build number
	 */
	public String getRevision() {
		return aClient.getRevision();
	}


	/**
	 * Turns debug logging on or off.
	 *
	 * @param enable whether to log client activity
	 */
	public void enableDebugOutput(boolean enable) {
		aClient.enableDebugOutput(enable);
	}

	/**
	 * Turns debug logging on or off for the client, channel and ping handling.
	 *
	 * @param enable        whether to log client activity
	 * @param enablechannel whether to log channel activity
	 * @param enableping    whether to log keep-alive pings
	 */
	public void enableDebugOutput(boolean enable, boolean enablechannel, boolean enableping) {
		aClient.enableDebugOutput(enable, enablechannel, enableping);
	}

	/**
	 * Returns the client identifier.
	 *
	 * @return the client id
	 */
	public String getClientId() {
		return aClient.getClientId();
	}

	/**
	 * Returns the client identifier. Equivalent to {@link #getClientId()}.
	 *
	 * @return the client id
	 */
	public String getClientID() {
		return aClient.getClientId();
	}


	/**
	 * Returns the unique identifier of the underlying client instance, passed to callbacks as {@code uuid}.
	 *
	 * @return the instance identifier
	 */
	public String getInstanceID() {
		return aClient.getInstanceID();
	}

	/**
	 * Returns the connection options. While a connection attempt is in progress, a new default
	 * options object is returned instead, so changes made to it have no effect.
	 *
	 * @return the connection options
	 */
	public MqttConnectOptions getOptions() {
		return aClient.getOptions();
	}

	/**
	 * Replaces the connection options. Takes effect on the next connect.
	 *
	 * @param options the options to use; {@code null} is ignored
	 */
	public void setOptions(MqttConnectOptions options) {
		if ((aClient != null) && (options != null)) {
			aClient.setOptions(options);
		}
	}

	/**
	 * Returns the delivery tokens for publishes that have not yet completed.
	 *
	 * @return the pending delivery tokens
	 */
	public IMqttDeliveryToken[] getPendingDeliveryTokens() {
		return aClient.getPendingDeliveryTokens();
	}

	/**
	 * Returns the URI of the broker this client connects to.
	 *
	 * @return the server URI, or {@code "unknown"} if none is configured
	 */
	public String getServerURI() {
		return aClient.getServerURI();
	}

	/**
	 * Returns the currently connected Server URI.
	 * Implemented due to: https://bugs.eclipse.org/bugs/show_bug.cgi?id=481097
	 * <p>
	 * In Paho this can differ from {@link #getServerURI()} when several server URIs are
	 * configured. This client supports a single server URI, so the two are always equal.
	 *
	 * @return the currently connected server URI
	 */
	public String getCurrentServerURI() {
		return aClient.getCurrentServerURI();
	}

	/**
	 * Returns the number of automatic reconnect attempts since the last successful connection.
	 *
	 * @return the retry count
	 */
	public int getConnectRetryCount() {
		return aClient.getConnectRetryCount();
	}

	/**
	 * Returns the maximum number of automatic reconnect attempts.
	 *
	 * @return the configured maximum, or {@code -1} for no limit
	 */
	public int getMaxConnectRetries() {
		return aClient.getMaxConnectRetries();
	}

	/**
	 * Returns a topic object bound to the underlying client, for publishing to a single topic.
	 *
	 * @param topic the topic name
	 * @return the topic
	 * @throws IllegalArgumentException if the topic is empty or contains wildcards
	 */
	public MqttTopic getTopic(String topic) {
		return aClient.getTopic(topic);
	}

	/**
	 * Returns whether the client is connected to the broker.
	 *
	 * @return {@code true} if a CONNACK has been received and the channel is active
	 */
	public boolean isConnected() {
		return aClient.isConnected();
	}

	/**
	 * Returns whether a connection attempt is in progress.
	 *
	 * @return {@code true} while connecting
	 */
	public boolean isConnecting() {
		return aClient.isConnecting();
	}

	/**
	 * Returns whether the client is reconnecting automatically after losing its connection,
	 * including while waiting between attempts; see {@link MqttAsyncClient#isReconnecting()}.
	 *
	 * @return {@code true} while automatic reconnect is under way
	 */
	public boolean isReconnecting() {
		return aClient.isReconnecting();
	}

	/**
	 * Sets the callback for connection events and incoming messages. Ignored while a
	 * connection attempt is in progress.
	 *
	 * @param callback the callback
	 */
	public void setCallback(MqttCallback callback) {
		aClient.setCallback(callback);
	}

	/**
	 * Sets whether incoming messages must be acknowledged explicitly.
	 *
	 * @param manualAcks {@code true} to enable manual acknowledgements
	 */
	public void setManualAcks(boolean manualAcks) {
		aClient.setManualAcks(manualAcks);
	}

	/**
	 * Returns whether manual acknowledgements are enabled.
	 *
	 * @return {@code true} if incoming messages must be acknowledged explicitly
	 */
	public boolean isManualAcks() {
		return aClient.isManualAcks();
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Does nothing.
	 */
	@Override
	public void test() {
		// Do nothing

	}

	/**
	 * Acknowledges an incoming message when manual acknowledgements are enabled.
	 * <p>
	 * Delegates to {@link MqttAsyncClient#messageArrivedComplete(int, int)}; does nothing if
	 * manual acknowledgements are disabled.
	 *
	 * @param messageId the packet identifier of the received message
	 * @param qos       the QoS of the received message
	 * @throws MqttException if manual acknowledgements are enabled and the client is not connected
	 */
	public void messageArrivedComplete(int messageId, int qos) throws MqttException {
		aClient.messageArrivedComplete(messageId, qos);
	}

	/**
	 * Returns a new randomly generated client identifier, as Paho's static method does; see
	 * {@link MqttAsyncClient#generateClientId()}.
	 * <p>When cleanSession is set to false, an application must ensure it uses the
	 * same client identifier when it reconnects to the server to resume state and maintain
	 * assured message delivery, so a generated identifier should be kept, not regenerated.</p>
	 *
	 * @return a new client identifier
	 * @see MqttConnectOptions#setCleanSession(boolean)
	 */
	public static String generateClientId() {
		return MqttAsyncClient.generateClientId();
	}

	/**
	 * Will attempt to reconnect to the server after the client has lost connection.
	 * Returns as soon as the attempt has started, without waiting for it to complete.
	 *
	 * @throws MqttException if an error occurs attempting to reconnect
	 */
	public void reconnect() throws MqttException {
		aClient.reconnect();
	}


	/**
	 * Returns the underlying asynchronous client.
	 *
	 * @return the async client
	 */
	public IMqttAsyncClient getAsyncClient() {
		return aClient;
	}
}//class
