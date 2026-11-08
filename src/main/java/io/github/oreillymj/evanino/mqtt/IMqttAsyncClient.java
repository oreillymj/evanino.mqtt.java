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
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.mqtt.MqttQoS;

/**
 * Non-blocking MQTT client API, modelled on Paho's {@code IMqttAsyncClient}. Operations
 * return immediately with a token that completes when the broker acknowledges the action.
 * <p>
 * Incoming messages for subscriptions made without a per-subscription listener are delivered
 * to the {@link MqttCallback} set with {@link #setCallback(MqttCallback)}.
 */
public interface IMqttAsyncClient {

	/**
	 * Returns the URI of the broker this client connects to.
	 *
	 * @return the server URI, or {@code "unknown"} if none is configured
	 */
	String getServerURI();


	/**
	 * Connect to the broker configured in this client's {@link MqttConnectOptions}.
	 *
	 * @return a token which will be completed when the connection is opened and a CONNACK has been received
	 * @throws IllegalStateException if no options or host are configured
	 */
	IMqttToken connect();

	/**
	 * Connect to the broker using the given options.
	 *
	 * @param options the connection options
	 * @return a token which will be completed when the connection is opened and a CONNACK has been received
	 */
	IMqttToken connect(MqttConnectOptions options);

	/**
	 * Connect to the broker using the given options, notifying a listener when done.
	 *
	 * @param options     the connection options
	 * @param userContext an object attached to the returned token, or {@code null}
	 * @param callback    a listener notified when the connect completes or fails, or {@code null}
	 * @return a token which will be completed when the connection is opened and a CONNACK has been received
	 */
	IMqttToken connect(MqttConnectOptions options, Object userContext, IMqttActionListener callback);

	/**
	 * Connect to the broker using the client's current options, notifying a listener when done.
	 *
	 * @param userContext an object attached to the returned token, or {@code null}
	 * @param callback    a listener notified when the connect completes or fails, or {@code null}
	 * @return a token which will be completed when the connection is established or fails
	 */
	IMqttToken connect(Object userContext, IMqttActionListener callback);

	/**
	 * Acknowledges an incoming message when manual acknowledgements are enabled with
	 * {@link MqttConnectOptions#setManualAcks(boolean)}. Does nothing otherwise.
	 *
	 * @param messageId the packet identifier of the received message
	 * @param qos       the QoS of the received message
	 * @throws MqttException if the client is not connected
	 */
	void messageArrivedComplete(int messageId, int qos) throws MqttException;

	/**
	 * Subscribe on the given topic filter, with the given qos. Messages are delivered to the
	 * client's default {@link MqttCallback}.
	 *
	 * @param topic       the topic filter to subscribe to
	 * @param qos         the qos to request from the server: 0, 1 or 2
	 * @param userContext an object attached to the returned token, or {@code null}
	 * @param callback    a listener notified when the subscribe completes or fails, or {@code null}
	 * @return a token which will be completed when the server acknowledges the subscribe request
	 * @throws MqttException if the subscribe cannot be started
	 */
	IMqttToken subscribe(String topic, int qos, Object userContext, IMqttActionListener callback) throws MqttException;

	/**
	 * Subscribe on the given topic filter, with the given qos, delivering matching messages to
	 * the given listener rather than the client's default {@link MqttCallback}.
	 *
	 * @param topic           the topic filter to subscribe to
	 * @param qos             the qos to request from the server: 0, 1 or 2
	 * @param messageListener the listener for matching messages; {@code null} uses the default
	 *                        callback
	 * @return a token which will be completed when the server acknowledges the subscribe request
	 * @throws MqttException if the subscribe cannot be started
	 */
	IMqttToken subscribe(String topic, int qos, IMqttMessageListener messageListener) throws MqttException;

	/**
	 * Subscribe on several topic filters in a single SUBSCRIBE, each with its own listener.
	 *
	 * @param topicFilters     the topic filters to subscribe to
	 * @param qos              the qos to request for each topic filter, in the same order
	 * @param messageListeners a listener per topic filter; a {@code null} array or entry uses
	 *                         the default callback
	 * @return a token which will be completed when the server acknowledges the subscribe request
	 * @throws MqttException if the subscribe cannot be started
	 */
	IMqttToken subscribe(String[] topicFilters, int[] qos, IMqttMessageListener[] messageListeners) throws MqttException;

	/**
	 * Publish a message, using the QoS and retain flag set on the message.
	 *
	 * @param topic       the topic to publish to
	 * @param message     the message to publish
	 * @param userContext an object attached to the returned token, or {@code null}
	 * @param callback    a listener notified when delivery completes or fails, or {@code null}
	 * @return a token which will be completed when delivery completes for the message's QoS
	 * @throws MqttException if the publish cannot be started
	 */
	IMqttDeliveryToken publish(String topic, MqttMessage message, Object userContext, IMqttActionListener callback) throws MqttException;

	/**
	 * Publish a payload to the given topic, notifying a listener when delivery completes.
	 *
	 * @param topic       the topic to publish to
	 * @param payload     the message payload
	 * @param qos         the QoS: 0, 1 or 2
	 * @param retained    whether the broker should retain the message
	 * @param userContext an object attached to the returned token, or {@code null}
	 * @param callback    a listener notified when delivery completes or fails, or {@code null}
	 * @return a token which will be completed when delivery completes for the QoS
	 * @throws MqttException if the publish cannot be started
	 */
	IMqttDeliveryToken publish(String topic, byte[] payload, int qos, boolean retained, Object userContext,
							   IMqttActionListener callback) throws MqttException;

	/**
	 * Send DISCONNECT and close the channel. Automatic reconnection is disabled until the
	 * next explicit connect. With a positive {@code quiesceTimeout}, new work is refused with
	 * {@link MqttException#REASON_CODE_CLIENT_DISCONNECTING} and in-flight messages are given up
	 * to that long to complete before DISCONNECT is sent.
	 *
	 * @param quiesceTimeout time in milliseconds to allow in-flight work to finish; 0 or less
	 *                       disconnects at once
	 * @param userContext    an object attached to the returned token, or {@code null}
	 * @param callback       a listener notified when the disconnect completes, or {@code null}
	 * @return a token which will be completed when the channel has closed
	 */
	IMqttToken disconnect(long quiesceTimeout, Object userContext, IMqttActionListener callback);

	/**
	 * Connect to the specified hostname/ip, using the port already configured in this
	 * client's options unless {@code host} includes a port of its own.
	 * If you want to change the port number, see {@link #connect(String, int)}
	 *
	 * @param host The ip address or host to connect to
	 * @return a token which will be completed when the connection is opened and a CONNACK has been received
	 */
	IMqttToken connect(String host);

	/**
	 * Connect to the specified hostname/ip using the specified port, which takes precedence
	 * over any port included in {@code host}.
	 *
	 * @param host The ip address or host to connect to
	 * @param port The tcp port to connect to
	 * @return a token which will be completed when the connection is opened and a CONNACK has been received
	 */
	IMqttToken connect(String host, int port);

	/**
	 * Returns whether the client is connected to the broker.
	 *
	 * @return boolean value indicating if a CONNACK has been received and the channel is active
	 */
	boolean isConnected();


	/**
	 * Returns whether a connection attempt is in progress.
	 *
	 * @return boolean value indicating if connection attempt in progress
	 */
	boolean isConnecting();

	/**
	 * Attempt to reconnect to the host and port in this client's options
	 *
	 * @return a token which will be completed when the connection is opened and a CONNACK has been received
	 * @throws IllegalStateException if no host has been configured
	 */
	IMqttToken reconnect();

	/**
	 * Retrieve the netty {@link EventLoopGroup} we are using
	 *
	 * @return The netty {@link EventLoopGroup} we use for the connection
	 */
	EventLoopGroup getEventLoop();

	/**
	 * Sets the Netty {@link EventLoopGroup} the client connects on. By default all clients share
	 * one NIO event loop group. If you give the client a group of another type, such as epoll,
	 * make sure to change the {@link Channel} class using
	 * {@link MqttConnectOptions#setChannelClass(Class)}.
	 * Call this before connecting. The group stays the caller's: shutting the client down does
	 * not shut it down.
	 *
	 * @param eventLoop The new eventloop to use
	 */
	void setEventLoop(EventLoopGroup eventLoop);

	/**
	 * Subscribe on the given topic filter, with the given qos. Messages are delivered to the
	 * client's default {@link MqttCallback}.
	 *
	 * @param topic The topic filter to subscribe to
	 * @param qos   The qos to request from the server: 0, 1 or 2
	 * @return a token which will be completed when the server acknowledges the subscribe request
	 * @throws MqttException if the subscribe cannot be started
	 */
	IMqttToken subscribe(String topic, int qos) throws MqttException;


	/**
	 * Subscribe on the given topic filter, with the given qos. Messages are delivered to the
	 * client's default {@link MqttCallback}.
	 *
	 * @param topic The topic filter to subscribe to
	 * @param qos   The qos to request from the server
	 * @return a token which will be completed when the server acknowledges the subscribe request
	 * @throws MqttException if the subscribe cannot be started
	 */
	IMqttToken subscribe(String topic, MqttQoS qos) throws MqttException;



	/**
	 * Subscribe on the given topic filter at QoS 0. Messages are delivered to the client's
	 * default {@link MqttCallback}.
	 * This subscription is only once. If the MqttClient has received 1 message, the subscription will be removed
	 *
	 * @param topic The topic filter to subscribe to
	 * @return a token which will be completed when the server acknowledges the subscribe request
	 * @throws MqttException if the subscribe cannot be started
	 */
	IMqttToken subscribeOnce(String topic) throws MqttException;

	/**
	 * Subscribe on several topic filters in a single SUBSCRIBE packet. Messages are delivered
	 * to the client's default {@link MqttCallback}.
	 *
	 * @param topicFilters The topic filters to subscribe to
	 * @param qos          The qos to request for each topic filter, in the same order
	 * @param userContext  an object attached to the returned token, or {@code null}
	 * @param callback     a listener notified when the subscribe completes or fails, or {@code null}
	 * @return a token which will be completed when the server acknowledges the subscribe request
	 * @throws MqttException if the subscribe cannot be started
	 * @throws IllegalArgumentException if the arrays differ in length
	 */
	IMqttToken subscribe(String[] topicFilters, int[] qos, Object userContext, IMqttActionListener callback) throws MqttException;

	/**
	 * Subscribe on several topic filters in a single SUBSCRIBE packet. Messages are delivered
	 * to the client's default {@link MqttCallback}.
	 *
	 * @param topicFilters A String array of topic filter to subscribe to
	 * @param qos          An array of qos integers, one per topic filter
	 * @return a token which will be completed when the server acknowledges the subscribe request
	 * @throws MqttException if the subscribe cannot be started
	 * @throws IllegalArgumentException if the arrays differ in length
	 */
	IMqttToken subscribe(String[] topicFilters, int[] qos) throws MqttException;

	/**
	 * Remove the subscription for the given topic and handler. An UNSUBSCRIBE is only sent to
	 * the broker once no handlers remain for the topic.
	 * If you want to unsubscribe from all handlers known for this topic, use {@link #unsubscribe(String)}
	 *
	 * @param topic   The topic to unsubscribe for
	 * @param handler The handler to unsubscribe
	 * @return a token which will be completed when the server acknowledges the unsubscribe
	 *         request, or immediately if no UNSUBSCRIBE is needed
	 * @throws MqttException if the unsubscribe cannot be started
	 */
	IMqttToken unsubscribe(String topic, MqttCallback handler) throws MqttException;

	/**
	 * Remove all subscriptions for the given topic.
	 * If you want to specify which handler to unsubscribe, use {@link #unsubscribe(String, MqttCallback)}
	 *
	 * @param topic The topic to unsubscribe for
	 * @return a token which will be completed when the server acknowledges the unsubscribe
	 *         request, or immediately if no UNSUBSCRIBE is needed
	 * @throws MqttException if the unsubscribe cannot be started
	 */
	IMqttToken unsubscribe(String topic) throws MqttException;

	/**
	 * Remove all subscriptions for the given topic, notifying a listener when done.
	 *
	 * @param topic       The topic to unsubscribe for
	 * @param userContext an object attached to the returned token, or {@code null}
	 * @param callback    a listener notified when the unsubscribe completes or fails, or {@code null}
	 * @return a token which will be completed when the server acknowledges the unsubscribe
	 *         request, or immediately if no UNSUBSCRIBE is needed
	 * @throws MqttException if the unsubscribe cannot be started
	 */
	IMqttToken unsubscribe(String topic, Object userContext, IMqttActionListener callback) throws MqttException;

	/**
	 * Remove all subscriptions for the given topics, sending a single UNSUBSCRIBE for the
	 * topics that are subscribed on the broker.
	 *
	 * @param topicFilters The topics to unsubscribe for
	 * @param userContext  an object attached to the returned token, or {@code null}
	 * @param callback     a listener notified when the unsubscribe completes or fails, or {@code null}
	 * @return a token which will be completed when the server acknowledges the unsubscribe
	 *         request, or immediately if no UNSUBSCRIBE is needed
	 * @throws MqttException if the unsubscribe cannot be started
	 */
	IMqttToken unsubscribe(String[] topicFilters, Object userContext, IMqttActionListener callback) throws MqttException;

	/**
	 * Remove all subscriptions for the given topics, sending a single UNSUBSCRIBE for the
	 * topics that are subscribed on the broker.
	 *
	 * @param topicFilters The topics to unsubscribe for
	 * @return a token which will be completed when the server acknowledges the unsubscribe
	 *         request, or immediately if no UNSUBSCRIBE is needed
	 * @throws MqttException if the unsubscribe cannot be started
	 */
	IMqttToken unsubscribe(String[] topicFilters) throws MqttException;


	/**
	 * Publish a payload to the given topic at QoS 0, not retained. The buffer is consumed and
	 * released by this call.
	 *
	 * @param topic   The topic to publish to
	 * @param payload The payload to send
	 * @return a token which will be completed when the message has been written to the network
	 * @throws MqttException if the publish cannot be started
	 */
	IMqttDeliveryToken publish(String topic, ByteBuf payload) throws MqttException;

	/**
	 * Publish a payload to the given topic, using the given qos, not retained. The buffer is
	 * consumed and released by this call.
	 *
	 * @param topic   The topic to publish to
	 * @param payload The payload to send
	 * @param qos     The qos to use while publishing
	 * @return a token which will be completed when delivery completes for the given QoS
	 * @throws MqttException if the publish cannot be started
	 */
	IMqttDeliveryToken publish(String topic, ByteBuf payload, MqttQoS qos) throws MqttException;

	/**
	 * Publish a payload to the given topic at QoS 0, using optional retain. The buffer is
	 * consumed and released by this call.
	 *
	 * @param topic   The topic to publish to
	 * @param payload The payload to send
	 * @param retain  true if you want to retain the message on the server, false otherwise
	 * @return a token which will be completed when the message has been written to the network
	 * @throws MqttException if the publish cannot be started
	 */
	IMqttDeliveryToken publish(String topic, ByteBuf payload, boolean retain) throws MqttException;

	/**
	 * Publish a payload to the given topic, using the given qos and optional retain. The
	 * buffer is consumed and released by this call.
	 *
	 * @param topic   The topic to publish to
	 * @param payload The payload to send
	 * @param qos     The qos to use while publishing
	 * @param retain  true if you want to retain the message on the server, false otherwise
	 * @return a token which will be completed when delivery completes for the given QoS
	 * @throws MqttException if the publish cannot be started
	 */
	IMqttDeliveryToken publish(String topic, ByteBuf payload, MqttQoS qos, boolean retain) throws MqttException;


	/**
	 * Publish a payload to the given topic, using the given qos and optional retain. The
	 * buffer is consumed and released by this call.
	 *
	 * @param topic   The topic to publish to
	 * @param payload The payload to send
	 * @param qos     The qos to use while publishing: 0, 1 or 2
	 * @param retain  true if you want to retain the message on the server, false otherwise
	 * @return a token which will be completed when delivery completes for the given QoS
	 * @throws MqttException if the publish cannot be started
	 */
	IMqttDeliveryToken publish(String topic, ByteBuf payload, int qos, boolean retain) throws MqttException;


	/**
	 * Publish a UTF-8 string payload to the given topic, using the given qos, not retained.
	 *
	 * @param topic   The topic to publish to
	 * @param payload The payload to send
	 * @param qos     The qos to use while publishing: 0, 1 or 2
	 * @return a token which will be completed when delivery completes for the given QoS
	 * @throws MqttException if the publish cannot be started
	 */
	IMqttDeliveryToken publish(String topic, String payload, int qos) throws MqttException;

	/**
	 * Publish a UTF-8 string payload to the given topic at QoS 0, not retained.
	 *
	 * @param topic   The topic to publish to
	 * @param payload The payload to send
	 * @return a token which will be completed when the message has been written to the network
	 * @throws MqttException if the publish cannot be started
	 */
	IMqttDeliveryToken publish(String topic, String payload) throws MqttException;

	/**
	 * Publish a UTF-8 string payload to the given topic, using the given qos and optional retain.
	 *
	 * @param topic   The topic to publish to
	 * @param payload The payload to send
	 * @param qos     The qos to use while publishing: 0, 1 or 2
	 * @param retain  true if you want to retain the message on the server, false otherwise
	 * @return a token which will be completed when delivery completes for the given QoS
	 * @throws MqttException if the publish cannot be started
	 */
	IMqttDeliveryToken publish(String topic, String payload, int qos, boolean retain) throws MqttException;

	/**
	 * Publish a payload to the given topic at QoS 0, not retained.
	 *
	 * @param topic   The topic to publish to
	 * @param payload The payload to send
	 * @return a token which will be completed when the message has been written to the network
	 * @throws MqttException if the publish cannot be started
	 */
	IMqttDeliveryToken publish(String topic, byte[] payload) throws MqttException;

	/**
	 * Publish a payload to the given topic at QoS 0, using optional retain.
	 *
	 * @param topic   The topic to publish to
	 * @param payload The payload to send
	 * @param retain  true if you want to retain the message on the server, false otherwise
	 * @return a token which will be completed when the message has been written to the network
	 * @throws MqttException if the publish cannot be started
	 */
	IMqttDeliveryToken publish(String topic, byte[] payload, boolean retain) throws MqttException;


	/**
	 * Publish a payload to the given topic, using the given qos, not retained.
	 *
	 * @param topic   The topic to publish to
	 * @param payload The payload to send
	 * @param qos     The qos to use while publishing: 0, 1 or 2
	 * @return a token which will be completed when delivery completes for the given QoS
	 * @throws MqttException if the publish cannot be started
	 */
	IMqttDeliveryToken publish(String topic, byte[] payload, int qos) throws MqttException;


	/**
	 * Publish a payload to the given topic, using the given qos and optional retain
	 *
	 * @param topic   The topic to publish to
	 * @param payload The payload to send
	 * @param qos     The qos to use while publishing: 0, 1 or 2
	 * @param retain  true if you want to retain the message on the server, false otherwise
	 * @return a token which will be completed when delivery completes for the given QoS
	 * @throws MqttException if the publish cannot be started
	 */
	IMqttDeliveryToken publish(String topic, byte[] payload, int qos, boolean retain) throws MqttException;

	/**
	 * Publish a payload to the given topic, using the given qos and optional retain
	 *
	 * @param topic   The topic to publish to
	 * @param payload The payload to send
	 * @param qos     The qos to use while publishing
	 * @param retain  true if you want to retain the message on the server, false otherwise
	 * @return a token which will be completed when delivery completes for the given QoS
	 * @throws MqttException if the publish cannot be started
	 */
	IMqttDeliveryToken publish(String topic, byte[] payload, MqttQoS qos, boolean retain) throws MqttException;

	/**
	 * Publish a message to the given topic, using the QoS and retain flag set on the message
	 *
	 * @param topic   The topic to publish to
	 * @param message The message to send
	 * @return a token which will be completed when delivery completes for the message's QoS
	 * @throws MqttException if the publish cannot be started
	 */
	IMqttDeliveryToken publish(String topic, MqttMessage message) throws MqttException;

	/**
	 * Retrieve the MqttClient configuration
	 *
	 * @return The {@link MqttConnectOptions} instance we use
	 */
	MqttConnectOptions getOptions();

	/**
	 * Replace the MqttClient configuration. Takes effect on the next connect.
	 *
	 * @param options the options to use; {@code null} is ignored
	 */
	void setOptions(MqttConnectOptions options);


	/**
	 * Construct the MqttClient with additional config.
	 * This config can also be changed using the {@link #setOptions(MqttConnectOptions)} function
	 *
	 * @param config                 The config object to use while looking for settings
	 * @param defaultCallbackHandler The handler for connection events and for incoming messages that have no per-subscription listener
	 * @return a new client
	 */
	static IMqttAsyncClient create(MqttConnectOptions config, MqttCallback defaultCallbackHandler) {
		return new MqttAsyncClient(config, defaultCallbackHandler);
	}

	/**
	 * Send disconnect and close channel. Automatic reconnection is disabled until the next
	 * explicit connect.
	 *
	 * @return a token which will be completed when the channel has closed
	 */
	IMqttToken disconnect();

	/**
	 * Send DISCONNECT and close the channel after quiescing, as
	 * {@link #disconnect(long, Object, IMqttActionListener)} does. Automatic reconnection is
	 * disabled until the next explicit connect.
	 *
	 * @param quiesceTimeout time in milliseconds to allow in-flight work to finish; 0 or less
	 *                       disconnects at once
	 * @return a token which will be completed when the channel has closed
	 */
	IMqttToken disconnect(long quiesceTimeout);

	/**
	 * Send DISCONNECT and close the channel, notifying a listener when done. Automatic
	 * reconnection is disabled until the next explicit connect.
	 *
	 * @param userContext an object attached to the returned token, or {@code null}
	 * @param callback    a listener notified when the disconnect completes, or {@code null}
	 * @return a token which will be completed when the channel has closed
	 */
	IMqttToken disconnect(Object userContext, IMqttActionListener callback);

	/**
	 * Close the connection immediately without quiescing or sending DISCONNECT.
	 *
	 * @return a token which is already complete
	 */
	IMqttToken disconnectForcibly();

	/**
	 * Send DISCONNECT, then close the connection whether or not it could be written.
	 *
	 * @param disconnectTimeout time in milliseconds to allow for writing DISCONNECT; 0 or less
	 *                          waits for the write
	 * @return a token which will be completed when the channel has closed
	 */
	IMqttToken disconnectForcibly(long disconnectTimeout);

	/**
	 * Quiesce, send DISCONNECT, then close the connection whether or not it could be written.
	 *
	 * @param quiesceTimeout    time in milliseconds to allow in-flight work to finish; 0 or less
	 *                          does not quiesce
	 * @param disconnectTimeout time in milliseconds to allow for writing DISCONNECT; 0 or less
	 *                          waits for the write
	 * @return a token which will be completed when the channel has closed
	 */
	IMqttToken disconnectForcibly(long quiesceTimeout, long disconnectTimeout);

	/**
	 * Close the connection, optionally sending DISCONNECT first. Useful when a normal
	 * disconnect has failed, or when the server is not an MQTT broker and would not accept
	 * DISCONNECT.
	 *
	 * @param quiesceTimeout       time in milliseconds to allow in-flight work to finish; 0 or
	 *                             less does not quiesce
	 * @param disconnectTimeout    when {@code sendDisconnectPacket} is set, the time in
	 *                             milliseconds to allow for writing DISCONNECT before the channel
	 *                             is closed anyway; 0 or less waits for the write
	 * @param sendDisconnectPacket whether to send DISCONNECT before closing the channel
	 * @return a token which will be completed when the channel has closed
	 */
	IMqttToken disconnectForcibly(long quiesceTimeout, long disconnectTimeout, boolean sendDisconnectPacket);

	/**
	 * Disconnect cleanly, then release all client resources. The client cannot be used
	 * afterwards. Paho's {@code close()} throws if the client is still connected; this
	 * disconnects first instead.
	 *
	 * @throws MqttException if the disconnect fails or times out
	 */
	void close() throws MqttException;

	/**
	 * Release all client resources. Without {@code force}, this is {@link #close()}: a clean
	 * disconnect first. With {@code force}, the connection is closed at once without
	 * DISCONNECT, and the resources are released even if closing fails. The client cannot be
	 * used afterwards.
	 *
	 * @param force whether to close the connection forcibly instead of disconnecting cleanly
	 * @throws MqttException if a clean disconnect fails or times out
	 */
	void close(boolean force) throws MqttException;

	/**
	 * Disconnect and optionally release client resources. The default implementation calls
	 * {@link #disconnect()} and ignores the flags.
	 *
	 * @param deletecallback    whether to clear the callback after disconnecting
	 * @param deleteoptions     whether to reset the connection options after disconnecting
	 * @param shutdowneventloop whether to let go of the event loop after disconnecting; a group
	 *                          set with {@link #setEventLoop(EventLoopGroup)} is left running
	 * @throws MqttException if the disconnect fails
	 */
	default void disconnect(boolean deletecallback, boolean deleteoptions, boolean shutdowneventloop) throws MqttException {
		disconnect();
	}

	/**
	 * Send a PINGREQ to the broker. The default implementation does nothing.
	 */
	default void sendping() {
	}

	/**
	 * Sets the {@link MqttCallback} object for this MqttClient
	 *
	 * @param callback The callback to be set
	 */
	void setCallback(MqttCallback callback);

	/**
	 * Turns debug logging on or off. The default implementation does nothing.
	 *
	 * @param enable whether to log client activity
	 */
	default void enableDebugOutput(boolean enable) {
	}

	/**
	 * Turns debug logging on or off for the client, channel and ping handling. The default
	 * implementation calls {@link #enableDebugOutput(boolean)} with {@code enable}.
	 *
	 * @param enable        whether to log client activity
	 * @param enablechannel whether to log channel activity
	 * @param enableping    whether to log keep-alive pings
	 */
	default void enableDebugOutput(boolean enable, boolean enablechannel, boolean enableping) {
		enableDebugOutput(enable);
	}

	/**
	 * Returns the delivery tokens for publishes that have not yet completed.
	 *
	 * @return the pending delivery tokens
	 */
	IMqttDeliveryToken[] getPendingDeliveryTokens();

	/**
	 * Returns the number of QoS 1 and 2 publishes awaiting acknowledgement, including those
	 * held while disconnected. This is what {@link MqttConnectOptions#setMaxInflight(int)}
	 * limits. As in Paho, the count can change while this method returns.
	 *
	 * @return the number of in-flight publishes
	 */
	int getInFlightMessageCount();
}
