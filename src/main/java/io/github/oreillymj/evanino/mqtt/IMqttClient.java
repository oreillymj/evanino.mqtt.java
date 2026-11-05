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

import io.netty.handler.codec.mqtt.MqttQoS;

/**
 * Blocking MQTT client API, modelled on Paho's {@code IMqttClient}. Each method waits for
 * the operation to complete, up to the time set with {@link #setTimeToWait(long)}.
 */
public interface IMqttClient {


	/**
	 * Connects to the broker using the given options.
	 *
	 * @param options the connection options
	 * @throws MqttException if the connection fails or times out
	 */
	void connect(MqttConnectOptions options) throws MqttException;

	/**
	 * Connects to the broker using the client's current options.
	 *
	 * @throws MqttException if the connection fails or times out
	 */
	void connect() throws MqttException;


	/**
	 * Subscribes to a topic filter.
	 *
	 * @param topic the topic filter, which may include wildcards
	 * @param qos   the maximum QoS (0, 1 or 2) at which to receive messages
	 * @throws MqttException if the subscription fails or times out
	 */
	void subscribe(String topic, int qos) throws MqttException;

	/**
	 * Subscribes to a topic filter.
	 *
	 * @param topic the topic filter, which may include wildcards
	 * @param qos   the maximum QoS at which to receive messages
	 * @throws MqttException if the subscription fails or times out
	 */
	void subscribe(String topic, MqttQoS qos) throws MqttException;

	/**
	 * Subscribes to a topic filter at QoS 1.
	 *
	 * @param topicFilter the topic filter, which may include wildcards
	 * @throws MqttException if the subscription fails or times out
	 */
	void subscribe(String topicFilter) throws MqttException;

	/**
	 * Subscribes to several topic filters at QoS 1, in a single SUBSCRIBE.
	 *
	 * @param topicFilters the topic filters, which may include wildcards
	 * @throws MqttException if the subscription fails or times out
	 */
	void subscribe(String[] topicFilters) throws MqttException;

	/**
	 * Subscribes to several topic filters in a single SUBSCRIBE.
	 *
	 * @param topicFilters the topic filters, which may include wildcards
	 * @param qos          the maximum QoS for each topic filter, in the same order
	 * @throws MqttException if the subscription fails or times out
	 */
	void subscribe(String[] topicFilters, int[] qos) throws MqttException;

	/**
	 * Subscribes to a topic filter at QoS 1, delivering matching messages to the given listener.
	 *
	 * @param topicFilter     the topic filter, which may include wildcards
	 * @param messageListener the listener for matching messages
	 * @throws MqttException if the subscription fails or times out
	 */
	void subscribe(String topicFilter, IMqttMessageListener messageListener) throws MqttException;

	/**
	 * Subscribes to a topic filter, delivering matching messages to the given listener.
	 *
	 * @param topicFilter     the topic filter, which may include wildcards
	 * @param qos             the maximum QoS (0, 1 or 2) at which to receive messages
	 * @param messageListener the listener for matching messages
	 * @throws MqttException if the subscription fails or times out
	 */
	void subscribe(String topicFilter, int qos, IMqttMessageListener messageListener) throws MqttException;

	/**
	 * Subscribes to several topic filters at QoS 1, in a single SUBSCRIBE, each with its own
	 * listener.
	 *
	 * @param topicFilters     the topic filters, which may include wildcards
	 * @param messageListeners a listener per topic filter
	 * @throws MqttException if the subscription fails or times out
	 */
	void subscribe(String[] topicFilters, IMqttMessageListener[] messageListeners) throws MqttException;

	/**
	 * Subscribes to several topic filters in a single SUBSCRIBE, each with its own listener.
	 *
	 * @param topicFilters     the topic filters, which may include wildcards
	 * @param qos              the maximum QoS for each topic filter, in the same order
	 * @param messageListeners a listener per topic filter
	 * @throws MqttException if the subscription fails or times out
	 */
	void subscribe(String[] topicFilters, int[] qos, IMqttMessageListener[] messageListeners) throws MqttException;

	/**
	 * Unsubscribes from a topic filter.
	 *
	 * @param topic the topic filter, exactly as it was subscribed
	 * @throws MqttException if the unsubscribe fails or times out
	 */
	void unsubscribe(String topic) throws MqttException;

	/**
	 * Unsubscribes from several topic filters, in a single UNSUBSCRIBE.
	 *
	 * @param topicFilters the topic filters, exactly as they were subscribed
	 * @throws MqttException if the unsubscribe fails or times out
	 */
	void unsubscribe(String[] topicFilters) throws MqttException;

	/**
	 * Publishes a message, using the QoS and retain flag set on the message.
	 *
	 * @param topic   the topic to publish to
	 * @param message the message to publish
	 * @throws MqttException if the publish fails or times out
	 */
	void publish(String topic, MqttMessage message) throws MqttException;

	/**
	 * Publishes a UTF-8 string payload at QoS 0, not retained.
	 *
	 * @param topic   the topic to publish to
	 * @param payload the message payload
	 * @throws MqttException if the publish fails or times out
	 */
	void publish(String topic, String payload) throws MqttException;

	/**
	 * Publishes a UTF-8 string payload, not retained.
	 *
	 * @param topic   the topic to publish to
	 * @param payload the message payload
	 * @param qos     the QoS level (0, 1 or 2)
	 * @throws MqttException if the publish fails or times out
	 */
	void publish(String topic, String payload, int qos) throws MqttException;

	/**
	 * Publishes a UTF-8 string payload.
	 *
	 * @param topic   the topic to publish to
	 * @param payload the message payload
	 * @param qos     the QoS level (0, 1 or 2)
	 * @param retain  whether the broker should retain the message
	 * @throws MqttException if the publish fails or times out
	 */
	void publish(String topic, String payload, int qos, boolean retain) throws MqttException;

	/**
	 * Publishes a binary payload.
	 *
	 * @param topic   the topic to publish to
	 * @param payload the message payload
	 * @param qos     the QoS level (0, 1 or 2)
	 * @param retain  whether the broker should retain the message
	 * @throws MqttException if the publish fails or times out
	 */
	void publish(String topic, byte[] payload, int qos, boolean retain) throws MqttException;

	/**
	 * Disconnects from the broker.
	 *
	 * @throws MqttException if the disconnect fails or times out
	 */
	void disconnect() throws MqttException;

	/**
	 * Test hook with no defined behaviour; implementations may do nothing.
	 */
	void test();

	/**
	 * Sets how long blocking methods wait for an operation to complete.
	 *
	 * @param ms the maximum wait in milliseconds, or {@code -1} to wait indefinitely
	 */
	void setTimeToWait(long ms);
}
