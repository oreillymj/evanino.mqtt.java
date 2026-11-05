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

import java.net.URI;

/**
 * Stand-in for Paho's {@code MqttClientFactoryPaho}. In Paho the server URI and client ID are
 * given to the client constructor; in this client they live in {@link MqttConnectOptions}, so
 * the factory builds options that carry them.
 */
final class TestClients {

	/**
	 * Prevents instantiation; this class only holds static helpers.
	 */
	private TestClients() {
	}

	/**
	 * Creates connection options for the given broker and client ID, with all other values at
	 * their defaults.
	 *
	 * @param serverURI the broker URI
	 * @param clientId  the client identifier
	 * @return new connection options
	 */
	static MqttConnectOptions options(URI serverURI, String clientId) {
		MqttConnectOptions options = new MqttConnectOptions();
		options.setServerURI(serverURI.toString());
		options.setClientId(clientId);
		return options;
	}

	/**
	 * Creates a blocking client for the given broker and client ID; equivalent to Paho's
	 * {@code new MqttClient(serverURI, clientId)}.
	 *
	 * @param serverURI the broker URI
	 * @param clientId  the client identifier
	 * @return a new, unconnected client
	 * @throws MqttException if the client cannot be created
	 */
	static MqttClient createMqttClient(URI serverURI, String clientId) throws MqttException {
		return new MqttClient(serverURI.toString(), clientId);
	}

	/**
	 * Creates an asynchronous client for the given broker and client ID; equivalent to Paho's
	 * {@code new MqttAsyncClient(serverURI, clientId)}.
	 *
	 * @param serverURI the broker URI
	 * @param clientId  the client identifier
	 * @return a new, unconnected client
	 * @throws MqttException if the client cannot be created
	 */
	static MqttAsyncClient createMqttAsyncClient(URI serverURI, String clientId) throws MqttException {
		return new MqttAsyncClient(serverURI.toString(), clientId);
	}

	/**
	 * Publishes a payload; equivalent to Paho's
	 * {@code publish(topic, payload, qos, retained, null, null)}.
	 *
	 * @param client   the client to publish with
	 * @param topic    the topic to publish to
	 * @param payload  the message payload
	 * @param qos      the QoS level
	 * @param retained whether the broker should retain the message
	 * @return the delivery token
	 * @throws MqttException if the publish cannot be started
	 */
	static IMqttDeliveryToken publish(IMqttAsyncClient client, String topic, byte[] payload, int qos, boolean retained)
			throws MqttException {
		MqttMessage message = new MqttMessage(payload);
		message.setQos(qos);
		message.setRetained(retained);
		return client.publish(topic, message, null, null);
	}

	/**
	 * Disconnects and shuts down an asynchronous client, ignoring any failure; used in
	 * {@code finally} blocks.
	 *
	 * @param client the client to shut down, or {@code null}
	 */
	static void shutdownQuietly(MqttAsyncClient client) {
		if (client == null) {
			return;
		}
		try {
			client.shutdown();
		} catch (Exception ignored) {
		}
	}

	/**
	 * Closes a client, ignoring any failure; used in {@code finally} blocks.
	 *
	 * @param client the client to close, or {@code null}
	 */
	static void closeQuietly(MqttClient client) {
		if (client == null) {
			return;
		}
		try {
			client.disconnect();
		} catch (Exception ignored) {
		}
		try {
			client.close();
		} catch (Exception ignored) {
		}
	}
}
