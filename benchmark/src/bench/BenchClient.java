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

package bench;

import java.util.function.Consumer;

/**
 * The operations the benchmarks need, implemented once for each client under test so the
 * scenarios run identical code against both.
 */
interface BenchClient {

	/** Receives a message for a subscription. */
	interface MessageHandler {
		/**
		 * Called for each message, on the client's callback thread.
		 *
		 * @param topic   the topic
		 * @param payload the payload
		 */
		void onMessage(String topic, byte[] payload);
	}

	/**
	 * Connects and waits for the CONNACK.
	 *
	 * @param settings the connection settings
	 * @throws Exception if the connection fails or times out
	 */
	void connect(Settings settings) throws Exception;

	/**
	 * Subscribes and waits for the SUBACK.
	 *
	 * @param topic   the topic filter
	 * @param qos     the QoS
	 * @param handler receives the messages
	 * @throws Exception if the subscribe fails or times out
	 */
	void subscribe(String topic, int qos, MessageHandler handler) throws Exception;

	/**
	 * Publishes without waiting; exactly one of the callbacks is called when the publish
	 * completes (QoS 0: written; QoS 1: PUBACK; QoS 2: PUBCOMP) or fails. If the client's
	 * in-flight window is full, nothing is sent and {@code false} is returned, so the caller can
	 * retry. If the publish is refused for another reason, the exception is thrown. In both cases
	 * neither callback is called.
	 *
	 * @param topic      the topic
	 * @param payload    the payload
	 * @param qos        the QoS
	 * @param onComplete called on completion
	 * @param onFailure  called on failure
	 * @return {@code false} if the in-flight window is full
	 * @throws Exception if the client refuses the publish
	 */
	boolean publish(String topic, byte[] payload, int qos, Runnable onComplete, Consumer<Throwable> onFailure) throws Exception;

	/**
	 * Returns whether the client is connected.
	 *
	 * @return {@code true} if connected
	 */
	boolean isConnected();

	/**
	 * Disconnects, waiting for completion, and releases the client.
	 *
	 * @throws Exception if the disconnect fails
	 */
	void close() throws Exception;

	/** Connection settings, applied the same way to both clients. */
	final class Settings {
		/** Broker URI. */
		String serverURI;
		/** Client identifier. */
		String clientId;
		/** In-flight window; 0 keeps the client's default. */
		int maxInflight;
		/** Clean session. */
		boolean cleanSession = true;
		/** Automatic reconnect, retrying for ever. */
		boolean automaticReconnect;
		/** Connection timeout in seconds. */
		int connectionTimeout = 30;
		/** Keep-alive in seconds. */
		int keepAlive = 60;
		/** Called when a lost connection has been re-established by automatic reconnect. */
		Runnable onReconnect = () -> { };
		/** Called when the connection is lost. */
		Runnable onConnectionLost = () -> { };
	}
}
