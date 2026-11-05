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

/**
 * Adapts a Paho-style per-subscription {@link IMqttMessageListener} to the client's
 * {@link MqttCallback} interface. Only {@link #messageArrived(String, MqttMessage)} is
 * forwarded; every other callback is a no-op.
 */
final class MqttMessageListenerAdapter implements MqttCallback {
	private final String TAG = "MqttMessageListenerAdapter";
	private final boolean enableLogging = false;

	private SimpleLogger logger = new SimpleLogger();

	private final IMqttMessageListener delegate;

	/**
	 * Creates an adapter that forwards incoming messages to the given listener.
	 *
	 * @param delegate the listener to forward messages to
	 */
	MqttMessageListenerAdapter(IMqttMessageListener delegate) {
		this.delegate = delegate;
	}

	// Only messageArrived matters to a per-subscription message listener; every
	// other MqttCallback event is a no-op here.

	/**
	 * Forwards the message to the wrapped listener. Exceptions from the listener propagate, and
	 * the client handles them like any other callback error.
	 *
	 * @param topic the topic the message was published to
	 * @param info  the received message
	 * @throws Exception if the listener throws
	 */
	@Override
	public void messageArrived(String topic, MqttMessage info) throws Exception {
		delegate.messageArrived(topic, info);
	}

	/** Does nothing. */
	@Override
	public void onConnectComplete(boolean reconnect, String serverURI) {
	}

	/** Does nothing. */
	@Override
	public void onConnectFailed(String serverURI, String reason) {
	}

	/** Does nothing. */
	@Override
	public void onConnecting(boolean connecting, String serverURI) {
	}

	/** Does nothing. */
	@Override
	public void onConnectionLost(Throwable cause) {
	}

	/** Does nothing. */
	@Override
	public void onDeliveryComplete(IMqttDeliveryToken token) {
	}

	/** Does nothing; the message is forwarded by {@link #messageArrived(String, MqttMessage)}. */
	@Override
	public void onMessageArrived(String topic, MqttMessage message) {
	}

	/** Does nothing. */
	@Override
	public void onReconnectFailed(String serverURI, String reason) {
	}

	/** Does nothing. */
	@Override
	public void onSuccessfulReconnect(boolean isCleanSession, String serverURI) {
	}

	/**
	 * Logs a message if logging is enabled for this adapter.
	 *
	 * @param data the message to log
	 */
	private void log(String data) {
		if (enableLogging) {
			logger.log(data);
		}
	}


}