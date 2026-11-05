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

/**
 * {@link MqttCallback} with no-op implementations of every abstract method, so tests only
 * override the events they care about.
 */
public class MqttCallbackAdapter implements MqttCallback {

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

	/** Does nothing. */
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
}
