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

import java.nio.charset.StandardCharsets;

/**
 * Raises {@link MqttCallback} events. Each method calls every overload of one event: the Paho
 * name first, then the names from earlier versions of this client, then the {@code on<Event>}
 * overloads from shortest to longest. An exception thrown by
 * one overload does not stop the rest from being called; the first is rethrown once all have
 * run, with any later ones attached as suppressed exceptions.
 */
final class MqttCallbackEvents {

	/**
	 * One overload call.
	 */
	@FunctionalInterface
	private interface Call {
		void run() throws Exception;
	}

	private MqttCallbackEvents() {
	}

	/**
	 * Raises the connect-complete event.
	 *
	 * @param cb           the callback
	 * @param reconnect    whether this was an automatic reconnect
	 * @param cleanSession whether the connection uses a clean session
	 * @param serverURI    the URI of the server connected to
	 * @param uuid         the unique identifier of the client instance
	 */
	static void connectComplete(MqttCallback cb, boolean reconnect, boolean cleanSession, String serverURI, String uuid) {
		callAllUnchecked(
				() -> cb.connectComplete(reconnect, serverURI),
				() -> cb.connectComplete(reconnect, cleanSession, serverURI),
				() -> cb.connectComplete(reconnect, cleanSession, serverURI, uuid),
				() -> cb.onConnectComplete(reconnect, serverURI),
				() -> cb.onConnectComplete(reconnect, cleanSession, serverURI),
				() -> cb.onConnectComplete(reconnect, cleanSession, serverURI, uuid));
	}

	/**
	 * Raises the connect-failed event.
	 *
	 * @param cb        the callback
	 * @param serverURI the URI of the server that could not be reached
	 * @param reason    a description of the failure
	 * @param uuid      the unique identifier of the client instance
	 */
	static void connectFailed(MqttCallback cb, String serverURI, String reason, String uuid) {
		callAllUnchecked(
				() -> cb.onConnectFailed(serverURI),
				() -> cb.onConnectFailed(serverURI, reason),
				() -> cb.onConnectFailed(serverURI, reason, uuid));
	}

	/**
	 * Raises the connecting event.
	 *
	 * @param cb         the callback
	 * @param connecting {@code true} when an attempt starts, {@code false} when it ends
	 * @param serverURI  the URI of the server being connected to
	 * @param uuid       the unique identifier of the client instance
	 */
	static void connecting(MqttCallback cb, boolean connecting, String serverURI, String uuid) {
		callAllUnchecked(
				() -> cb.isConnecting(serverURI),
				() -> cb.isConnecting(serverURI, uuid),
				() -> cb.onConnecting(connecting, serverURI),
				() -> cb.onConnecting(connecting, serverURI, uuid));
	}

	/**
	 * Raises the connection-lost event.
	 *
	 * @param cb        the callback
	 * @param cause     the reason the connection was lost
	 * @param serverURI the URI of the server the connection was lost to
	 * @param uuid      the unique identifier of the client instance
	 */
	static void connectionLost(MqttCallback cb, Throwable cause, String serverURI, String uuid) {
		callAllUnchecked(
				() -> cb.connectionLost(cause),
				() -> cb.connectionLost(cause, uuid),
				() -> cb.connectionLost(cause, serverURI, uuid),
				() -> cb.onConnectionLost(cause),
				() -> cb.onConnectionLost(cause, uuid),
				() -> cb.onConnectionLost(cause, serverURI, uuid));
	}

	/**
	 * Raises the delivery-complete event.
	 *
	 * @param cb    the callback
	 * @param token the delivery token for the message
	 */
	static void deliveryComplete(MqttCallback cb, IMqttDeliveryToken token) {
		callAllUnchecked(
				() -> cb.deliveryComplete(token),
				() -> cb.onDeliveryComplete(token));
	}

	/**
	 * Raises the message-arrived event. The payload is decoded as UTF-8 for
	 * {@link MqttCallback#onMessage(String, String)} and
	 * {@link MqttCallback#onMessageArrived(String, String)}.
	 *
	 * @param cb      the callback
	 * @param topic   the topic the message was published to
	 * @param message the received message
	 * @throws Exception the first exception thrown by any overload
	 */
	static void messageArrived(MqttCallback cb, String topic, MqttMessage message) throws Exception {
		String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
		callAll(
				() -> cb.messageArrived(topic, message),
				() -> cb.onMessage(topic, message),
				() -> cb.onMessage(topic, payload),
				() -> cb.onMessageArrived(topic, message),
				() -> cb.onMessageArrived(topic, payload));
	}

	/**
	 * Raises the reconnect-failed event.
	 *
	 * @param cb        the callback
	 * @param serverURI the URI of the server being reconnected to
	 * @param reason    a description of the failure
	 * @param uuid      the unique identifier of the client instance
	 */
	static void reconnectFailed(MqttCallback cb, String serverURI, String reason, String uuid) {
		callAllUnchecked(
				() -> cb.onReconnectFailed(serverURI, reason),
				() -> cb.onReconnectFailed(serverURI, reason, uuid));
	}

	/**
	 * Raises the successful-reconnect event.
	 *
	 * @param cb           the callback
	 * @param cleanSession whether the new connection uses a clean session
	 * @param serverURI    the URI of the server reconnected to
	 * @param uuid         the unique identifier of the client instance
	 */
	static void successfulReconnect(MqttCallback cb, boolean cleanSession, String serverURI, String uuid) {
		callAllUnchecked(
				() -> cb.onSuccessfulReconnect(cleanSession, serverURI),
				() -> cb.onSuccessfulReconnect(cleanSession, serverURI, uuid));
	}

	/**
	 * Runs every call, then rethrows the first exception, with later ones suppressed.
	 *
	 * @param calls the overload calls, in order
	 * @throws Exception the first exception thrown by any call
	 */
	private static void callAll(Call... calls) throws Exception {
		Exception first = null;
		for (Call call : calls) {
			try {
				call.run();
			} catch (Exception ex) {
				if (first == null) {
					first = ex;
				} else {
					first.addSuppressed(ex);
				}
			}
		}
		if (first != null) {
			throw first;
		}
	}

	/**
	 * Runs every call, as {@link #callAll(Call...)}, for overloads that declare no checked
	 * exceptions.
	 *
	 * @param calls the overload calls, in order
	 * @throws RuntimeException the first exception thrown by any call
	 */
	private static void callAllUnchecked(Call... calls) {
		try {
			callAll(calls);
		} catch (RuntimeException ex) {
			throw ex;
		} catch (Exception ex) {
			// unreachable: none of these overloads declares a checked exception
			throw new IllegalStateException(ex);
		}
	}
}
