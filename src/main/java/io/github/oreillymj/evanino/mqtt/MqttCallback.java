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
 * Receives connection lifecycle events and incoming messages from the client.
 * <p>
 * Every event has an {@code on<Event>} method. The shortest {@code on<Event>} overload of each
 * event is abstract, so it is what an IDE inserts when the interface is implemented, and the
 * handlers sort together alphabetically. Where Paho has the same event, the abstract handler
 * takes Paho's parameters, so a Paho callback is migrated by adding {@code on} to the front of
 * each method name. The other overloads are empty defaults:
 * <ul>
 *   <li>Longer {@code on<Event>} overloads add more detail, such as the server URI or the
 *       {@code uuid} of the client instance that raised the event.</li>
 *   <li>The Paho names and signatures ({@link #connectComplete(boolean, String)},
 *       {@link #connectionLost(Throwable)}, {@link #messageArrived(String, MqttMessage)} and
 *       {@link #deliveryComplete(IMqttDeliveryToken)}) are kept so that callbacks written for
 *       Paho keep working.</li>
 *   <li>The names used by earlier versions of this client, such as
 *       {@link #connectComplete(boolean, boolean, String)} and
 *       {@link #onMessage(String, MqttMessage)}, are kept so that callbacks written for those
 *       versions keep working.</li>
 * </ul>
 * When an event occurs the client calls <em>every</em> overload of it: the Paho name first, then
 * the earlier names, then the {@code on<Event>} overloads from shortest to longest, so any
 * combination can be overridden.
 * Implement only one overload of an event unless the handlers are meant to run in addition to
 * each other. An exception thrown by one overload does not stop the others from being called.
 * <p>
 * Originally created by Valerii Sosliuk on 12/30/2017.
 */
public interface MqttCallback {

	// -------------------------------------------------------------
	// Paho-compatible names
	// -------------------------------------------------------------

	/**
	 * Paho's {@code MqttCallbackExtended.connectComplete}: called when a connection to the
	 * server has been established. The default implementation does nothing.
	 *
	 * @param reconnect {@code true} if this was an automatic reconnect
	 * @param serverURI the URI of the server connected to
	 * @see #onConnectComplete(boolean, String)
	 */
	default void connectComplete(boolean reconnect, String serverURI) {
	}

	/**
	 * Paho's {@code MqttCallback.connectionLost}: called when the connection to the server is
	 * lost. The default implementation does nothing.
	 *
	 * @param cause the reason behind the loss of connection
	 * @see #onConnectionLost(Throwable)
	 */
	default void connectionLost(Throwable cause) {
	}

	/**
	 * Paho's {@code MqttCallback.deliveryComplete}: called when delivery of an outgoing message
	 * has completed. The default implementation does nothing.
	 *
	 * @param token the delivery token for the message
	 * @see #onDeliveryComplete(IMqttDeliveryToken)
	 */
	default void deliveryComplete(IMqttDeliveryToken token) {
	}

	/**
	 * Paho's {@code MqttCallback.messageArrived}: called with each incoming message. The default
	 * implementation does nothing.
	 *
	 * @param topic   the topic the message was published to
	 * @param message the received message
	 * @throws Exception to report that the message could not be processed; see
	 *                   {@link MqttConnectOptions#setMessageArrivedErrorsTriggerDisconnect(boolean)}
	 * @see #onMessageArrived(String, MqttMessage)
	 */
	default void messageArrived(String topic, MqttMessage message) throws Exception {
	}

	// -------------------------------------------------------------
	// Names from earlier versions of this client
	// -------------------------------------------------------------

	/**
	 * Called when a connection to the server has been established. Kept so that callbacks
	 * written for earlier versions of this client keep working; new code should override
	 * {@link #onConnectComplete(boolean, boolean, String)}. The default implementation does
	 * nothing.
	 *
	 * @param reconnect      {@code true} if this was an automatic reconnect
	 * @param isCleanSession whether the connection uses a clean session
	 * @param serverURI      the URI of the server connected to
	 */
	default void connectComplete(boolean reconnect, boolean isCleanSession, String serverURI) {
	}

	/**
	 * Called when a connection to the server has been established, with the identifier of the
	 * client instance. Kept so that callbacks written for earlier versions of this client keep
	 * working; new code should override
	 * {@link #onConnectComplete(boolean, boolean, String, String)}. The default implementation
	 * does nothing.
	 *
	 * @param reconnect      {@code true} if this was an automatic reconnect
	 * @param isCleanSession whether the connection uses a clean session
	 * @param serverURI      the URI of the server connected to
	 * @param uuid           the unique identifier of the client instance
	 */
	default void connectComplete(boolean reconnect, boolean isCleanSession, String serverURI, String uuid) {
	}

	/**
	 * Called when the connection to the server is lost, with the identifier of the client
	 * instance. Kept so that callbacks written for earlier versions of this client keep working;
	 * new code should override {@link #onConnectionLost(Throwable, String)}. The default
	 * implementation does nothing.
	 *
	 * @param cause the reason behind the loss of connection
	 * @param uuid  the unique identifier of the client instance
	 */
	default void connectionLost(Throwable cause, String uuid) {
	}

	/**
	 * Called when the connection to the server is lost, with the server URI and the identifier
	 * of the client instance. Kept so that callbacks written for earlier versions of this client
	 * keep working; new code should override
	 * {@link #onConnectionLost(Throwable, String, String)}. The default implementation does
	 * nothing.
	 *
	 * @param cause     the reason behind the loss of connection
	 * @param serverURI the URI of the server the connection was lost to
	 * @param uuid      the unique identifier of the client instance
	 */
	default void connectionLost(Throwable cause, String serverURI, String uuid) {
	}

	/**
	 * Called when the client starts or stops a connection attempt. Kept so that callbacks
	 * written for earlier versions of this client keep working; new code should override
	 * {@link #onConnecting(boolean, String)}, which also says whether the attempt started or
	 * ended. The default implementation does nothing.
	 *
	 * @param serverURI the URI of the server being connected to
	 */
	default void isConnecting(String serverURI) {
	}

	/**
	 * Called when the client starts or stops a connection attempt, with the identifier of the
	 * client instance. Kept so that callbacks written for earlier versions of this client keep
	 * working; new code should override {@link #onConnecting(boolean, String, String)}. The
	 * default implementation does nothing.
	 *
	 * @param serverURI the URI of the server being connected to
	 * @param uuid      the unique identifier of the client instance
	 */
	default void isConnecting(String serverURI, String uuid) {
	}

	/**
	 * Called when a connection attempt fails. Kept so that callbacks written for earlier
	 * versions of this client keep working; new code should override
	 * {@link #onConnectFailed(String, String)}, which also gives the reason. The default
	 * implementation does nothing.
	 *
	 * @param serverURI the URI of the server that could not be reached
	 */
	default void onConnectFailed(String serverURI) {
	}

	/**
	 * Called with each incoming message. Kept so that callbacks written for earlier versions of
	 * this client keep working; new code should override
	 * {@link #onMessageArrived(String, MqttMessage)}. The default implementation does nothing.
	 *
	 * @param topic   the topic the message was published to
	 * @param message the received message
	 */
	default void onMessage(String topic, MqttMessage message) {
	}

	/**
	 * Called with each incoming message's payload, decoded as UTF-8. Kept so that callbacks
	 * written for earlier versions of this client keep working; new code should override
	 * {@link #onMessageArrived(String, String)}. The default implementation does nothing.
	 *
	 * @param topic   the topic the message was published to
	 * @param payload the message payload as a string
	 */
	default void onMessage(String topic, String payload) {
	}

	// -------------------------------------------------------------
	// on<Event> handlers
	// -------------------------------------------------------------

	/**
	 * Called when a connection to the server has been established. Takes the same parameters as
	 * Paho's {@link #connectComplete(boolean, String)}.
	 *
	 * @param reconnect {@code true} if this was an automatic reconnect
	 * @param serverURI the URI of the server connected to
	 */
	void onConnectComplete(boolean reconnect, String serverURI);

	/**
	 * Called when a connection to the server has been established, with the session type. The
	 * default implementation does nothing.
	 *
	 * @param reconnect      {@code true} if this was an automatic reconnect
	 * @param isCleanSession whether the connection uses a clean session
	 * @param serverURI      the URI of the server connected to
	 */
	default void onConnectComplete(boolean reconnect, boolean isCleanSession, String serverURI) {
	}

	/**
	 * Called when a connection to the server has been established, with the identifier of the
	 * client instance. The default implementation does nothing.
	 *
	 * @param reconnect      {@code true} if this was an automatic reconnect
	 * @param isCleanSession whether the connection uses a clean session
	 * @param serverURI      the URI of the server connected to
	 * @param uuid           the unique identifier of the client instance
	 */
	default void onConnectComplete(boolean reconnect, boolean isCleanSession, String serverURI, String uuid) {
	}

	/**
	 * Called when a connection attempt fails.
	 *
	 * @param serverURI the URI of the server that could not be reached
	 * @param reason    a description of the failure
	 */
	void onConnectFailed(String serverURI, String reason);

	/**
	 * Called when a connection attempt fails, with the identifier of the client instance. The
	 * default implementation does nothing.
	 *
	 * @param serverURI the URI of the server that could not be reached
	 * @param reason    a description of the failure
	 * @param uuid      the unique identifier of the client instance
	 */
	default void onConnectFailed(String serverURI, String reason, String uuid) {
	}

	/**
	 * Called when the client starts or stops a connection attempt.
	 *
	 * @param connecting {@code true} when an attempt starts, {@code false} when it ends
	 * @param serverURI  the URI of the server being connected to
	 */
	void onConnecting(boolean connecting, String serverURI);

	/**
	 * Called when the client starts or stops a connection attempt, with the identifier of the
	 * client instance. The default implementation does nothing.
	 *
	 * @param connecting {@code true} when an attempt starts, {@code false} when it ends
	 * @param serverURI  the URI of the server being connected to
	 * @param uuid       the unique identifier of the client instance
	 */
	default void onConnecting(boolean connecting, String serverURI, String uuid) {
	}

	/**
	 * Called when the connection to the server is lost.
	 *
	 * @param cause the reason behind the loss of connection
	 */
	void onConnectionLost(Throwable cause);

	/**
	 * Called when the connection to the server is lost, with the identifier of the client
	 * instance. The default implementation does nothing.
	 *
	 * @param cause the reason behind the loss of connection
	 * @param uuid  the unique identifier of the client instance
	 */
	default void onConnectionLost(Throwable cause, String uuid) {
	}

	/**
	 * Called when the connection to the server is lost, with the server URI and the identifier
	 * of the client instance. The default implementation does nothing.
	 *
	 * @param cause     the reason behind the loss of connection
	 * @param serverURI the URI of the server the connection was lost to
	 * @param uuid      the unique identifier of the client instance
	 */
	default void onConnectionLost(Throwable cause, String serverURI, String uuid) {
	}

	/**
	 * Called when delivery of an outgoing message has completed: for QoS 0 once it has been
	 * written, for QoS 1 when PUBACK is received, and for QoS 2 when PUBCOMP is received.
	 *
	 * @param token the delivery token for the message
	 */
	void onDeliveryComplete(IMqttDeliveryToken token);

	/**
	 * Called with each incoming message.
	 *
	 * @param topic   the topic the message was published to
	 * @param message the received message
	 * @throws Exception to report that the message could not be processed; see
	 *                   {@link MqttConnectOptions#setMessageArrivedErrorsTriggerDisconnect(boolean)}
	 */
	void onMessageArrived(String topic, MqttMessage message) throws Exception;

	/**
	 * Called with each incoming message's payload, decoded as UTF-8. The default implementation
	 * does nothing.
	 *
	 * @param topic   the topic the message was published to
	 * @param payload the message payload as a string
	 * @throws Exception to report that the message could not be processed; see
	 *                   {@link MqttConnectOptions#setMessageArrivedErrorsTriggerDisconnect(boolean)}
	 */
	default void onMessageArrived(String topic, String payload) throws Exception {
	}

	/**
	 * Called when automatic reconnection fails, for example because the maximum number of
	 * reconnect attempts was exceeded.
	 *
	 * @param serverURI the URI of the server being reconnected to
	 * @param reason    a description of the failure
	 */
	void onReconnectFailed(String serverURI, String reason);

	/**
	 * Called when automatic reconnection fails, with the identifier of the client instance. The
	 * default implementation does nothing.
	 *
	 * @param serverURI the URI of the server being reconnected to
	 * @param reason    a description of the failure
	 * @param uuid      the unique identifier of the client instance
	 */
	default void onReconnectFailed(String serverURI, String reason, String uuid) {
	}

	/**
	 * Called when the connection to the server is recovered by an automatic reconnect.
	 *
	 * @param isCleanSession whether the new connection uses a clean session
	 * @param serverURI      the URI of the server reconnected to
	 */
	void onSuccessfulReconnect(boolean isCleanSession, String serverURI);

	/**
	 * Called when the connection to the server is recovered by an automatic reconnect, with the
	 * identifier of the client instance. The default implementation does nothing.
	 *
	 * @param isCleanSession whether the new connection uses a clean session
	 * @param serverURI      the URI of the server reconnected to
	 * @param uuid           the unique identifier of the client instance
	 */
	default void onSuccessfulReconnect(boolean isCleanSession, String serverURI, String uuid) {
	}
}
