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
 * Paho-compatible token that tracks the completion of an asynchronous action, mirroring
 * {@code org.eclipse.paho.client.mqttv3.IMqttToken} so code written against Paho can be
 * ported by changing imports.
 */
public interface IMqttToken {

	/**
	 * Blocks until the action this token represents has completed.
	 *
	 * @throws MqttException if the action fails, or on interruption.
	 */
	void waitForCompletion() throws MqttException;

	/**
	 * Blocks until the action this token represents has completed, or the
	 * timeout expires.
	 *
	 * @param timeout maximum time to wait in milliseconds. A negative value waits without a
	 *                caller-specified limit.
	 * @throws MqttException if the action fails, times out, or on interruption.
	 */
	void waitForCompletion(long timeout) throws MqttException;

	/**
	 * Returns whether the action has completed.
	 *
	 * @return true once the action has completed, successfully or not.
	 */
	boolean isComplete();


	/**
	 * Returns the exception the action failed with.
	 *
	 * @return the exception the action failed with, or null if it succeeded
	 * or has not yet completed.
	 */
	MqttException getException();

	/**
	 * Registers a listener to be notified when the action completes.
	 * If the action has already completed when this is called, implementations
	 * should fire the callback immediately rather than dropping it.
	 *
	 * @param listener the listener to notify
	 */
	void setActionCallback(IMqttActionListener listener);

	/**
	 * Returns the listener registered for this token.
	 *
	 * @return the listener previously registered via setActionCallback, or null.
	 */
	IMqttActionListener getActionCallback();

	/**
	 * Returns the client that initiated the action.
	 *
	 * @return the client instance that initiated the action this token represents.
	 */
	IMqttAsyncClient getClient();

	/**
	 * Returns the topic string(s) for the action being tracked by this
	 * token. If the action has not been initiated or the action has not
	 * topic associated with it such as connect then null will be returned.
	 *
	 * @return the topic string(s) for the subscribe being tracked by this token or null
	 */
	String[] getTopics();

	/**
	 * Attaches an arbitrary object to this token, returned unchanged by getUserContext().
	 * Useful for correlating a token back to caller-side state.
	 *
	 * @param userContext the object to attach
	 */
	void setUserContext(Object userContext);

	/**
	 * Returns the object attached to this token.
	 *
	 * @return the object previously attached via setUserContext, or null.
	 */
	Object getUserContext();

	/**
	 * Returns the MQTT packet identifier for the action.
	 *
	 * @return the MQTT packet identifier associated with this action, or -1/0
	 * for actions that don't use one (e.g. QoS 0 publish, connect).
	 */
	int getMessageId();

	/**
	 * Returns the QoS levels granted by the broker for a subscribe.
	 *
	 * @return the QoS levels granted by the broker, in the same order as the
	 * topics passed to subscribe(). Only meaningful for subscribe tokens.
	 */
	int[] getGrantedQos();

	/**
	 * Returns whether the broker resumed an existing session on connect.
	 *
	 * @return true if the broker reported an existing session was resumed
	 * on connect (i.e. cleanSession/cleanStart was false and a prior session existed).
	 * Only meaningful for connect tokens.
	 */
	boolean getSessionPresent();


}