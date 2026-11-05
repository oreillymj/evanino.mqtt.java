/*******************************************************************************
 * The interface and most of its documentation are from Eclipse Paho's Java client
 * (org.eclipse.paho.client.mqttv3.IMqttActionListener), with changes. This project uses the
 * Paho code under the Eclipse Distribution License v1.0 (see licenses/EDL-1.0.txt).
 *
 * The original file has no notice of its own; it is part of Eclipse Paho, copyright
 * IBM Corp. and the other Paho contributors, under the Eclipse Public License v2.0
 * and the Eclipse Distribution License v1.0.
 *
 * Changes copyright (c) 2026 Michael OReilly, under the Apache License 2.0 (see LICENSE).
 *******************************************************************************/

package io.github.oreillymj.evanino.mqtt;

/**
 * Implementors of this interface will be notified when an asynchronous action completes.
 *
 * <p>A listener is registered on an {@link IMqttToken} and a token is associated
 * with an action like connect or publish. When used with tokens on the {@link MqttAsyncClient}
 * the listener will be called back on the client's callback executor thread, not on the Netty
 * event loop. The listener will be informed if the action succeeds or fails. It is important that
 * the listener returns control quickly otherwise delivery of other callbacks will be stalled.
 * </p>
 */
public interface IMqttActionListener {
    /**
     * This method is invoked when an action has completed successfully.
     * @param asyncActionToken associated with the action that has completed
     */
    void onSuccess(IMqttToken asyncActionToken);
    /**
     * This method is invoked when an action fails.
     * If a client is disconnected while an action is in progress
     * onFailure will be called. For connections
     * that use cleanSession set to false, any QoS 1 and 2 messages that
     * are in the process of being delivered will be delivered to the requested
     * quality of service next time the client connects.
     * @param asyncActionToken associated with the action that has failed
     * @param exception thrown by the action that has failed
     */
    void onFailure(IMqttToken asyncActionToken, Throwable exception);
}