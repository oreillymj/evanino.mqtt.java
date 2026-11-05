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
 * Paho-compatible listener that is notified when a message arrives on a topic it was
 * registered against at subscribe time.
 */
public interface IMqttMessageListener {
    /**
     * Called when a message arrives on a subscription this listener is attached to.
     *
     * @param topic   the topic the message was published to
     * @param message the received message
     * @throws Exception if the listener fails to process the message
     */
    void messageArrived(String topic, MqttMessage message) throws Exception;
}
