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

import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;

/**
 * Records an incoming publish whose acknowledgement to the broker has not yet been sent.
 */
public class PendingIncomingAck {
    private int messageid;
    private MqttQoS qos;
    private MqttPublishMessage message;

    /**
     * Creates a pending acknowledgement without the original message.
     *
     * @param messageid the packet identifier of the incoming publish
     * @param qos       the QoS of the incoming publish
     */
    public PendingIncomingAck(int messageid, MqttQoS qos) {
        this.messageid=messageid;
        this.qos=qos;
    }
    /**
     * Creates a pending acknowledgement that also holds the original message.
     *
     * @param messageid the packet identifier of the incoming publish
     * @param qos       the QoS of the incoming publish
     * @param message   the incoming publish
     */
    public PendingIncomingAck(int messageid, MqttQoS qos, MqttPublishMessage message) {
        this.messageid=messageid;
        this.qos=qos;
        this.message=message;
    }

    /**
     * Returns the packet identifier of the incoming publish.
     *
     * @return the packet identifier
     */
    public int getMessageID(){
        return this.messageid;
    }

    /**
     * Returns the QoS of the incoming publish.
     *
     * @return the QoS level
     */
    public MqttQoS getQos(){
        return this.qos;
    }

    /**
     * Returns the incoming publish, if it was supplied.
     *
     * @return the incoming publish, or {@code null}
     */
    public MqttPublishMessage getMessage(){
        return this.message;
    }
}
