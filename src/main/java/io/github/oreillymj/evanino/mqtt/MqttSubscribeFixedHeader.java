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

import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;

/**
 * Builds the fixed header for SUBSCRIBE packets. The spec requires the QoS bits of a
 * SUBSCRIBE header to be set to 1.
 */
public final class MqttSubscribeFixedHeader {


    /**
     * Creates a SUBSCRIBE fixed header with the DUP flag cleared.
     *
     * @return a new SUBSCRIBE fixed header
     */
    public static MqttFixedHeader getFixedHeader(){
        // Create a new MqttFixedHeader
        return  getFixedHeader(false);
    }


    /**
     * Creates a SUBSCRIBE fixed header.
     *
     * @param duplicate the DUP flag; set when the packet is a retransmission
     * @return a new SUBSCRIBE fixed header
     */
    public static MqttFixedHeader getFixedHeader(boolean duplicate){
        // Create a new MqttFixedHeader
        return  new MqttFixedHeader(
                MqttMessageType.SUBSCRIBE, // Message Type
                duplicate, // DUP flag - Oasis spec says this must be false - http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718064
                MqttQoS.AT_LEAST_ONCE, // Quality of Service Level must 1 as per spec - https://stanford-clark.com/MQTT/#subscribe / http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718064
                false, // Retain
                0); // Remaining Length
    }
}
