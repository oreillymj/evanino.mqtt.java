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
 * Builds the fixed header for PUBLISH packets. The DUP flag is always cleared.
 */
public interface IMqttPublishFixedHeader {
	/**
	 * Creates a QoS 0, non-retained PUBLISH fixed header.
	 *
	 * @return a new PUBLISH fixed header
	 */
	static MqttFixedHeader getFixedHeader() {
		return getFixedHeader(MqttQoS.AT_MOST_ONCE, false);
	}

	/**
	 * Creates a non-retained PUBLISH fixed header with the given QoS.
	 *
	 * @param qos the quality of service for the message
	 * @return a new PUBLISH fixed header
	 */
	static MqttFixedHeader getFixedHeader(MqttQoS qos) {
		// Create a new MqttFixedHeader
		return getFixedHeader(qos, false);
	}

	/**
	 * Creates a PUBLISH fixed header with the given QoS and retain flag.
	 *
	 * @param qos    the quality of service for the message
	 * @param retain whether the broker should retain the message
	 * @return a new PUBLISH fixed header
	 */
	static MqttFixedHeader getFixedHeader(MqttQoS qos, boolean retain) {
		// Create a new MqttFixedHeader
		return new MqttFixedHeader(
				MqttMessageType.PUBLISH, // Message Type
				false, // DUP flag - Oasis spec says this must be false - https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc385349261
				qos, // Quality of Service Level must 1 as per spec - https://stanford-clark.com/MQTT/#publish / https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718037
				retain, // Retain
				0); // Remaining Length
	}
}
