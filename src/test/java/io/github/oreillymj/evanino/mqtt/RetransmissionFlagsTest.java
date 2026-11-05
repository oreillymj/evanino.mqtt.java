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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Retransmitted packets have valid fixed header flags. Only PUBLISH may carry the DUP flag; MQTT
 * 3.1.1 fixes the flags of PUBREC at {@code 0000} and of PUBREL, SUBSCRIBE and UNSUBSCRIBE at
 * {@code 0010}. The client once set DUP on every retransmission, so a PUBREL or PUBREC resent
 * after 10 s made Mosquitto close the connection as a malformed packet. Each packet is sent
 * through the real {@link RetransmissionHandler} timer and encoded by Netty's
 * {@link MqttEncoder}, and the first byte on the wire is checked. Not part of Paho's suite;
 * needs no broker.
 */
public class RetransmissionFlagsTest {

	/**
	 * A resent PUBLISH has DUP set and keeps its QoS and retain flags.
	 */
	@Test(timeout = 10_000)
	public void testPublishIsResentWithDup() {
		MqttMessage publish = MqttMessageBuilders.publish().topicName("t").qos(MqttQoS.EXACTLY_ONCE)
				.retained(true).messageId(7).payload(Unpooled.wrappedBuffer(new byte[]{1})).build();
		// 0011 DUP=1 QoS=10 RETAIN=1
		Assert.assertEquals(0x3D, firstByteOfRetransmission(publish));
	}

	/**
	 * A resent PUBREL keeps the flags {@code 0010}.
	 */
	@Test(timeout = 10_000)
	public void testPubrelIsResentWithoutDup() {
		MqttMessage pubrel = new MqttMessage(MqttPubRelFixedHeader.getFixedHeader(), MqttMessageIdVariableHeader.from(7));
		Assert.assertEquals(0x62, firstByteOfRetransmission(pubrel));
	}

	/**
	 * A resent PUBREC keeps the flags {@code 0000}.
	 */
	@Test(timeout = 10_000)
	public void testPubrecIsResentWithoutDup() {
		MqttMessage pubrec = new MqttMessage(
				new MqttFixedHeader(MqttMessageType.PUBREC, false, MqttQoS.AT_MOST_ONCE, false, 0),
				MqttMessageIdVariableHeader.from(7));
		Assert.assertEquals(0x50, firstByteOfRetransmission(pubrec));
	}

	/**
	 * Resent SUBSCRIBE and UNSUBSCRIBE packets keep the flags {@code 0010}.
	 */
	@Test(timeout = 10_000)
	public void testSubscribeAndUnsubscribeAreResentWithoutDup() {
		MqttMessage subscribe = MqttMessageBuilders.subscribe().messageId(7).addSubscription(MqttQoS.AT_LEAST_ONCE, "t").build();
		Assert.assertEquals(0x82, firstByteOfRetransmission(subscribe));
		MqttMessage unsubscribe = MqttMessageBuilders.unsubscribe().messageId(7).addTopicFilter("t").build();
		Assert.assertEquals(0xA2, firstByteOfRetransmission(unsubscribe));
	}

	/**
	 * Returns {@code message} with another fixed header, as the packet type Netty's encoder expects.
	 *
	 * @param fixedHeader the header to use
	 * @param message     the packet
	 * @return the packet with the new header
	 */
	private static MqttMessage withHeader(MqttFixedHeader fixedHeader, MqttMessage message) {
		switch (fixedHeader.messageType()) {
			case PUBLISH:
				MqttPublishMessage publish = (MqttPublishMessage) message;
				return new MqttPublishMessage(fixedHeader, publish.variableHeader(), publish.payload().retainedDuplicate());
			case SUBSCRIBE:
				MqttSubscribeMessage subscribe = (MqttSubscribeMessage) message;
				return new MqttSubscribeMessage(fixedHeader, subscribe.variableHeader(), subscribe.payload());
			case UNSUBSCRIBE:
				MqttUnsubscribeMessage unsubscribe = (MqttUnsubscribeMessage) message;
				return new MqttUnsubscribeMessage(fixedHeader, unsubscribe.variableHeader(), unsubscribe.payload());
			default:
				return new MqttMessage(fixedHeader, message.variableHeader());
		}
	}

	/**
	 * Starts a {@link RetransmissionHandler} for {@code original} on an embedded channel, lets
	 * its first retransmission fire, and returns the first byte the encoder wrote for it.
	 *
	 * @param original the packet as first sent
	 * @return the first byte of the encoded retransmission
	 */
	private static int firstByteOfRetransmission(MqttMessage original) {
		EmbeddedChannel channel = new EmbeddedChannel(MqttEncoder.INSTANCE);
		try {
			channel.freezeTime();
			RetransmissionHandler<MqttMessage> handler = new RetransmissionHandler<>();
			handler.setOriginalMessage(original);
			// Rebuild the packet as the client's handlers do, with the header the timer supplies.
			handler.setHandle((fixedHeader, message) -> channel.writeOutbound(withHeader(fixedHeader, message)));
			handler.start(channel.eventLoop(), 10);

			channel.advanceTimeBy(11, TimeUnit.SECONDS);
			channel.runScheduledPendingTasks();
			handler.stop();

			ByteBuf encoded = channel.readOutbound();
			Assert.assertNotNull("nothing was retransmitted", encoded);
			try {
				return encoded.getUnsignedByte(0);
			} finally {
				encoded.release();
			}
		} finally {
			channel.finishAndReleaseAll();
		}
	}
}
