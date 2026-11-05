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
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnAckVariableHeader;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.ImmediateEventExecutor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Every incoming PUBLISH payload is released once the client has finished with it, so the
 * socket buffer it points into is freed. Incoming QoS 2 payloads were retained once too often and
 * leaked, about one buffer per message, and payloads of QoS 2 messages dropped without delivery
 * were never released. The client's {@link MqttPingHandler} and {@link MqttChannelHandler} run
 * in an {@link EmbeddedChannel}; each PUBLISH carries a payload with one reference, as the
 * decoder's would, whose count is checked. Not part of Paho's suite; needs no broker.
 */
public class IncomingPayloadReleaseTest {

	private MqttAsyncClient client;
	private EmbeddedChannel channel;

	/**
	 * Closes the channel and the client.
	 */
	@After
	public void tearDown() {
		if (channel != null) {
			channel.finishAndReleaseAll();
		}
		TestClients.shutdownQuietly(client);
	}

	/**
	 * QoS 0 and QoS 1 payloads are released once delivered.
	 */
	@Test(timeout = 10_000)
	public void testQos0AndQos1PayloadsAreReleased() {
		connect(false);
		for (MqttQoS qos : new MqttQoS[]{MqttQoS.AT_MOST_ONCE, MqttQoS.AT_LEAST_ONCE}) {
			ByteBuf payload = payload();
			channel.writeInbound(publish(qos, 1, payload));
			Assert.assertEquals(qos + " payload not released", 0, payload.refCnt());
		}
	}

	/**
	 * A QoS 2 payload is held until PUBREL, then released once delivered.
	 */
	@Test(timeout = 10_000)
	public void testQos2PayloadIsHeldUntilPubrelThenReleased() {
		connect(false);
		ByteBuf payload = payload();
		channel.writeInbound(publish(MqttQoS.EXACTLY_ONCE, 5, payload));
		Assert.assertEquals("payload should be held until PUBREL", 1, payload.refCnt());
		channel.writeInbound(pubrel(5));
		Assert.assertEquals("payload not released after PUBREL", 0, payload.refCnt());
	}

	/**
	 * A QoS 2 PUBLISH sent again by the broker replaces the held one, whose payload is released.
	 */
	@Test(timeout = 10_000)
	public void testReplacedQos2PayloadIsReleased() {
		connect(false);
		ByteBuf first = payload();
		ByteBuf second = payload();
		channel.writeInbound(publish(MqttQoS.EXACTLY_ONCE, 6, first));
		channel.writeInbound(publish(MqttQoS.EXACTLY_ONCE, 6, second));
		Assert.assertEquals("replaced payload not released", 0, first.refCnt());
		Assert.assertEquals(1, second.refCnt());
		channel.writeInbound(pubrel(6));
		Assert.assertEquals(0, second.refCnt());
	}

	/**
	 * A held QoS 2 payload is released when the connection of a clean session closes.
	 */
	@Test(timeout = 10_000)
	public void testHeldQos2PayloadIsReleasedWithItsSession() {
		connect(false);
		ByteBuf payload = payload();
		channel.writeInbound(publish(MqttQoS.EXACTLY_ONCE, 7, payload));
		Assert.assertEquals(1, payload.refCnt());
		channel.close();
		Assert.assertEquals("payload not released when the session ended", 0, payload.refCnt());
	}

	/**
	 * With manual acknowledgements, a QoS 2 payload is released once the message is delivered,
	 * and acknowledging it and the PUBREL release nothing more.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 10_000)
	public void testManualAckQos2PayloadIsReleased() throws Exception {
		CountDownLatch delivered = new CountDownLatch(1);
		connect(true, delivered);
		ByteBuf payload = payload();
		channel.writeInbound(publish(MqttQoS.EXACTLY_ONCE, 8, payload));
		Assert.assertTrue(delivered.await(5, TimeUnit.SECONDS));
		Assert.assertEquals("payload not released after delivery", 0, payload.refCnt());
		channel.runPendingTasks();
		channel.writeInbound(pubrel(8));
		Assert.assertEquals(0, payload.refCnt());
	}

	/**
	 * Connects the handlers to an embedded channel and accepts the connection with CONNACK.
	 *
	 * @param manualAcks whether to use manual acknowledgements
	 */
	private void connect(boolean manualAcks) {
		connect(manualAcks, null);
	}

	/**
	 * Connects the handlers to an embedded channel and accepts the connection with CONNACK.
	 *
	 * @param manualAcks whether to use manual acknowledgements
	 * @param delivered  counted down, and the message acknowledged, as each message arrives; or {@code null}
	 */
	private void connect(boolean manualAcks, CountDownLatch delivered) {
		MqttConnectOptions options = new MqttConnectOptions();
		options.setServerURI("tcp://127.0.0.1:1");
		options.setClientId("IncomingPayloadReleaseTest");
		options.setManualAcks(manualAcks);
		client = new MqttAsyncClient(options);
		if (delivered != null) {
			client.setCallback(new MqttCallbackAdapter() {
				@Override
				public void messageArrived(String topic, MqttMessage message) throws Exception {
					delivered.countDown();
				}
			});
		}
		channel = new EmbeddedChannel(new MqttPingHandler(client, 60),
				new MqttChannelHandler(client, new DefaultPromise<>(ImmediateEventExecutor.INSTANCE)));
		channel.writeInbound(new MqttConnAckMessage(
				new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
				new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_ACCEPTED, false)));
	}

	/**
	 * Returns a payload with one reference, as the decoder hands on.
	 *
	 * @return the payload
	 */
	private static ByteBuf payload() {
		return Unpooled.buffer(4).writeBytes(new byte[]{1, 2, 3, 4});
	}

	/**
	 * Builds an incoming PUBLISH.
	 *
	 * @param qos      the QoS
	 * @param packetId the packet identifier
	 * @param payload  the payload
	 * @return the PUBLISH
	 */
	private static MqttPublishMessage publish(MqttQoS qos, int packetId, ByteBuf payload) {
		return new MqttPublishMessage(new MqttFixedHeader(MqttMessageType.PUBLISH, false, qos, false, 0),
				new MqttPublishVariableHeader("IncomingPayloadReleaseTest", qos == MqttQoS.AT_MOST_ONCE ? -1 : packetId), payload);
	}

	/**
	 * Builds an incoming PUBREL.
	 *
	 * @param packetId the packet identifier
	 * @return the PUBREL
	 */
	private static io.netty.handler.codec.mqtt.MqttMessage pubrel(int packetId) {
		return new io.netty.handler.codec.mqtt.MqttMessage(MqttPubRelFixedHeader.getFixedHeader(),
				MqttMessageIdVariableHeader.from(packetId));
	}
}
