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
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttVersion;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Paho issue 1101 reports CVE-2025-10543 against the Java client. The CVE is in Paho Go: a UTF-8
 * string longer than 65535 bytes had its length truncated to 16 bits but was written in full,
 * so the excess leaked into the next field (for example the topic into the payload).
 * <p>
 * These tests check the same pattern here: a string field that cannot fit in MQTT's 16-bit
 * length must be refused, never written with a wrapped length. The encoder tests need no
 * broker; the client test uses {@link MosquittoBroker}.
 */
public class PahoIssue1101LongStringTest {

	/** The broker for the client test. */
	@ClassRule
	public static final MosquittoBroker BROKER = new MosquittoBroker();

	/** 65536 + 10 bytes: one more than MQTT allows, plus a recognisable tail. */
	private static final String LONG = repeat('t', 65536) + "LEAKEDTAIL";

	/**
	 * Netty's encoder, which this client uses, refuses a PUBLISH whose topic is too long rather
	 * than writing a truncated length.
	 */
	@Test
	public void testEncoderRefusesLongPublishTopic() {
		MqttPublishMessage publish = MqttMessageBuilders.publish()
				.topicName(LONG).qos(MqttQoS.AT_MOST_ONCE).retained(false)
				.payload(Unpooled.wrappedBuffer("payload".getBytes(StandardCharsets.UTF_8))).build();
		assertRefusedOrIntact(publish, "PUBLISH topic");
	}

	/**
	 * The same for a SUBSCRIBE topic filter.
	 */
	@Test
	public void testEncoderRefusesLongSubscribeFilter() {
		MqttMessage subscribe = MqttMessageBuilders.subscribe().messageId(1)
				.addSubscription(MqttQoS.AT_LEAST_ONCE, LONG).build();
		assertRefusedOrIntact(subscribe, "SUBSCRIBE filter");
	}

	/**
	 * The same for CONNECT's user name and will topic.
	 */
	@Test
	public void testEncoderRefusesLongConnectStrings() {
		MqttMessage connect = MqttMessageBuilders.connect().clientId("c").protocolVersion(MqttVersion.MQTT_3_1_1)
				.username(LONG).build();
		assertRefusedOrIntact(connect, "CONNECT user name");
		MqttMessage will = MqttMessageBuilders.connect().clientId("c").protocolVersion(MqttVersion.MQTT_3_1_1)
				.willFlag(true).willTopic(LONG).willMessage("w".getBytes(StandardCharsets.UTF_8)).build();
		assertRefusedOrIntact(will, "CONNECT will topic");
	}

	/**
	 * Publishing with a too-long topic through the client is refused at once, nothing reaches
	 * subscribers on another topic (with the wrapped length, the broker sees the topic
	 * {@code tttttttttt} and the rest of the topic as the payload), and the connection stays
	 * usable.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testClientRefusesLongTopic() throws Exception {
		clientRefusesLongTopic(1);
	}

	/**
	 * The same at QoS 0.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testClientRefusesLongTopicQos0() throws Exception {
		clientRefusesLongTopic(0);
	}

	/**
	 * What the client writes on the wire for a too-long topic, read by a minimal fake broker
	 * that accepts the CONNECT and decodes the next packet. Passes if nothing is written.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testClientWritesNoWrappedLength() throws Exception {
		try (java.net.ServerSocket server = new java.net.ServerSocket(0, 1, java.net.InetAddress.getLoopbackAddress())) {
			server.setSoTimeout(10_000);
			MqttAsyncClient client = new MqttAsyncClient("tcp://127.0.0.1:" + server.getLocalPort(), "PahoIssue1101-wire");
			try {
				IMqttToken connect = client.connect(TestClients.options(java.net.URI.create("tcp://127.0.0.1:" + server.getLocalPort()), "PahoIssue1101-wire"));
				try (java.net.Socket s = server.accept()) {
					s.setSoTimeout(3000);
					java.io.DataInputStream in = new java.io.DataInputStream(s.getInputStream());
					readPacket(in);                                         // CONNECT
					s.getOutputStream().write(new byte[]{0x20, 0x02, 0x00, 0x00}); // CONNACK, accepted
					connect.waitForCompletion(5000);
					try {
						client.publish(LONG, "payload".getBytes(StandardCharsets.UTF_8), 0, false);
					} catch (MqttException | IllegalArgumentException e) {
						System.out.println("wire: refused with " + e);
						return;
					}
					byte[] packet;
					try {
						packet = readPacket(in);
					} catch (java.net.SocketTimeoutException e) {
						System.out.println("wire: nothing written");
						return;
					}
					int topicLength = ((packet[0] & 0xff) << 8) | (packet[1] & 0xff);
					String topic = new String(packet, 2, topicLength, StandardCharsets.UTF_8);
					String payload = new String(packet, 2 + topicLength, packet.length - 2 - topicLength, StandardCharsets.UTF_8);
					String seen = "PUBLISH with topic length " + topicLength + " (\"" + topic + "\") and a payload of "
							+ payload.length() + " bytes ending \"" + payload.substring(Math.max(0, payload.length() - 17)) + "\"";
					System.out.println("wire: " + seen);
					Assert.fail("the client wrote " + seen);
				}
			} finally {
				TestClients.shutdownQuietly(client);
			}
		}
	}

	/** Reads one MQTT packet and returns its variable header and payload. */
	private static byte[] readPacket(java.io.DataInputStream in) throws java.io.IOException {
		in.readUnsignedByte();
		int length = 0;
		int multiplier = 1;
		int b;
		do {
			b = in.readUnsignedByte();
			length += (b & 0x7f) * multiplier;
			multiplier *= 128;
		} while ((b & 0x80) != 0);
		byte[] body = new byte[length];
		in.readFully(body);
		return body;
	}

	private void clientRefusesLongTopic(int qos) throws Exception {
		MqttClient sub = TestClients.createMqttClient(BROKER.getServerURI(), "PahoIssue1101-sub");
		MqttClient client = TestClients.createMqttClient(BROKER.getServerURI(), "PahoIssue1101");
		java.util.List<String> seen = java.util.Collections.synchronizedList(new java.util.ArrayList<>());
		try {
			sub.connect(TestClients.options(BROKER.getServerURI(), "PahoIssue1101-sub"));
			sub.subscribe("#", 1, (topic, m) -> seen.add(topic + " payload=" + m.getPayload().length + " bytes"
					+ (new String(m.getPayload(), StandardCharsets.UTF_8).contains("LEAKEDTAIL") ? " containing the topic's tail" : "")));
			MqttConnectOptions options = TestClients.options(BROKER.getServerURI(), "PahoIssue1101");
			client.connect(options);
			Throwable refusal = null;
			long start = System.nanoTime();
			try {
				client.publish(LONG, "payload".getBytes(StandardCharsets.UTF_8), qos, false);
			} catch (MqttException | IllegalArgumentException e) {
				refusal = e;
			}
			long ms = (System.nanoTime() - start) / 1_000_000;
			Thread.sleep(1000);
			System.out.println("QoS " + qos + ": publish ended after " + ms + " ms with " + refusal + "; subscriber saw " + seen
					+ "; publisher still connected=" + client.isConnected());
			Assert.assertTrue("delivered to subscribers: " + seen, seen.isEmpty());
			Assert.assertNotNull("a 65546-byte topic was accepted", refusal);
			Assert.assertTrue("refused only after " + ms + " ms (a timeout, not validation): " + refusal, ms < 1000);
			client.publish("PahoIssue1101/ok", "ok".getBytes(StandardCharsets.UTF_8), 1, false);
			Assert.assertTrue("disconnected after the long publish", client.isConnected());
		} finally {
			TestClients.closeQuietly(client);
			TestClients.closeQuietly(sub);
		}
	}

	/**
	 * Encodes a message. Passes if the encoder refuses it; if it writes it, decodes it again and
	 * fails if the long string did not survive intact.
	 */
	private static void assertRefusedOrIntact(MqttMessage message, String what) {
		EmbeddedChannel encoder = new EmbeddedChannel(MqttEncoder.INSTANCE);
		ByteBuf encoded;
		try {
			encoder.writeOutbound(message);
			encoded = encoder.readOutbound();
		} catch (Exception e) {
			System.out.println(what + ": refused by the encoder: " + e);
			return;
		} finally {
			encoder.finishAndReleaseAll();
		}
		Assert.assertNotNull(what + ": nothing encoded", encoded);
		EmbeddedChannel decoder = new EmbeddedChannel(new MqttDecoder(1 << 20));
		try {
			decoder.writeInbound(encoded);
			Object decoded = decoder.readInbound();
			String text = String.valueOf(decoded);
			if (decoded instanceof MqttPublishMessage) {
				MqttPublishMessage p = (MqttPublishMessage) decoded;
				text = "topic \"" + p.variableHeader().topicName() + "\", payload of "
						+ p.payload().readableBytes() + " bytes ending \""
						+ p.payload().toString(StandardCharsets.UTF_8).substring(Math.max(0, p.payload().readableBytes() - 17)) + "\"";
			}
			boolean intact = decoded instanceof MqttPublishMessage
					? LONG.equals(((MqttPublishMessage) decoded).variableHeader().topicName())
					: String.valueOf(decoded).contains(LONG);
			Assert.assertTrue(what + ": encoded with a wrapped length; decoded as " + abbreviate(text), intact);
		} finally {
			decoder.finishAndReleaseAll();
		}
	}

	private static String abbreviate(String s) {
		return s.length() <= 300 ? s : s.substring(0, 150) + " ... " + s.substring(s.length() - 150);
	}

	private static String repeat(char c, int n) {
		char[] chars = new char[n];
		Arrays.fill(chars, c);
		return new String(chars);
	}
}
