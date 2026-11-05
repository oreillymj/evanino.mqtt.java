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

import io.netty.channel.Channel;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * {@code -1} for {@link MqttConnectOptions#setConnectionTimeout(int)} and
 * {@link MqttConnectOptions#setKeepAliveInterval(int)} means "none" and "off", as Paho's
 * {@code 0} does. It used to be passed on as it was: the connect timeout became -1000 ms, which
 * Netty rejects, so every connect failed with an {@code IllegalArgumentException}, and CONNECT
 * carried a keep-alive of 65535 s. The broker tests require a broker; see
 * {@link MosquittoBroker}.
 */
public class DisabledTimeoutsTest {

	/** The broker used by the connection tests. */
	@ClassRule
	public static final MosquittoBroker broker = new MosquittoBroker();

	/**
	 * The connect timeout given to Netty is the timeout in milliseconds, or 0, meaning none,
	 * for {@code -1}.
	 */
	@Test
	public void testConnectTimeoutMillis() {
		MqttConnectOptions options = new MqttConnectOptions();
		Assert.assertEquals(5000, options.getConnectTimeoutMillis());
		options.setConnectionTimeout(12);
		Assert.assertEquals(12_000, options.getConnectTimeoutMillis());
		options.setConnectionTimeout(-1);
		Assert.assertEquals(-1, options.getConnectionTimeout());
		Assert.assertEquals(0, options.getConnectTimeoutMillis());
	}

	/**
	 * The keep-alive sent in CONNECT is the interval, or 0, meaning off, for {@code -1}.
	 */
	@Test
	public void testConnectKeepAlive() {
		MqttConnectOptions options = new MqttConnectOptions();
		Assert.assertEquals(60, options.getConnectKeepAlive());
		options.setKeepAliveInterval(-1);
		Assert.assertEquals(-1, options.getKeepAliveInterval());
		Assert.assertEquals(0, options.getConnectKeepAlive());
		Assert.assertEquals("no all-idle close when keep-alive is off", 0, options.getKeepAliveMax());
	}

	/**
	 * CONNECT carries the keep-alive interval on the wire: 60 by default, the value set, and 0
	 * for {@code -1} (it was 65535).
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testKeepAliveOnTheWire() throws Exception {
		Assert.assertEquals(60, keepAliveSent(null));
		Assert.assertEquals(20, keepAliveSent(20));
		Assert.assertEquals(0, keepAliveSent(-1));
	}

	/**
	 * With both set to {@code -1}, the client connects, the channel has no connect timeout,
	 * and the connection stays up and usable while idle.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testConnectsWithBothDisabled() throws Exception {
		MqttConnectOptions options = TestClients.options(broker.getServerURI(), "DisabledTimeoutsTest-" + UUID.randomUUID());
		options.setConnectionTimeout(-1);
		options.setKeepAliveInterval(-1);
		MqttAsyncClient client = new MqttAsyncClient(options);
		try {
			client.connect().waitForCompletion(5000);
			Assert.assertEquals(0, channel(client).config().getConnectTimeoutMillis());
			Thread.sleep(2500);
			Assert.assertTrue(client.isConnected());
			TestClients.publish(client, "DisabledTimeoutsTest/" + UUID.randomUUID(), new byte[]{1}, 1, false)
					.waitForCompletion(5000);
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * With the default timeout, the channel's connect timeout is 5 s, so the option reaches
	 * Netty.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testDefaultConnectTimeoutReachesChannel() throws Exception {
		MqttAsyncClient client = new MqttAsyncClient(
				TestClients.options(broker.getServerURI(), "DisabledTimeoutsTest-" + UUID.randomUUID()));
		try {
			client.connect().waitForCompletion(5000);
			Assert.assertEquals(5000, channel(client).config().getConnectTimeoutMillis());
		} finally {
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * Returns the client's current channel.
	 *
	 * @param client the client
	 * @return the channel
	 * @throws Exception if the field cannot be read
	 */
	private static Channel channel(MqttAsyncClient client) throws Exception {
		Field field = MqttAsyncClient.class.getDeclaredField("channel");
		field.setAccessible(true);
		return (Channel) field.get(client);
	}

	/**
	 * Connects a client to a local socket and returns the keep-alive field of the CONNECT
	 * packet it sends.
	 *
	 * @param keepAlive the keep-alive interval to set, or {@code null} for the default
	 * @return the keep-alive in CONNECT, in seconds
	 * @throws Exception if the test fails unexpectedly
	 */
	private static int keepAliveSent(Integer keepAlive) throws Exception {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
			server.setSoTimeout(5000);
			Future<Integer> sent = executor.submit(() -> {
				try (Socket socket = server.accept()) {
					DataInputStream in = new DataInputStream(socket.getInputStream());
					Assert.assertEquals("CONNECT", 0x10, in.readUnsignedByte());
					readRemainingLength(in);
					in.skipBytes(in.readUnsignedShort());   // protocol name
					in.readUnsignedByte();                   // protocol level
					in.readUnsignedByte();                   // connect flags
					return in.readUnsignedShort();
				}
			});
			MqttConnectOptions options = TestClients.options(
					new URI("tcp://127.0.0.1:" + server.getLocalPort()), "DisabledTimeoutsTest-" + UUID.randomUUID());
			if (keepAlive != null) {
				options.setKeepAliveInterval(keepAlive);
			}
			MqttClient client = new MqttClient(options);
			client.setTimeToWait(5000);
			try {
				client.connect(options);
				Assert.fail("The local socket is not a broker");
			} catch (MqttException expected) {
				// no CONNACK
			} finally {
				TestClients.closeQuietly(client);
			}
			return sent.get(5, TimeUnit.SECONDS);
		} finally {
			executor.shutdownNow();
		}
	}

	/**
	 * Reads an MQTT variable-length remaining length.
	 *
	 * @param in the stream
	 * @return the remaining length
	 * @throws Exception if the stream ends
	 */
	private static int readRemainingLength(InputStream in) throws Exception {
		int value = 0;
		int multiplier = 1;
		int b;
		do {
			b = in.read();
			value += (b & 0x7f) * multiplier;
			multiplier *= 128;
		} while ((b & 0x80) != 0);
		return value;
	}
}
