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

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * QoS 0 publishes have back-pressure: a token completes once its PUBLISH is written to the
 * socket, and at most {@link MqttConnectOptions#setMaxUnwrittenQos0(int)} may be waiting to be
 * written, after which {@code publish} throws {@link MqttException#REASON_CODE_MAX_INFLIGHT}.
 * QoS 0 tokens used to complete before anything was written and nothing limited them, so a fast
 * publisher could queue messages without bound. The broker here is a fake that accepts the
 * connection and then stops reading until told to. Not part of Paho's suite; needs no broker.
 */
public class Qos0BackPressureTest {

	/** The QoS 0 window used here. */
	private static final int WINDOW = 20;

	/**
	 * Against a broker that stops reading, QoS 0 publishes are refused once {@value #WINDOW}
	 * are unwritten, and their tokens are not complete; when the broker reads again, every token
	 * completes and publishing works again.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 60_000)
	public void testUnwrittenQos0PublishesAreLimited() throws Exception {
		try (StallingBroker broker = new StallingBroker()) {
			MqttConnectOptions options = new MqttConnectOptions();
			options.setServerURI("tcp://127.0.0.1:" + broker.port());
			options.setClientId("Qos0BackPressureTest");
			options.setMaxUnwrittenQos0(WINDOW);
			options.setKeepAliveInterval(-1);
			MqttAsyncClient client = new MqttAsyncClient(options);
			try {
				client.connect().waitForCompletion(5000);
				byte[] payload = new byte[64 * 1024];
				List<IMqttDeliveryToken> tokens = new ArrayList<>();
				MqttException refused = null;
				for (int i = 0; i < 2000 && refused == null; i++) {
					try {
						tokens.add(client.publish("Qos0BackPressureTest", payload, 0, false));
					} catch (MqttException e) {
						refused = e;
					}
					Thread.sleep(1);
				}
				Assert.assertNotNull("QoS 0 publishes were never refused", refused);
				Assert.assertEquals(MqttException.REASON_CODE_MAX_INFLIGHT, refused.getReasonCode());
				long incomplete = tokens.stream().filter(t -> !t.isComplete()).count();
				Assert.assertTrue("unwritten publishes should not be complete: " + incomplete, incomplete > 0);
				Assert.assertTrue("at most the window should be unwritten: " + incomplete, incomplete <= WINDOW);

				broker.drain();
				for (IMqttDeliveryToken token : tokens) {
					token.waitForCompletion(10_000);
				}
				client.publish("Qos0BackPressureTest", new byte[]{1}, 0, false).waitForCompletion(10_000);
			} finally {
				TestClients.shutdownQuietly(client);
			}
		}
	}

	/**
	 * A one-connection fake broker: answers CONNECT with CONNACK, then reads nothing more until
	 * {@link #drain()} is called, after which it reads and discards everything.
	 */
	private static final class StallingBroker implements AutoCloseable {
		private final ServerSocket server;
		private final CountDownLatch draining = new CountDownLatch(1);
		private volatile Socket socket;

		/**
		 * Starts listening on a free loopback port.
		 *
		 * @throws IOException if the port cannot be opened
		 */
		StallingBroker() throws IOException {
			server = new ServerSocket();
			server.setReceiveBufferSize(4096);   // inherited by the accepted socket
			server.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
			Thread t = new Thread(this::serve, "stalling-broker");
			t.setDaemon(true);
			t.start();
		}

		/**
		 * Returns the listening port.
		 *
		 * @return the port
		 */
		int port() {
			return server.getLocalPort();
		}

		/**
		 * Starts reading again.
		 */
		void drain() {
			draining.countDown();
		}

		/**
		 * Accepts one client, completes the MQTT handshake, stalls, then discards input.
		 */
		private void serve() {
			try {
				socket = server.accept();
				InputStream in = socket.getInputStream();
				OutputStream out = socket.getOutputStream();
				readPacket(in);                              // CONNECT
				out.write(new byte[]{0x20, 0x02, 0x00, 0x00});  // CONNACK, accepted
				out.flush();
				draining.await();
				byte[] buffer = new byte[64 * 1024];
				while (in.read(buffer) != -1) {
					// discard
				}
			} catch (IOException | InterruptedException ignored) {
				// closed
			}
		}

		/**
		 * Reads one MQTT packet: the fixed header byte, the remaining length and the rest.
		 *
		 * @param in the stream
		 * @throws IOException if the stream fails or ends
		 */
		private static void readPacket(InputStream in) throws IOException {
			if (in.read() < 0) {
				throw new IOException("closed");
			}
			int length = 0;
			int shift = 0;
			int b;
			do {
				b = in.read();
				if (b < 0) {
					throw new IOException("closed");
				}
				length |= (b & 0x7F) << shift;
				shift += 7;
			} while ((b & 0x80) != 0);
			for (int i = 0; i < length; i++) {
				if (in.read() < 0) {
					throw new IOException("closed");
				}
			}
		}

		@Override
		public void close() throws IOException {
			draining.countDown();
			if (socket != null) {
				socket.close();
			}
			server.close();
		}
	}
}
