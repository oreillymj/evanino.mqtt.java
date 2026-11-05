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

import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.util.Arrays;

/**
 * Paho issue 1089: "The connect time for mqtt in version 1.2.5 has increased by approximately
 * 300ms compared to version 1.2.0": Paho 1.2.5 waits more than 300 ms after the TCP handshake
 * before sending CONNECT.
 * <p>
 * Here: a plain server socket measures the time from accepting the connection to the first byte
 * of CONNECT, over several connects after a warm-up. The median must stay well under 300 ms.
 * Needs no broker.
 */
public class PahoIssue1089ConnectDelayTest {

	/**
	 * Measures accept-to-CONNECT for ten connects.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testConnectIsSentPromptlyAfterTcpHandshake() throws Exception {
		long[] delays = new long[10];
		try (ServerSocket server = new ServerSocket(0, 5, InetAddress.getLoopbackAddress())) {
			server.setSoTimeout(10_000);
			URI uri = URI.create("tcp://127.0.0.1:" + server.getLocalPort());
			for (int i = -3; i < delays.length; i++) {   // three warm-up connects
				MqttAsyncClient client = new MqttAsyncClient(uri.toString(), "PahoIssue1089-" + i);
				try {
					MqttConnectOptions o = TestClients.options(uri, "PahoIssue1089-" + i);
					o.setConnectionTimeout(2);
					client.connect(o);
					try (Socket s = server.accept()) {
						long accepted = System.nanoTime();
						s.setSoTimeout(5000);
						InputStream in = s.getInputStream();
						int first = in.read();
						long delayMicros = (System.nanoTime() - accepted) / 1000;
						Assert.assertEquals("first byte is not a CONNECT", 0x10, first);
						if (i >= 0) {
							delays[i] = delayMicros;
						}
					}
				} finally {
					TestClients.shutdownQuietly(client);
				}
			}
		}
		Arrays.sort(delays);
		long median = delays[delays.length / 2];
		System.out.println("accept to CONNECT, microseconds (sorted): " + Arrays.toString(delays));
		Assert.assertTrue("median accept-to-CONNECT " + median + " us", median < 50_000);
	}
}
