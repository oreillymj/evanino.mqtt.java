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

import java.util.concurrent.TimeUnit;

/**
 * Paho issue 1093: "Race condition between disconnect() and close() leaves zombie client/threads
 * behind". {@code client.disconnect(); client.close();} sends DISCONNECT, but close() sees the
 * disconnect in progress and only sets a flag, which then makes the disconnect thread skip its
 * cleanup: the socket stays open and three threads are left behind.
 * <p>
 * Here: after {@code disconnect()} immediately followed by {@code close()}, the channel must be
 * closed, the disconnect token complete, and the client's own threads (its callback executors)
 * gone. The shared Netty event loop is shared by design and not counted. Uses
 * {@link MosquittoBroker}.
 */
public class PahoIssue1093DisconnectCloseTest {

	/** The broker. */
	@ClassRule
	public static final MosquittoBroker BROKER = new MosquittoBroker();

	/**
	 * Repeats the issue's two calls, so a race shows up even if it is not hit every time.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testDisconnectThenCloseCleansUp() throws Exception {
		int baseline = clientThreads();
		for (int i = 0; i < 20; i++) {
			MqttAsyncClient client = TestClients.createMqttAsyncClient(BROKER.getServerURI(), "PahoIssue1093-" + i);
			client.setCallback(new MqttCallbackAdapter());
			client.connect(TestClients.options(BROKER.getServerURI(), "PahoIssue1093-" + i)).waitForCompletion(10_000);
			Assert.assertTrue(client.isConnected());
			Channel channel = client.getChannel();

			IMqttToken disconnect = client.disconnect();
			client.close();

			Assert.assertTrue("run " + i + ": channel still open 5 s after close()",
					channel.closeFuture().await(5, TimeUnit.SECONDS));
			disconnect.waitForCompletion(5000);
			Assert.assertTrue("run " + i + ": disconnect token not complete", disconnect.isComplete());
			Assert.assertFalse(client.isConnected());
		}
		long deadline = System.currentTimeMillis() + 5000;
		while (clientThreads() > baseline && System.currentTimeMillis() < deadline) {
			Thread.sleep(100);
		}
		Assert.assertEquals("client threads left behind: " + threadNames(), baseline, clientThreads());
	}

	/** Counts the clients' own threads (callback executors), not the shared event loop. */
	private static int clientThreads() {
		int n = 0;
		for (Thread t : Thread.getAllStackTraces().keySet()) {
			if (t.isAlive() && t.getName().startsWith("mqtt-callback-") && !t.getName().equals("mqtt-callback-after-close")) {
				n++;
			}
		}
		return n;
	}

	private static String threadNames() {
		StringBuilder b = new StringBuilder();
		for (Thread t : Thread.getAllStackTraces().keySet()) {
			if (t.getName().startsWith("mqtt-")) {
				b.append(t.getName()).append(' ');
			}
		}
		return b.toString();
	}
}
