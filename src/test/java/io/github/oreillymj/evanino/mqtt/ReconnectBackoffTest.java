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

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Automatic reconnect backs off as in Paho: the wait starts at the retry interval and doubles
 * after each failed attempt, up to {@link MqttConnectOptions#setMaxReconnectDelay(int)}. Not
 * part of Paho's suite. The timing test requires a broker; see {@link MosquittoBroker}.
 */
public class ReconnectBackoffTest {

	/** The broker used by the timing test. */
	@ClassRule
	public static final MosquittoBroker broker = new MosquittoBroker();

	/** Gives each test its own client ID. */
	@Rule
	public final TestName name = new TestName();

	private static ConnectionManipulationProxyServer proxy;
	private static URI proxyURI;

	/**
	 * Starts a proxy in front of the broker, so the network can be dropped.
	 *
	 * @throws Exception if the proxy cannot be started
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		URI serverURI = broker.getServerURI();
		proxy = new ConnectionManipulationProxyServer(serverURI.getHost(), serverURI.getPort(), 0);
		proxy.startProxy();
		while (!proxy.isPortSet()) {
			Thread.sleep(0);
		}
		proxyURI = new URI("tcp://localhost:" + proxy.getLocalPort());
	}

	/**
	 * Stops the proxy.
	 */
	@AfterClass
	public static void tearDownAfterClass() {
		if (proxy != null) {
			proxy.stopProxy();
		}
	}

	/**
	 * The wait doubles with each attempt and stops at the cap.
	 */
	@Test
	public void testDelayDoublesUpToCap() {
		long[] expected = {1000, 2000, 4000, 8000, 16000, 32000, 64000, 128000, 128000, 128000};
		for (int attempt = 1; attempt <= expected.length; attempt++) {
			Assert.assertEquals("attempt " + attempt, expected[attempt - 1],
					MqttAsyncClient.reconnectDelayMillis(1, attempt, 128000));
		}
		Assert.assertEquals("attempt 0 is treated as the first", 1000, MqttAsyncClient.reconnectDelayMillis(1, 0, 128000));
		Assert.assertEquals(5000, MqttAsyncClient.reconnectDelayMillis(5, 1, 128000));
		Assert.assertEquals(10000, MqttAsyncClient.reconnectDelayMillis(5, 2, 128000));
		Assert.assertEquals("a cap below the interval wins", 500, MqttAsyncClient.reconnectDelayMillis(2, 1, 500));
		Assert.assertEquals("an odd cap is reached exactly", 2500, MqttAsyncClient.reconnectDelayMillis(1, 3, 2500));
		Assert.assertEquals("a zero interval stays zero", 0, MqttAsyncClient.reconnectDelayMillis(0, 5, 128000));
		Assert.assertEquals("many attempts do not overflow", Integer.MAX_VALUE,
				MqttAsyncClient.reconnectDelayMillis(1, Integer.MAX_VALUE, Integer.MAX_VALUE));
	}

	/**
	 * {@code setMaxReconnectDelay} rejects values below 1 ms, and defaults to 128 seconds as in
	 * Paho.
	 */
	@Test
	public void testMaxReconnectDelayValidation() {
		MqttConnectOptions options = new MqttConnectOptions();
		Assert.assertEquals(128000, options.getMaxReconnectDelay());
		for (int bad : new int[]{0, -1}) {
			try {
				options.setMaxReconnectDelay(bad);
				Assert.fail("accepted " + bad);
			} catch (IllegalArgumentException expected) {
				// rejected
			}
		}
		options.setMaxReconnectDelay(1);
		Assert.assertEquals(1, options.getMaxReconnectDelay());
	}

	/**
	 * With the broker unreachable, the wait between a failed reconnect attempt and the next one
	 * grows 1 s, 2 s, then stops at the 2.5 s cap; the client reconnects once the broker is back.
	 * The wait is measured from the end of each attempt, because a refused connection can itself
	 * take time (about 2 s on Windows).
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 60_000)
	public void testReconnectAttemptsBackOff() throws Exception {
		List<Long> attempts = new ArrayList<>();   // when each attempt started
		List<Long> failures = new ArrayList<>();   // when each attempt ended
		MqttConnectOptions options = TestClients.options(proxyURI, name.getMethodName());
		options.setAutomaticReconnect(true);
		options.setRetryInterval(1);
		options.setMaxReconnectDelay(2500);
		options.setMaxRetries(-1);
		MqttAsyncClient client = new MqttAsyncClient(options);
		client.setCallback(new MqttCallbackAdapter() {
			@Override
			public void onConnecting(boolean connecting, String serverURI) {
				synchronized (attempts) {
					(connecting ? attempts : failures).add(System.currentTimeMillis());
					attempts.notifyAll();
				}
			}
		});
		try {
			client.connect().waitForCompletion(5000);
			synchronized (attempts) {
				attempts.clear();   // ignore the first connect
				failures.clear();
			}
			long lost = System.currentTimeMillis();
			proxy.disableProxy();

			synchronized (attempts) {
				long deadline = System.currentTimeMillis() + 20_000;
				while (attempts.size() < 4 && System.currentTimeMillis() < deadline) {
					attempts.wait(deadline - System.currentTimeMillis());
				}
				Assert.assertTrue("too few reconnect attempts: " + attempts.size(), attempts.size() >= 4);
				long first = attempts.get(0) - lost;
				long gap1 = attempts.get(1) - endOf(attempts.get(0), failures);
				long gap2 = attempts.get(2) - endOf(attempts.get(1), failures);
				long gap3 = attempts.get(3) - endOf(attempts.get(2), failures);
				String timings = "first=" + first + " gaps=" + gap1 + "," + gap2 + "," + gap3;
				Assert.assertTrue("first attempt should come after about 1 s: " + timings, first >= 800 && first < 2000);
				Assert.assertTrue("second wait should be about 2 s: " + timings, gap1 >= 1800 && gap1 < 2400);
				Assert.assertTrue("third wait should stop at the 2.5 s cap: " + timings, gap2 >= 2300 && gap2 < 2900);
				Assert.assertTrue("fourth wait should stay at the cap: " + timings, gap3 >= 2300 && gap3 < 2900);
			}

			proxy.enableProxy();
			long deadline = System.currentTimeMillis() + 10_000;
			while (!client.isConnected() && System.currentTimeMillis() < deadline) {
				Thread.sleep(100);
			}
			Assert.assertTrue("did not reconnect once the broker was back", client.isConnected());
		} finally {
			proxy.enableProxy();
			TestClients.shutdownQuietly(client);
		}
	}

	/**
	 * Returns when an attempt ended: the first {@code onConnecting(false)} after it started.
	 * Earlier ones, such as the one raised when the connection is lost, are skipped.
	 *
	 * @param start    when the attempt started
	 * @param failures when attempts ended, in order
	 * @return the end of the attempt
	 */
	private static long endOf(long start, List<Long> failures) {
		for (long end : failures) {
			if (end >= start) {
				return end;
			}
		}
		Assert.fail("attempt started at " + start + " never ended; ends: " + failures);
		return start;
	}
}
