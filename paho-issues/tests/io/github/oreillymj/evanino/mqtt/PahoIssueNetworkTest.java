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
import org.junit.ClassRule;
import org.junit.Test;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Older Paho issues about the network and the client's life cycle. Each test names the issues it
 * covers. Uses {@link UnlimitedMosquitto}, {@link BlackholeRelay} and {@link RawBroker}.
 */
public class PahoIssueNetworkTest {

	/** The broker. */
	@ClassRule
	public static final UnlimitedMosquitto BROKER = new UnlimitedMosquitto();

	/**
	 * 962, 925, 891, 890, 770, 696, 466, 467: the network silently stops (no FIN or RST, as when
	 * a cable is pulled). connectionLost comes within about 2 times the keep-alive, the old socket
	 * is closed, and once the network is back the client reconnects by itself.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testSilentNetworkLossIsDetected() throws Exception {
		try (BlackholeRelay relay = new BlackholeRelay(BROKER.getServerURI().getPort())) {
			MqttAsyncClient client = new MqttAsyncClient(relay.uri().toString(), "PahoIssueBlackhole");
			AtomicReference<Long> lostAt = new AtomicReference<>();
			CountDownLatch reconnected = new CountDownLatch(1);
			try {
				MqttConnectOptions o = TestClients.options(relay.uri(), "PahoIssueBlackhole");
				o.setKeepAliveInterval(2);
				o.setAutomaticReconnect(true);
				o.setRetryInterval(1);
				o.setMaxReconnectDelay(1000);
				o.setMaxRetries(-1);
				client.setCallback(new MqttCallbackAdapter() {
					@Override
					public void connectionLost(Throwable cause) {
						lostAt.compareAndSet(null, System.nanoTime());
					}

					@Override
					public void connectComplete(boolean reconnect, String serverURI) {
						if (reconnect) {
							reconnected.countDown();
						}
					}
				});
				client.connect(o).waitForCompletion(10_000);
				long start = System.nanoTime();
				relay.blackhole(true);
				long deadline = System.currentTimeMillis() + 15_000;
				while (lostAt.get() == null && System.currentTimeMillis() < deadline) {
					Thread.sleep(20);
				}
				Assert.assertNotNull("connection loss never detected", lostAt.get());
				long ms = (lostAt.get() - start) / 1_000_000;
				System.out.println("silent network loss detected after " + ms + " ms (keep-alive 2 s)");
				Assert.assertTrue("detected after " + ms + " ms", ms <= 5000);
				relay.blackhole(false);
				Assert.assertTrue("did not reconnect", reconnected.await(20, TimeUnit.SECONDS));
				Assert.assertEquals("connections left open", 1, relay.openConnections());
			} finally {
				TestClients.shutdownQuietly(client);
			}
		}
	}

	/**
	 * 716, 576: a subscriber receives a constant stream of QoS 0 messages and sends nothing of
	 * its own. The broker (which expects a packet from the client within 1.5 times the
	 * keep-alive) must not disconnect it: the client still sends PINGREQ.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testKeepAliveWhileOnlyReceiving() throws Exception {
		MqttAsyncClient sub = TestClients.createMqttAsyncClient(BROKER.getServerURI(), "PahoIssue716-sub");
		MqttAsyncClient pub = TestClients.createMqttAsyncClient(BROKER.getServerURI(), "PahoIssue716-pub");
		AtomicInteger lost = new AtomicInteger();
		AtomicInteger received = new AtomicInteger();
		try {
			MqttConnectOptions o = TestClients.options(BROKER.getServerURI(), "PahoIssue716-sub");
			o.setKeepAliveInterval(2);
			sub.setCallback(new MqttCallbackAdapter() {
				@Override
				public void connectionLost(Throwable cause) {
					lost.incrementAndGet();
				}

				@Override
				public void messageArrived(String topic, MqttMessage message) {
					received.incrementAndGet();
				}
			});
			sub.connect(o).waitForCompletion(10_000);
			sub.subscribe("PahoIssue716/t", 0).waitForCompletion(10_000);
			pub.connect(TestClients.options(BROKER.getServerURI(), "PahoIssue716-pub")).waitForCompletion(10_000);
			long end = System.currentTimeMillis() + 10_000;
			while (System.currentTimeMillis() < end) {
				pub.publish("PahoIssue716/t", new byte[]{1}, 0, false);
				Thread.sleep(50);
			}
			Assert.assertEquals("disconnected while only receiving", 0, lost.get());
			Assert.assertTrue(sub.isConnected());
			Assert.assertTrue(received.get() > 100);
		} finally {
			TestClients.shutdownQuietly(pub);
			TestClients.shutdownQuietly(sub);
		}
	}

	/**
	 * 815: after a reconnect, keep-alive still works: PINGREQs at most the keep-alive apart.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testKeepAliveAfterReconnect() throws Exception {
		try (RawBroker broker = new RawBroker()) {
			MqttAsyncClient client = new MqttAsyncClient(broker.uri().toString(), "PahoIssue815");
			try {
				MqttConnectOptions o = TestClients.options(broker.uri(), "PahoIssue815");
				o.setKeepAliveInterval(2);
				o.setAutomaticReconnect(true);
				o.setRetryInterval(1);
				o.setMaxRetries(-1);
				client.setCallback(new MqttCallbackAdapter());
				client.connect(o);
				broker.acceptAndConnack(0);
				broker.readUntil(12);
				broker.acceptAndConnack(0);   // closes the first connection; the client reconnects
				long last = System.nanoTime();
				List<Long> gaps = new ArrayList<>();
				for (int i = 0; i < 3; i++) {
					RawBroker.Packet p = broker.read();
					Assert.assertEquals(12, p.type());
					long now = System.nanoTime();
					gaps.add((now - last) / 1_000_000);
					last = now;
					broker.write(0xd0, 0x00);
				}
				System.out.println("PINGREQ gaps after reconnect (ms): " + gaps);
				for (long g : gaps) {
					Assert.assertTrue("gap " + g + " ms", g <= 2100);
				}
			} finally {
				TestClients.shutdownQuietly(client);
			}
		}
	}

	/**
	 * 812: QoS 0 publishes in flight when the connection fails all complete (succeed or
	 * fail); none is left waiting.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testQos0TokensCompleteOnConnectionFailure() throws Exception {
		ConnectionManipulationProxyServer proxy = new ConnectionManipulationProxyServer("127.0.0.1", BROKER.getServerURI().getPort(), 0);
		proxy.startProxy();
		URI uri = URI.create("tcp://127.0.0.1:" + proxy.getLocalPort());
		MqttAsyncClient client = new MqttAsyncClient(uri.toString(), "PahoIssue812");
		try {
			client.connect(TestClients.options(uri, "PahoIssue812")).waitForCompletion(10_000);
			List<IMqttDeliveryToken> tokens = new ArrayList<>();
			Thread killer = new Thread(() -> {
				try {
					Thread.sleep(5);
				} catch (InterruptedException ignored) {
					// no
				}
				proxy.disableProxy();
			});
			killer.start();
			for (int i = 0; i < 20_000; i++) {
				try {
					tokens.add(client.publish("PahoIssue812/t", new byte[200], 0, false));
				} catch (MqttException e) {
					// not connected any more, or window full: fine
				}
			}
			killer.join();
			int pending = 0;
			for (IMqttDeliveryToken t : tokens) {
				try {
					t.waitForCompletion(5000);
				} catch (MqttException ignored) {
					// failed: fine
				}
				if (!t.isComplete()) {
					pending++;
				}
			}
			Assert.assertEquals("QoS 0 tokens never completed, of " + tokens.size(), 0, pending);
		} finally {
			TestClients.shutdownQuietly(client);
			proxy.stopProxy();
		}
	}

	/**
	 * 571: MqttClient used from 8 threads at once for QoS 0 publishes; every call returns.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testSyncQos0FromManyThreads() throws Exception {
		MqttClient client = TestClients.createMqttClient(BROKER.getServerURI(), "PahoIssue571");
		ExecutorService pool = Executors.newFixedThreadPool(8);
		try {
			client.connect(TestClients.options(BROKER.getServerURI(), "PahoIssue571"));
			List<Future<?>> results = new ArrayList<>();
			for (int t = 0; t < 8; t++) {
				results.add(pool.submit(() -> {
					for (int i = 0; i < 2000; i++) {
						client.publish("PahoIssue571/t", new byte[]{1}, 0, false);
					}
					return null;
				}));
			}
			for (Future<?> f : results) {
				f.get(60, TimeUnit.SECONDS);
			}
		} finally {
			pool.shutdownNow();
			TestClients.closeQuietly(client);
		}
	}

	/**
	 * 747, 693, 678, 654: creating, connecting and closing clients over and over (to a good
	 * broker and to one that refuses) leaves no threads behind: once the last client is closed,
	 * the thread count is back where it started.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testNoThreadsLeftAfterManyClients() throws Exception {
		int before = Thread.activeCount();
		int refused;
		try (java.net.ServerSocket s = new java.net.ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
			refused = s.getLocalPort();
		}
		for (int i = 0; i < 100; i++) {
			MqttAsyncClient c = TestClients.createMqttAsyncClient(BROKER.getServerURI(), "PahoIssueLeak-" + i);
			c.setCallback(new MqttCallbackAdapter());
			c.connect(TestClients.options(BROKER.getServerURI(), "PahoIssueLeak-" + i)).waitForCompletion(10_000);
			c.publish("PahoIssueLeak/t", new byte[]{1}, 1, false).waitForCompletion(10_000);
			c.close();
			if (i % 10 == 0) {
				URI bad = URI.create("tcp://127.0.0.1:" + refused);
				MqttAsyncClient b = new MqttAsyncClient(bad.toString(), "PahoIssueLeak-bad" + i);
				MqttConnectOptions o = TestClients.options(bad, "PahoIssueLeak-bad" + i);
				o.setConnectionTimeout(1);
				try {
					b.connect(o).waitForCompletion(10_000);
				} catch (MqttException expected) {
					// refused
				}
				b.close();
			}
		}
		long deadline = System.currentTimeMillis() + 10_000;
		while (Thread.activeCount() > before + 1 && System.currentTimeMillis() < deadline) {
			Thread.sleep(100);
		}
		StringBuilder names = new StringBuilder();
		for (Thread t : Thread.getAllStackTraces().keySet()) {
			names.append(t.getName()).append(", ");
		}
		Assert.assertTrue("threads before " + before + ", after " + Thread.activeCount() + ": " + names,
				Thread.activeCount() <= before + 1);
	}

	/**
	 * 632: disconnectForcibly() on a client with automatic reconnect stops it reconnecting.
	 * 875, 641: close(true) on a connected client closes it at once.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testForcedDisconnectAndClose() throws Exception {
		try (RawBroker broker = new RawBroker()) {
			MqttAsyncClient client = new MqttAsyncClient(broker.uri().toString(), "PahoIssue632");
			MqttConnectOptions o = TestClients.options(broker.uri(), "PahoIssue632");
			o.setAutomaticReconnect(true);
			o.setRetryInterval(1);
			o.setMaxRetries(-1);
			client.setCallback(new MqttCallbackAdapter());
			IMqttToken t = client.connect(o);
			broker.acceptAndConnack(0);
			t.waitForCompletion(5000);
			client.disconnectForcibly();
			Assert.assertFalse(client.isConnected());
			Assert.assertFalse(client.isReconnecting());
			try {
				broker.accept();
				Assert.fail("reconnected after disconnectForcibly()");
			} catch (java.net.SocketTimeoutException expected) {
				// no reconnect within 15 s
			}

			IMqttToken again = client.connect(o);
			broker.acceptAndConnack(0);
			again.waitForCompletion(5000);
			long start = System.nanoTime();
			client.close(true);
			long ms = (System.nanoTime() - start) / 1_000_000;
			Assert.assertTrue("close(true) took " + ms + " ms", ms < 2000);
			Assert.assertTrue("socket still open after close(true)", broker.isClosedByPeer());
		}
	}

	/**
	 * 686: after a connect that failed, disconnect() and close() work.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testDisconnectAndCloseAfterFailedConnect() throws Exception {
		try (RawBroker broker = new RawBroker()) {
			MqttAsyncClient client = new MqttAsyncClient(broker.uri().toString(), "PahoIssue686");
			IMqttToken t = client.connect(TestClients.options(broker.uri(), "PahoIssue686"));
			broker.acceptAndConnack(5);   // not authorised
			try {
				t.waitForCompletion(5000);
				Assert.fail("connect succeeded");
			} catch (MqttException expected) {
				// refused
			}
			client.disconnect().waitForCompletion(5000);
			client.close();
		}
	}

	/**
	 * 399: a thread interrupted while blocked in MqttClient.connect gets an exception, and
	 * the client can then be closed.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testInterruptDuringSyncConnect() throws Exception {
		try (RawBroker broker = new RawBroker()) {
			MqttClient client = new MqttClient(broker.uri().toString(), "PahoIssue399");
			AtomicReference<Throwable> result = new AtomicReference<>();
			Thread t = new Thread(() -> {
				try {
					client.connect(TestClients.options(broker.uri(), "PahoIssue399"));
					result.set(new AssertionError("connected"));
				} catch (Throwable e) {
					result.set(e);
				}
			});
			t.start();
			broker.accept();      // TCP up, CONNACK never sent
			Thread.sleep(300);
			t.interrupt();
			t.join(5000);
			Assert.assertFalse("connect still blocked after interrupt", t.isAlive());
			System.out.println("interrupted connect: " + result.get());
			client.close(true);
		}
	}

	/**
	 * 763: a CONNACK that takes 3 s to come is waited for, and the connect completes.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testSlowConnack() throws Exception {
		try (RawBroker broker = new RawBroker()) {
			MqttAsyncClient client = new MqttAsyncClient(broker.uri().toString(), "PahoIssue763");
			try {
				MqttConnectOptions o = TestClients.options(broker.uri(), "PahoIssue763");
				o.setCleanSession(false);
				IMqttToken t = client.connect(o);
				broker.accept();
				broker.read();
				Thread.sleep(3000);
				broker.write(0x20, 0x02, 0x01, 0x00);   // session present
				t.waitForCompletion(5000);
				Assert.assertTrue(client.isConnected());
			} finally {
				TestClients.shutdownQuietly(client);
			}
		}
	}

	/**
	 * 722: publishing from the connect token's onSuccess works.
	 * 568: connecting again, and waiting for it, from connectionLost works.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testCallsFromCallbacks() throws Exception {
		ConnectionManipulationProxyServer proxy = new ConnectionManipulationProxyServer("127.0.0.1", BROKER.getServerURI().getPort(), 0);
		proxy.startProxy();
		URI uri = URI.create("tcp://127.0.0.1:" + proxy.getLocalPort());
		MqttAsyncClient client = new MqttAsyncClient(uri.toString(), "PahoIssue722");
		List<String> events = new CopyOnWriteArrayList<>();
		CountDownLatch reconnected = new CountDownLatch(1);
		try {
			MqttConnectOptions o = TestClients.options(uri, "PahoIssue722");
			client.setCallback(new MqttCallbackAdapter() {
				@Override
				public void connectionLost(Throwable cause) {
					try {
						Thread.sleep(500);
						proxy.enableProxy();
						client.connect(o).waitForCompletion(10_000);
						events.add("reconnected from connectionLost");
						reconnected.countDown();
					} catch (Exception e) {
						events.add("connect from connectionLost failed: " + e);
						reconnected.countDown();
					}
				}
			});
			CountDownLatch published = new CountDownLatch(1);
			client.connect(o, null, new IMqttActionListener() {
				@Override
				public void onSuccess(IMqttToken asyncActionToken) {
					try {
						client.publish("PahoIssue722/t", new byte[]{1}, 1, false).waitForCompletion(10_000);
						events.add("published from onSuccess");
					} catch (Exception e) {
						events.add("publish from onSuccess failed: " + e);
					}
					published.countDown();
				}

				@Override
				public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
					published.countDown();
				}
			});
			Assert.assertTrue(published.await(20, TimeUnit.SECONDS));
			proxy.disableProxy();
			Assert.assertTrue(reconnected.await(30, TimeUnit.SECONDS));
			Assert.assertEquals(java.util.Arrays.asList("published from onSuccess", "reconnected from connectionLost"), events);
		} finally {
			TestClients.shutdownQuietly(client);
			proxy.stopProxy();
		}
	}

	/**
	 * 322, 734: a broker URI with an IPv6 literal, tcp://[::1]:port, connects.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testIpv6Literal() throws Exception {
		InetAddress v6 = InetAddress.getByName("::1");
		org.junit.Assume.assumeTrue(v6 instanceof Inet6Address);
		try (RawBroker broker = new RawBroker(v6)) {
			String uri = "tcp://[::1]:" + broker.port();
			MqttAsyncClient client = new MqttAsyncClient(uri, "PahoIssue322");
			try {
				MqttConnectOptions o = TestClients.options(URI.create(uri), "PahoIssue322");
				IMqttToken t = client.connect(o);
				broker.acceptAndConnack(0);
				t.waitForCompletion(5000);
				Assert.assertTrue(client.isConnected());
			} finally {
				TestClients.shutdownQuietly(client);
			}
		}
	}

	/**
	 * 902: a host name with an underscore (allowed in DNS names, but not in java.net.URI's
	 * idea of a host) is used as given. Here it resolves through the hosts file only if set up,
	 * so the test checks what the client would connect to.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test
	public void testHostWithUnderscore() throws Exception {
		MqttConnectOptions o = new MqttConnectOptions();
		o.setServerURI("tcp://my_broker.example:1883");
		o.getServerURI();   // parses it
		Assert.assertEquals("my_broker.example", o.getHost());
		Assert.assertEquals(1883, o.getPort());
	}
}
