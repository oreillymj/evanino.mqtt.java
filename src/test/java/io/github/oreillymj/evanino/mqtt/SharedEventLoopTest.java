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
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultithreadEventLoopGroup;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.ClassRule;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Clients share one event loop group by default ({@link SharedEventLoop}) instead of each
 * creating a group of twice the number of processors: the group is reference-counted, shutting
 * one client down leaves the others running, the last release shuts the group down, and a group
 * given to {@code setEventLoop} is used and never shut down by the client. Other test classes
 * may leave clients open in the same JVM, so reference counts are checked relative to their
 * starting value. Requires a broker; see {@link MosquittoBroker}.
 */
public class SharedEventLoopTest {

	/** The broker used by every test in this class. */
	@ClassRule
	public static final MosquittoBroker broker = new MosquittoBroker();

	/**
	 * Clients created without a group get the same shared one, and each holds a reference.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 10_000)
	public void testClientsShareOneGroup() throws Exception {
		int before = SharedEventLoop.referenceCount();
		MqttAsyncClient a = newClient();
		MqttAsyncClient b = newClient();
		try {
			Assert.assertSame(a.getEventLoop(), b.getEventLoop());
			Assert.assertTrue(SharedEventLoop.isShared(a.getEventLoop()));
			Assert.assertEquals(before + 2, SharedEventLoop.referenceCount());
		} finally {
			TestClients.shutdownQuietly(a);
			TestClients.shutdownQuietly(b);
		}
		Assert.assertEquals(before, SharedEventLoop.referenceCount());
	}

	/**
	 * More clients than the group has event loops all connect on the shared group, using no more
	 * I/O threads than it has loops. Before, each client started an I/O thread of its own.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 60_000)
	public void testManyClientsShareFewThreads() throws Exception {
		List<MqttAsyncClient> clients = new ArrayList<>();
		try {
			EventLoopGroup shared = null;
			int loops = 0;
			Set<EventLoop> used = new HashSet<>();
			MqttAsyncClient first = newClient();
			clients.add(first);
			shared = first.getEventLoop();
			loops = ((MultithreadEventLoopGroup) shared).executorCount();
			for (int i = 1; i < loops + 8; i++) {
				clients.add(newClient());
			}
			for (MqttAsyncClient client : clients) {
				client.connect().waitForCompletion(5000);
				Channel channel = channel(client);
				Assert.assertSame("connected on the shared group", shared, channel.eventLoop().parent());
				used.add(channel.eventLoop());
			}
			Assert.assertTrue("loops used: " + used.size(), used.size() <= loops);
			long ioThreads = Thread.getAllStackTraces().keySet().stream()
					.filter(t -> t.isAlive() && t.getName().startsWith(SharedEventLoop.THREAD_PREFIX))
					.count();
			Assert.assertTrue(clients.size() + " clients use " + ioThreads + " I/O threads", ioThreads <= loops);
		} finally {
			for (MqttAsyncClient client : clients) {
				TestClients.shutdownQuietly(client);
			}
		}
	}

	/**
	 * Shutting one client down leaves another on the shared group connected and working.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 20_000)
	public void testShutdownLeavesOthersRunning() throws Exception {
		MqttAsyncClient a = newClient();
		MqttAsyncClient b = newClient();
		try {
			a.connect().waitForCompletion(5000);
			b.connect().waitForCompletion(5000);
			EventLoopGroup shared = b.getEventLoop();
			a.close();
			Assert.assertFalse(shared.isShuttingDown());
			Assert.assertTrue(b.isConnected());
			TestClients.publish(b, "SharedEventLoopTest/" + UUID.randomUUID(), new byte[]{1}, 1, false)
					.waitForCompletion(5000);
		} finally {
			TestClients.shutdownQuietly(a);
			TestClients.shutdownQuietly(b);
		}
	}

	/**
	 * {@code disconnect(..., shutdowneventloop = true)} releases the client's reference without
	 * stopping the group for the others.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 20_000)
	public void testDisconnectReleasingLoopLeavesOthersRunning() throws Exception {
		MqttAsyncClient a = newClient();
		MqttAsyncClient b = newClient();
		try {
			a.connect().waitForCompletion(5000);
			b.connect().waitForCompletion(5000);
			int before = SharedEventLoop.referenceCount();
			a.disconnect(false, false, true);
			Assert.assertNull(a.getEventLoop());
			Assert.assertEquals(before - 1, SharedEventLoop.referenceCount());
			Assert.assertTrue(b.isConnected());
			TestClients.publish(b, "SharedEventLoopTest/" + UUID.randomUUID(), new byte[]{1}, 1, false)
					.waitForCompletion(5000);

			a.connect().waitForCompletion(5000);   // takes a reference again
			Assert.assertSame(b.getEventLoop(), a.getEventLoop());
			Assert.assertEquals(before, SharedEventLoop.referenceCount());
		} finally {
			TestClients.shutdownQuietly(a);
			TestClients.shutdownQuietly(b);
		}
	}

	/**
	 * When the last client lets go, the shared group is shut down, and the next client gets a
	 * new, working group. Skipped if other clients in this JVM still hold references.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 20_000)
	public void testLastReleaseShutsGroupDown() throws Exception {
		Assume.assumeTrue("other clients hold the shared group", SharedEventLoop.referenceCount() == 0);
		MqttAsyncClient a = newClient();
		a.connect().waitForCompletion(5000);
		EventLoopGroup old = a.getEventLoop();
		a.close();
		Assert.assertEquals(0, SharedEventLoop.referenceCount());
		Assert.assertTrue(old.isShuttingDown());
		Assert.assertTrue(old.awaitTermination(5, TimeUnit.SECONDS));

		MqttAsyncClient b = newClient();
		try {
			Assert.assertNotSame(old, b.getEventLoop());
			b.connect().waitForCompletion(5000);
			Assert.assertTrue(b.isConnected());
		} finally {
			TestClients.shutdownQuietly(b);
		}
	}

	/**
	 * A group given to {@code setEventLoop} is used for the connection, the shared reference is
	 * released, and shutting the client down leaves the caller's group running for other
	 * clients. Before, the client shut the caller's group down.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 20_000)
	@SuppressWarnings("deprecation") // NioEventLoopGroup, which also runs on Netty 4.1
	public void testCallerGroupIsUsedAndNotShutDown() throws Exception {
		EventLoopGroup own = new io.netty.channel.nio.NioEventLoopGroup(1);
		try {
			int before = SharedEventLoop.referenceCount();
			MqttAsyncClient a = newClient();
			a.setEventLoop(own);
			Assert.assertEquals(before, SharedEventLoop.referenceCount());
			a.connect().waitForCompletion(5000);
			Assert.assertSame(own, channel(a).eventLoop().parent());
			a.close();
			Assert.assertFalse("the client shut the caller's group down", own.isShuttingDown());

			MqttAsyncClient b = newClient();
			b.setEventLoop(own);
			try {
				b.connect().waitForCompletion(5000);
				Assert.assertTrue(b.isConnected());
			} finally {
				TestClients.shutdownQuietly(b);
			}
			Assert.assertFalse(own.isShuttingDown());
			Assert.assertEquals(before, SharedEventLoop.referenceCount());
		} finally {
			own.shutdownGracefully(0, 1, TimeUnit.SECONDS).sync();
		}
	}

	/**
	 * {@code setEventLoop(null)} sends the next connect back to the shared group.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 20_000)
	public void testNullGoesBackToSharedGroup() throws Exception {
		int before = SharedEventLoop.referenceCount();
		MqttAsyncClient a = newClient();
		try {
			a.setEventLoop(null);
			Assert.assertEquals(before, SharedEventLoop.referenceCount());
			a.connect().waitForCompletion(5000);
			Assert.assertTrue(SharedEventLoop.isShared(a.getEventLoop()));
			Assert.assertEquals(before + 1, SharedEventLoop.referenceCount());
		} finally {
			TestClients.shutdownQuietly(a);
		}
		Assert.assertEquals(before, SharedEventLoop.referenceCount());
	}

	/**
	 * Creates an unconnected client for the broker.
	 *
	 * @return the client
	 * @throws Exception if the client cannot be created
	 */
	private static MqttAsyncClient newClient() throws Exception {
		return new MqttAsyncClient(TestClients.options(broker.getServerURI(), "SharedEventLoopTest-" + UUID.randomUUID()));
	}

	/**
	 * Returns a client's current channel.
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
}
