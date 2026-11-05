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

import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.netty.util.NettyRuntime;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.UUID;

/**
 * The performance options that are off by default: flush consolidation
 * ({@link MqttConnectOptions#enableFlushConsolidation(boolean)}) and the sizing of the shared
 * event loop group ({@link MqttAsyncClient#enableSharedGroupSizing(boolean)}). Not part of
 * Paho's suite. Requires a broker; see {@link MosquittoBroker}.
 */
public class OptionalTuningTest {

	/** The broker used by every test in this class. */
	@ClassRule
	public static final MosquittoBroker broker = new MosquittoBroker();

	/**
	 * Both options are off by default.
	 */
	@Test
	public void testOffByDefault() {
		Assert.assertFalse(new MqttConnectOptions().isFlushConsolidationEnabled());
		Assert.assertFalse(MqttAsyncClient.isSharedGroupSizingEnabled());
	}

	/**
	 * A connection has a {@link FlushConsolidationHandler} only when flush consolidation is
	 * enabled, and messages still go both ways with it.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testFlushConsolidationHandlerOnlyWhenEnabled() throws Exception {
		for (boolean enabled : new boolean[]{false, true}) {
			MqttConnectOptions options = TestClients.options(broker.getServerURI(), "OptionalTuningTest-" + enabled);
			options.enableFlushConsolidation(enabled);
			MqttAsyncClient client = new MqttAsyncClient(options);
			try {
				client.connect().waitForCompletion(5000);
				Assert.assertEquals("handler present with flush consolidation " + enabled, enabled,
						client.getChannel().pipeline().get(FlushConsolidationHandler.class) != null);

				String topic = "OptionalTuningTest/" + UUID.randomUUID();
				MqttV3Receiver receiver = new MqttV3Receiver();
				client.setCallback(receiver);
				client.subscribe(topic, 2).waitForCompletion(5000);
				for (int qos = 0; qos <= 2; qos++) {
					client.publish(topic, new byte[]{(byte) qos}, qos, false).waitForCompletion(5000);
				}
				long deadline = System.currentTimeMillis() + 10_000;
				while (receiver.receivedMessageCount() < 3 && System.currentTimeMillis() < deadline) {
					Thread.sleep(10);
				}
				Assert.assertEquals("messages received with flush consolidation " + enabled,
						3, receiver.receivedMessageCount());
			} finally {
				TestClients.shutdownQuietly(client);
			}
		}
	}

	/**
	 * With sizing enabled, the shared group is created with one thread per processor. Only
	 * checked when no other client holds the shared group, as its size is fixed when created.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testSharedGroupSizedToProcessors() throws Exception {
		Assume.assumeTrue("the shared group is in use by other clients", SharedEventLoop.referenceCount() == 0);
		Assume.assumeTrue("io.netty.eventLoopThreads is set", System.getProperty("io.netty.eventLoopThreads") == null);
		MqttAsyncClient.enableSharedGroupSizing(true);
		try {
			MqttAsyncClient client = new MqttAsyncClient(TestClients.options(broker.getServerURI(), "OptionalTuningTest-sized"));
			try {
				Assert.assertEquals(NettyRuntime.availableProcessors(),
						((NioEventLoopGroup) client.getEventLoop()).executorCount());
			} finally {
				TestClients.shutdownQuietly(client);
			}
		} finally {
			MqttAsyncClient.enableSharedGroupSizing(false);
		}
	}
}
