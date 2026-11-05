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

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * Callbacks still run after {@code close()}. Closing shut the callback executor down with
 * {@code shutdownNow()}: it interrupted the callback running at that moment and dropped those
 * queued behind it, and callbacks arriving later, such as the failures of tokens that close
 * itself failed, were rejected with a {@code RejectedExecutionException} warning from Netty and
 * never called. Not part of Paho's suite; the delivery test needs a broker, see
 * {@link MosquittoBroker}.
 */
public class CloseCallbacksTest {

	/** The broker used by the delivery test. */
	@ClassRule
	public static final MosquittoBroker broker = new MosquittoBroker();

	/** Publishes made before closing. */
	private static final int PUBLISHES = 100;

	/**
	 * Messages already handed to the callback thread when {@code close(true)} is called are all
	 * delivered, and the callback that is running is not interrupted.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testQueuedCallbacksRunAfterClose() throws Exception {
		String topic = "CloseCallbacksTest/" + UUID.randomUUID();
		MqttAsyncClient client = TestClients.createMqttAsyncClient(broker.getServerURI(), "CloseCallbacksTest-queued");
		CountDownLatch first = new CountDownLatch(1);
		CountDownLatch all = new CountDownLatch(5);
		AtomicInteger interrupted = new AtomicInteger();
		client.setCallback(new MqttCallbackAdapter() {
			@Override
			public void messageArrived(String t, MqttMessage message) {
				if (first.getCount() == 1) {
					first.countDown();
					try {
						Thread.sleep(1000);   // the other messages queue up behind this one
					} catch (InterruptedException e) {
						interrupted.incrementAndGet();
					}
				}
				all.countDown();
			}
		});
		client.connect().waitForCompletion(5000);
		client.subscribe(topic, 1).waitForCompletion(5000);
		for (int i = 0; i < 5; i++) {
			client.publish(topic, new byte[]{(byte) i}, 1, false).waitForCompletion(5000);
		}
		Assert.assertTrue(first.await(5, TimeUnit.SECONDS));
		Thread.sleep(200);   // the remaining deliveries are queued
		client.close(true);

		Assert.assertTrue("queued messages not delivered: " + all.getCount() + " missing", all.await(5, TimeUnit.SECONDS));
		Assert.assertEquals("the running callback was interrupted", 0, interrupted.get());
	}

	/**
	 * Every listener of a publish failed by {@code close(true)} is called once, with
	 * {@link MqttException#REASON_CODE_CLIENT_CLOSED}, and Netty logs no rejected callbacks.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 30_000)
	public void testListenersOfPublishesFailedByCloseAreCalled() throws Exception {
		List<LogRecord> rejected = new CopyOnWriteArrayList<>();
		Handler capture = new Handler() {
			@Override
			public void publish(LogRecord record) {
				for (Throwable t = record.getThrown(); t != null; t = t.getCause()) {
					if (t instanceof java.util.concurrent.RejectedExecutionException) {
						rejected.add(record);
						return;
					}
				}
			}

			@Override
			public void flush() {
			}

			@Override
			public void close() {
			}
		};
		Logger netty = Logger.getLogger("io.netty");
		netty.addHandler(capture);
		try {
			MqttAsyncClient client = new MqttAsyncClient("tcp://127.0.0.1:1", "CloseCallbacksTest");
			AtomicInteger calls = new AtomicInteger();
			AtomicInteger closedFailures = new AtomicInteger();
			CountDownLatch all = new CountDownLatch(PUBLISHES);
			for (int i = 0; i < PUBLISHES; i++) {
				// never connected, so each publish is held
				client.publish("CloseCallbacksTest", new byte[]{1}, 1, false, null, new IMqttActionListener() {
					@Override
					public void onSuccess(IMqttToken token) {
						calls.incrementAndGet();
						all.countDown();
					}

					@Override
					public void onFailure(IMqttToken token, Throwable exception) {
						calls.incrementAndGet();
						if (exception instanceof MqttException
								&& ((MqttException) exception).getReasonCode() == MqttException.REASON_CODE_CLIENT_CLOSED) {
							closedFailures.incrementAndGet();
						}
						all.countDown();
					}
				});
			}
			client.close(true);

			Assert.assertTrue("listeners not called: " + all.getCount() + " of " + PUBLISHES, all.await(10, TimeUnit.SECONDS));
			Thread.sleep(200);
			Assert.assertEquals("each listener should be called once", PUBLISHES, calls.get());
			Assert.assertEquals(PUBLISHES, closedFailures.get());
			Assert.assertTrue("Netty logged rejected callbacks: " + rejected.size(), rejected.isEmpty());
		} finally {
			netty.removeHandler(capture);
		}
	}
}
