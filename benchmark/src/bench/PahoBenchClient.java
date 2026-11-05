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

package bench;

import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.util.function.Consumer;

/**
 * {@link BenchClient} on Eclipse Paho's {@code MqttAsyncClient}. Uses {@link MemoryPersistence}
 * so Paho does not write a file per message, which would measure the disk rather than the client.
 */
final class PahoBenchClient implements BenchClient {

	private static final long TIMEOUT_MILLIS = 30_000;

	private MqttAsyncClient client;

	@Override
	public void connect(Settings s) throws Exception {
		client = new MqttAsyncClient(s.serverURI, s.clientId, new MemoryPersistence());
		MqttConnectOptions options = new MqttConnectOptions();
		options.setCleanSession(s.cleanSession);
		options.setAutomaticReconnect(s.automaticReconnect);
		options.setConnectionTimeout(s.connectionTimeout);
		options.setKeepAliveInterval(s.keepAlive);
		if (s.maxInflight > 0) {
			options.setMaxInflight(s.maxInflight);
		}
		client.setCallback(new MqttCallbackExtended() {
			@Override
			public void connectComplete(boolean reconnect, String serverURI) {
				if (reconnect) {
					s.onReconnect.run();
				}
			}

			@Override
			public void connectionLost(Throwable cause) {
				s.onConnectionLost.run();
			}

			@Override
			public void messageArrived(String topic, MqttMessage message) {
			}

			@Override
			public void deliveryComplete(IMqttDeliveryToken token) {
			}
		});
		client.connect(options).waitForCompletion(TIMEOUT_MILLIS);
	}

	@Override
	public void subscribe(String topic, int qos, MessageHandler handler) throws Exception {
		client.subscribe(topic, qos, (t, m) -> handler.onMessage(t, m.getPayload())).waitForCompletion(TIMEOUT_MILLIS);
	}

	@Override
	public boolean publish(String topic, byte[] payload, int qos, Runnable onComplete, Consumer<Throwable> onFailure) throws Exception {
		try {
			client.publish(topic, payload, qos, false, null, new IMqttActionListener() {
				@Override
				public void onSuccess(IMqttToken token) {
					onComplete.run();
				}

				@Override
				public void onFailure(IMqttToken token, Throwable exception) {
					onFailure.accept(exception);
				}
			});
			return true;
		} catch (MqttException e) {
			if (e.getReasonCode() == MqttException.REASON_CODE_MAX_INFLIGHT) {
				return false;
			}
			throw e;
		}
	}

	@Override
	public boolean isConnected() {
		return client != null && client.isConnected();
	}

	@Override
	public void close() throws Exception {
		if (client == null) {
			return;
		}
		try {
			if (client.isConnected()) {
				client.disconnect(0).waitForCompletion(TIMEOUT_MILLIS);
			}
		} finally {
			client.close(true);
		}
	}
}
