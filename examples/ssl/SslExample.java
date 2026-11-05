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

package org.example;

import io.github.oreillymj.evanino.mqtt.MqttAsyncClient;
import io.github.oreillymj.evanino.mqtt.MqttConnectOptions;
import io.github.oreillymj.evanino.mqtt.MqttException;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.security.GeneralSecurityException;

/**
 * Connects to a broker over TLS with PEM files, verifying the broker's host name.
 * <pre>
 * java org.example.SslExample ssl://localhost:8883 ssl/ca.crt
 * java org.example.SslExample ssl://localhost:8883 ssl/ca.crt ssl/client.crt ssl/client.key [key-password]
 * </pre>
 * See UsingSSL.md.
 */
public final class SslExample {

	/** Not instantiable. */
	private SslExample() {
	}

	/**
	 * Builds the TLS context for a broker: trust only, or with a client certificate.
	 *
	 * @param caFile      the CA certificate(s) that signed the broker's certificate
	 * @param certFile    the client certificate, or {@code null} for none
	 * @param keyFile     the client private key, or {@code null} for none
	 * @param keyPassword the private key password, or {@code null} if the key is not encrypted
	 * @return the context
	 * @throws IOException              if a file cannot be read
	 * @throws GeneralSecurityException if a file is not a valid certificate or key
	 */
	static SSLContext sslContext(String caFile, String certFile, String keyFile, char[] keyPassword)
			throws IOException, GeneralSecurityException {
		return certFile == null
				? SslUtil.forServerAuth(caFile, SslUtil.TLS_V1_2)
				: SslUtil.forMutualAuth(caFile, certFile, keyFile, keyPassword, SslUtil.TLS_V1_2);
	}

	/**
	 * Connects, prints the outcome and disconnects.
	 *
	 * @param args the broker URL, the CA file, and optionally the client certificate, key and key password
	 * @throws Exception if the TLS files cannot be loaded
	 */
	public static void main(String[] args) throws Exception {
		String serverURI = args[0];
		SSLContext context = sslContext(args[1],
				args.length > 2 ? args[2] : null,
				args.length > 3 ? args[3] : null,
				args.length > 4 ? args[4].toCharArray() : null);

		MqttConnectOptions options = new MqttConnectOptions();
		options.setServerURI(serverURI);
		options.setSslContext(context);
		options.setHttpsHostnameVerificationEnabled(true);   // check the certificate matches the host

		MqttAsyncClient client = new MqttAsyncClient(options);
		try {
			client.connect().waitForCompletion(10_000);
			System.out.println("CONNECTED " + serverURI);
			client.disconnect().waitForCompletion(10_000);
		} catch (MqttException e) {
			Throwable cause = e;
			while (cause.getCause() != null) {
				cause = cause.getCause();
			}
			System.out.println("FAILED " + serverURI + ": " + e.getReasonCode() + " " + cause);
		} finally {
			client.close(true);
		}
	}
}
