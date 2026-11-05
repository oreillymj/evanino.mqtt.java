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

import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import org.bouncycastle.openssl.jcajce.JcePEMDecryptorProviderBuilder;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;
import org.bouncycastle.pkcs.PKCSException;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyException;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.Collection;
import java.util.UUID;

/**
 * Builds {@link SSLContext}s from PEM files, such as those made by {@code openssl}, instead of
 * Java keystores.
 * <p>
 * Each file is looked up first as a classpath resource (so files under {@code src/main/resources}
 * work the same from the IDE and from a JAR), then as a file system path. Certificates are read
 * with the JDK's {@link CertificateFactory}; BouncyCastle is used only to read private keys, in
 * any of the forms OpenSSL writes:
 * <ul>
 *   <li>{@code BEGIN PRIVATE KEY}: PKCS#8, what OpenSSL 3 writes by default;</li>
 *   <li>{@code BEGIN ENCRYPTED PRIVATE KEY}: encrypted PKCS#8;</li>
 *   <li>{@code BEGIN RSA PRIVATE KEY} or {@code BEGIN EC PRIVATE KEY}: the traditional
 *       format of OpenSSL 1.x, optionally encrypted.</li>
 * </ul>
 * Errors are thrown rather than logged, so the caller decides what a missing or bad file means.
 * <p>
 * The context only checks that the broker's certificate chains to a trusted CA. Checking that
 * the certificate matches the broker's host name is done by the MQTT client; see
 * {@code MqttConnectOptions.setHttpsHostnameVerificationEnabled(true)}.
 */
public final class SslUtil {

	/** The newest TLS version both sides support. */
	public static final String TLS = "TLS";
	/** TLS 1.2. */
	public static final String TLS_V1_2 = "TLSv1.2";
	/** TLS 1.3 (Java 8u261 or later, or Java 11+). */
	public static final String TLS_V1_3 = "TLSv1.3";

	/**
	 * BouncyCastle's algorithms, used only to decrypt encrypted private keys (OpenSSL's key
	 * derivation is not in the JDK). Passed to BouncyCastle directly rather than installed with
	 * {@code Security.addProvider}, so the rest of the JVM is unaffected.
	 */
	private static final Provider BOUNCY_CASTLE = new BouncyCastleProvider();

	/** Not instantiable. */
	private SslUtil() {
	}

	/**
	 * Creates a context that trusts the CA certificates in {@code caFile} and presents no client
	 * certificate.
	 *
	 * @param caFile   one or more PEM CA certificates (a bundle may hold several)
	 * @param protocol the {@link SSLContext} protocol, for example {@link #TLS_V1_2}
	 * @return the context
	 * @throws IOException              if the file cannot be read
	 * @throws GeneralSecurityException if the file holds no certificate or the protocol is unknown
	 */
	public static SSLContext forServerAuth(String caFile, String protocol) throws IOException, GeneralSecurityException {
		return newContext(protocol, null, trustManagers(caFile));
	}

	/**
	 * Creates a context that trusts the CA certificates in {@code caFile} and presents the client
	 * certificate in {@code certFile} with the private key in {@code keyFile}, for brokers that
	 * require a client certificate ({@code require_certificate true} in Mosquitto).
	 *
	 * @param caFile      one or more PEM CA certificates
	 * @param certFile    the PEM client certificate, optionally followed by intermediate certificates
	 * @param keyFile     the PEM private key of the client certificate
	 * @param keyPassword the password of an encrypted private key, or {@code null} if it is not encrypted
	 * @param protocol    the {@link SSLContext} protocol, for example {@link #TLS_V1_2}
	 * @return the context
	 * @throws IOException              if a file cannot be read
	 * @throws GeneralSecurityException if a file holds no certificate or key, the key cannot be
	 *                                  decrypted, or the protocol is unknown
	 */
	public static SSLContext forMutualAuth(String caFile, String certFile, String keyFile, char[] keyPassword,
										   String protocol) throws IOException, GeneralSecurityException {
		return newContext(protocol, keyManagers(certFile, keyFile, keyPassword), trustManagers(caFile));
	}

	/**
	 * Creates and initialises a context.
	 *
	 * @param protocol      the protocol
	 * @param keyManagers   the client certificate, or {@code null} for none
	 * @param trustManagers the trusted CAs
	 * @return the context
	 * @throws GeneralSecurityException if the protocol is unknown or initialisation fails
	 */
	private static SSLContext newContext(String protocol, KeyManager[] keyManagers, TrustManager[] trustManagers)
			throws GeneralSecurityException {
		SSLContext context = SSLContext.getInstance(protocol);
		context.init(keyManagers, trustManagers, null);
		return context;
	}

	/**
	 * Trusts exactly the CA certificates in a file.
	 *
	 * @param caFile the PEM CA certificates
	 * @return the trust managers
	 * @throws IOException              if the file cannot be read
	 * @throws GeneralSecurityException if it holds no certificate
	 */
	private static TrustManager[] trustManagers(String caFile) throws IOException, GeneralSecurityException {
		KeyStore trustStore = emptyKeyStore();
		int n = 0;
		for (Certificate ca : readCertificates(caFile)) {
			trustStore.setCertificateEntry("ca-" + n++, ca);
		}
		TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		tmf.init(trustStore);
		return tmf.getTrustManagers();
	}

	/**
	 * Presents a client certificate and its private key.
	 *
	 * @param certFile    the PEM certificate chain, client certificate first
	 * @param keyFile     the PEM private key
	 * @param keyPassword the key's password, or {@code null} if it is not encrypted
	 * @return the key managers
	 * @throws IOException              if a file cannot be read
	 * @throws GeneralSecurityException if a file holds no certificate or key
	 */
	private static KeyManager[] keyManagers(String certFile, String keyFile, char[] keyPassword)
			throws IOException, GeneralSecurityException {
		Certificate[] chain = readCertificates(certFile).toArray(new Certificate[0]);
		PrivateKey key = readPrivateKey(keyFile, keyPassword);
		// The key store exists only in memory, so any password will do.
		char[] storePassword = UUID.randomUUID().toString().toCharArray();
		KeyStore keyStore = emptyKeyStore();
		keyStore.setKeyEntry("client", key, storePassword, chain);
		KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
		kmf.init(keyStore, storePassword);
		return kmf.getKeyManagers();
	}

	/**
	 * Creates an empty in-memory key store.
	 *
	 * @return the key store
	 * @throws IOException              never, for an in-memory store
	 * @throws GeneralSecurityException if the default key store type is unavailable
	 */
	private static KeyStore emptyKeyStore() throws IOException, GeneralSecurityException {
		KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
		keyStore.load(null, null);
		return keyStore;
	}

	/**
	 * Reads every PEM (or DER) certificate in a file.
	 *
	 * @param file the file or classpath resource
	 * @return the certificates, at least one
	 * @throws IOException          if the file cannot be read
	 * @throws CertificateException if it holds no valid certificate
	 */
	private static Collection<? extends Certificate> readCertificates(String file) throws IOException, CertificateException {
		Collection<? extends Certificate> certificates;
		try (InputStream in = open(file)) {
			certificates = CertificateFactory.getInstance("X.509").generateCertificates(in);
		}
		if (certificates.isEmpty()) {
			throw new CertificateException("No certificate in " + file);
		}
		return certificates;
	}

	/**
	 * Reads a PEM private key in any of the forms OpenSSL writes.
	 *
	 * @param file     the file or classpath resource
	 * @param password the password of an encrypted key, or {@code null}
	 * @return the key
	 * @throws IOException              if the file cannot be read
	 * @throws GeneralSecurityException if it holds no key, or the key cannot be decrypted
	 */
	private static PrivateKey readPrivateKey(String file, char[] password) throws IOException, GeneralSecurityException {
		Object pem;
		try (PEMParser parser = new PEMParser(new InputStreamReader(open(file), StandardCharsets.US_ASCII))) {
			pem = parser.readObject();
		}
		JcaPEMKeyConverter converter = new JcaPEMKeyConverter();
		try {
			if (pem instanceof PrivateKeyInfo) {                      // BEGIN PRIVATE KEY
				return converter.getPrivateKey((PrivateKeyInfo) pem);
			}
			if (pem instanceof PEMKeyPair) {                          // BEGIN RSA/EC PRIVATE KEY
				return converter.getKeyPair((PEMKeyPair) pem).getPrivate();
			}
			if (pem instanceof PKCS8EncryptedPrivateKeyInfo) {        // BEGIN ENCRYPTED PRIVATE KEY
				PrivateKeyInfo info = ((PKCS8EncryptedPrivateKeyInfo) pem).decryptPrivateKeyInfo(
						new JceOpenSSLPKCS8DecryptorProviderBuilder().setProvider(BOUNCY_CASTLE)
								.build(requirePassword(password, file)));
				return converter.getPrivateKey(info);
			}
			if (pem instanceof PEMEncryptedKeyPair) {                 // BEGIN RSA/EC PRIVATE KEY + Proc-Type: 4,ENCRYPTED
				PEMKeyPair pair = ((PEMEncryptedKeyPair) pem).decryptKeyPair(
						new JcePEMDecryptorProviderBuilder().setProvider(BOUNCY_CASTLE)
								.build(requirePassword(password, file)));
				return converter.getKeyPair(pair).getPrivate();
			}
		} catch (OperatorCreationException | PKCSException e) {
			throw new KeyException("Cannot decrypt the private key in " + file, e);
		}
		throw new KeyException("No private key in " + file
				+ (pem == null ? "" : " (found " + pem.getClass().getSimpleName() + ")"));
	}

	/**
	 * Returns the password, failing if there is none for an encrypted key.
	 *
	 * @param password the password, or {@code null}
	 * @param file     the key file, for the message
	 * @return the password
	 * @throws KeyException if {@code password} is {@code null}
	 */
	private static char[] requirePassword(char[] password, String file) throws KeyException {
		if (password == null) {
			throw new KeyException("The private key in " + file + " is encrypted; a password is needed");
		}
		return password;
	}

	/**
	 * Opens a classpath resource ({@code ssl/ca.crt}, with or without a leading {@code /}), or
	 * failing that a file.
	 *
	 * @param name the resource name or file path
	 * @return the open stream
	 * @throws IOException if it is neither, or cannot be opened
	 */
	private static InputStream open(String name) throws IOException {
		String resource = name.startsWith("/") ? name.substring(1) : name;
		ClassLoader loader = Thread.currentThread().getContextClassLoader();
		InputStream in = loader != null ? loader.getResourceAsStream(resource) : null;
		if (in == null) {
			in = SslUtil.class.getClassLoader().getResourceAsStream(resource);
		}
		if (in != null) {
			return in;
		}
		Path path = Paths.get(name);
		if (Files.isRegularFile(path)) {
			return Files.newInputStream(path);
		}
		throw new FileNotFoundException(name + " is neither a classpath resource nor a file");
	}
}
