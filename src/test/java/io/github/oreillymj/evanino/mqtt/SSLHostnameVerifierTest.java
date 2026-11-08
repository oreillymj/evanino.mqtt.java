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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.util.ReferenceCountUtil;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import java.io.File;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests {@link MqttConnectOptions#setSSLHostnameVerifier(HostnameVerifier)}, Paho's custom
 * hostname check: the verifier is called with the host and the {@code SSLSession} after the TLS
 * handshake, a {@code false} result or an exception fails the connect with an
 * {@link SSLPeerUnverifiedException} before CONNECT is sent, and, as in Paho, the verifier is not
 * used while HTTPS hostname verification is on. Requires a broker with TLS listeners; see
 * {@link MosquittoBroker}. The test server certificate is valid for {@code localhost} only.
 */
public class SSLHostnameVerifierTest {

	/** The broker used by every test in this class. */
	@ClassRule
	public static final MosquittoBroker broker = new MosquittoBroker();

	private static final String serverHost = "localhost";
	private static final String serverIP = "127.0.0.1";
	private static int serverPort;
	private static int websocketPort;

	/**
	 * Picks up the TLS and secure WebSocket ports.
	 */
	@BeforeClass
	public static void setUpBeforeClass() {
		serverPort = broker.getSslServerURI().getPort();
		websocketPort = broker.getSecureWebSocketServerURI().getPort();
	}

	/**
	 * There is no verifier by default, and the setter and getter round-trip, including
	 * {@code null}.
	 */
	@Test
	public void testGetterAndSetter() {
		MqttConnectOptions options = new MqttConnectOptions();
		Assert.assertNull(options.getSSLHostnameVerifier());
		HostnameVerifier verifier = (host, session) -> true;
		options.setSSLHostnameVerifier(verifier);
		Assert.assertSame(verifier, options.getSSLHostnameVerifier());
		options.setSSLHostnameVerifier(null);
		Assert.assertNull(options.getSSLHostnameVerifier());
	}

	/**
	 * An accepting verifier is called once, with the host from the URI and the session of the
	 * completed handshake, and the client connects.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 10_000)
	public void testAcceptingVerifierIsCalledWithHostAndSession() throws Exception {
		List<String> hosts = new ArrayList<>();
		List<String> subjects = new ArrayList<>();
		MqttConnectOptions options = options(new URI("mqtts://" + serverIP + ":" + serverPort));
		options.setSSLHostnameVerifier((host, session) -> {
			hosts.add(host);
			try {
				subjects.add(((X509Certificate) session.getPeerCertificates()[0]).getSubjectX500Principal().getName());
			} catch (SSLPeerUnverifiedException e) {
				subjects.add(null);
			}
			return true;
		});
		assertConnects(options);
		Assert.assertEquals(1, hosts.size());
		Assert.assertEquals(serverIP, hosts.get(0));
		Assert.assertNotNull("the session should hold the broker's certificate", subjects.get(0));
	}

	/**
	 * A verifier can accept a host that the certificate does not name: here {@code 127.0.0.1}
	 * is accepted because the certificate names {@code localhost}, which the HTTPS check alone
	 * would reject.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 10_000)
	public void testVerifierCanAcceptAlias() throws Exception {
		MqttConnectOptions options = options(new URI("mqtts://" + serverIP + ":" + serverPort));
		options.setSSLHostnameVerifier((host, session) -> serverIP.equals(host) && namesHost(session, serverHost));
		assertConnects(options);
	}

	/**
	 * A rejecting verifier fails the connect with an {@link SSLPeerUnverifiedException} naming
	 * the host, even though the certificate is valid for it.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 10_000)
	public void testRejectingVerifierFailsConnect() throws Exception {
		MqttConnectOptions options = options(new URI("mqtts://" + serverHost + ":" + serverPort));
		options.setSSLHostnameVerifier((host, session) -> false);
		SSLPeerUnverifiedException e = assertConnectFails(options);
		Assert.assertTrue(e.getMessage(), e.getMessage().startsWith("Host: " + serverHost + ", Peer Host: "));
	}

	/**
	 * A rejecting verifier fails a secure WebSocket connect too.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 10_000)
	public void testRejectingVerifierFailsWebSocketConnect() throws Exception {
		MqttConnectOptions options = options(new URI("wss://" + serverHost + ":" + websocketPort + "/ws"));
		options.setSSLHostnameVerifier((host, session) -> false);
		assertConnectFails(options);
	}

	/**
	 * A verifier that throws fails the connect, with its exception as the cause of the
	 * {@link SSLPeerUnverifiedException}.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 10_000)
	public void testThrowingVerifierFailsConnect() throws Exception {
		MqttConnectOptions options = options(new URI("mqtts://" + serverHost + ":" + serverPort));
		IllegalStateException thrown = new IllegalStateException("verifier failed");
		options.setSSLHostnameVerifier((host, session) -> {
			throw thrown;
		});
		SSLPeerUnverifiedException e = assertConnectFails(options);
		Assert.assertSame(thrown, e.getCause());
	}

	/**
	 * As in Paho, the verifier is not used while HTTPS hostname verification is on: a verifier
	 * that rejects everything is not called, and the connection to {@code localhost} succeeds.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 10_000)
	public void testVerifierIgnoredWithHttpsVerification() throws Exception {
		AtomicInteger calls = new AtomicInteger();
		MqttConnectOptions options = options(new URI("mqtts://" + serverHost + ":" + serverPort));
		options.setHttpsHostnameVerificationEnabled(true);
		options.setSSLHostnameVerifier((host, session) -> {
			calls.incrementAndGet();
			return false;
		});
		assertConnects(options);
		Assert.assertEquals(0, calls.get());
	}

	/**
	 * After a rejection the client sends nothing over the TLS connection: no CONNECT over TLS
	 * and no upgrade request over secure WebSockets. Uses a local TLS server that counts the
	 * decrypted bytes it receives.
	 *
	 * @throws Exception if the test fails unexpectedly
	 */
	@Test(timeout = 20_000)
	@SuppressWarnings("deprecation") // NioEventLoopGroup, which also runs on Netty 4.1
	public void testNothingSentAfterRejection()throws Exception {
		File cert = TestTls.file("server.crt");
		File key = TestTls.file("server.key");
		Assume.assumeTrue("test TLS certificates not on the classpath", cert != null && key != null);
		SslContext serverContext = SslContextBuilder.forServer(cert, key).build();

		AtomicInteger received = new AtomicInteger();
		CountDownLatch handshakes = new CountDownLatch(2);
		EventLoopGroup group = new io.netty.channel.nio.NioEventLoopGroup(1);
		try {
			Channel server = new ServerBootstrap()
					.group(group)
					.channel(NioServerSocketChannel.class)
					.childHandler(new ChannelInitializer<SocketChannel>() {
						@Override
						protected void initChannel(SocketChannel ch) {
							ch.pipeline().addLast(serverContext.newHandler(ch.alloc()));
							ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
								@Override
								public void channelRead(ChannelHandlerContext ctx, Object msg) {
									received.addAndGet(((ByteBuf) msg).readableBytes());
									ReferenceCountUtil.release(msg);
								}

								@Override
								public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
									if (evt instanceof SslHandshakeCompletionEvent
											&& ((SslHandshakeCompletionEvent) evt).isSuccess()) {
										handshakes.countDown();
									}
									ctx.fireUserEventTriggered(evt);
								}
							});
						}
					})
					.bind(new InetSocketAddress(serverIP, 0)).sync().channel();
			int port = ((InetSocketAddress) server.localAddress()).getPort();

			for (String uri : new String[]{"mqtts://" + serverHost + ":" + port, "wss://" + serverHost + ":" + port + "/ws"}) {
				MqttConnectOptions options = options(new URI(uri));
				options.setSSLHostnameVerifier((host, session) -> false);
				assertConnectFails(options);
			}
			Assert.assertTrue("both handshakes should complete", handshakes.await(5, TimeUnit.SECONDS));
			Thread.sleep(500);   // let anything still in flight arrive
			Assert.assertEquals("bytes received after a rejected handshake", 0, received.get());
			server.close().sync();
		} finally {
			group.shutdownGracefully(0, 1, TimeUnit.SECONDS).sync();
		}
	}

	/**
	 * Returns whether a session's peer certificate names a host as a DNS subject alternative name.
	 *
	 * @param session the TLS session
	 * @param host    the host name
	 * @return {@code true} if the certificate names the host
	 */
	private static boolean namesHost(SSLSession session, String host) {
		try {
			X509Certificate cert = (X509Certificate) session.getPeerCertificates()[0];
			if (cert.getSubjectAlternativeNames() == null) {
				return false;
			}
			for (List<?> name : cert.getSubjectAlternativeNames()) {
				if (Integer.valueOf(2).equals(name.get(0)) && host.equalsIgnoreCase((String) name.get(1))) {
					return true;
				}
			}
			return false;
		} catch (Exception e) {
			return false;
		}
	}

	/**
	 * Creates TLS options for a broker URI.
	 *
	 * @param serverURI the broker URI
	 * @return the options
	 * @throws Exception if the TLS context cannot be created
	 */
	private static MqttConnectOptions options(URI serverURI) throws Exception {
		MqttConnectOptions options = TestClients.options(serverURI, "SSLHostnameVerifierTest-" + UUID.randomUUID());
		options.setSslContext(TestTls.clientContext());
		return options;
	}

	/**
	 * Connects with the given options, checks the client is connected, then disconnects.
	 *
	 * @param options the connection options
	 * @throws Exception if the connection fails
	 */
	private static void assertConnects(MqttConnectOptions options) throws Exception {
		MqttClient mqttClient = new MqttClient(options);
		mqttClient.setTimeToWait(5000);
		try {
			mqttClient.connect(options);
			Assert.assertTrue(mqttClient.isConnected());
			mqttClient.disconnect();
		} finally {
			TestClients.closeQuietly(mqttClient);
		}
	}

	/**
	 * Connects with the given options and checks that the connect fails with an
	 * {@link SSLPeerUnverifiedException} in its cause chain.
	 *
	 * @param options the connection options
	 * @return the {@code SSLPeerUnverifiedException}
	 * @throws Exception if the test fails unexpectedly
	 */
	private static SSLPeerUnverifiedException assertConnectFails(MqttConnectOptions options) throws Exception {
		MqttClient mqttClient = new MqttClient(options);
		mqttClient.setTimeToWait(5000);
		try {
			mqttClient.connect(options);
			Assert.fail("The hostname verifier should have rejected the connection");
			return null;
		} catch (MqttException ex) {
			Assert.assertFalse(mqttClient.isConnected());
			for (Throwable t = ex; t != null && t.getCause() != t; t = t.getCause()) {
				if (t instanceof SSLPeerUnverifiedException) {
					return (SSLPeerUnverifiedException) t;
				}
			}
			throw new AssertionError("Expected an SSLPeerUnverifiedException in the cause chain of " + ex, ex);
		} finally {
			TestClients.closeQuietly(mqttClient);
		}
	}
}
