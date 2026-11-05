/*******************************************************************************
 * Copyright (c) 2017, 2026 Michael OReilly
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

import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.proxy.HttpProxyHandler;
import io.netty.handler.proxy.ProxyHandler;

import java.net.*;
import java.util.List;

/**
 * {@link ProxyHandlerFactory} that creates HTTP {@code CONNECT} proxy handlers, optionally
 * authenticating with a username and password.
 */
public class HttpProxyHandlerFactory implements ProxyHandlerFactory {

    private final String TAG = "HttpProxyHandlerFactory()";
    private boolean enableLogging=false;

    private SimpleLogger logger = new SimpleLogger();

    private final SocketAddress proxyAddress;

    private  String username;
    private  String password;

    private static final String PROXY_USERNAME_PROPERTY_KEY = "http.proxyUser";
    private static final String PROXY_PASSWORD_PROPERTY_KEY = "http.proxyPassword";

    private HttpHeaders headers=null;
    private boolean ignoreDefaultPortsInConnectHostHeader = false;

    // https://github.com/eclipse/paho.mqtt.java/issues/575#issuecomment-1827505526



    /**
     * Constructs an {@code HttpProxyHandlerFactory} that uses the HTTP proxy (if any) specified in Java's standard
     * <a href="https://docs.oracle.com/javase/7/docs/api/java/net/doc-files/net-properties.html#Proxies">proxy system
     * properties</a>.
     *
     * Proxy credentials are read from the {@code http.proxyUser} and {@code http.proxyPassword}
     * system properties.
     *
     * @param apnsHost the broker host for which to find proxy settings
     * @return an HTTP proxy factory if a proxy is configured for the given host, or {@code null} if no proxy is
     * configured for the given host
     * @throws URISyntaxException if {@code apnsHost} is malformed in some way
     *
     * @see <a href="https://docs.oracle.com/javase/7/docs/api/java/net/doc-files/net-properties.html#Proxies">Java™ Platform, Standard Edition 7 - Networking Properties - Proxies</a>
     */
    public static HttpProxyHandlerFactory fromSystemProxies(final String apnsHost) throws URISyntaxException {

        final SocketAddress proxyAddress = getProxyAddressForUri(new URI("https", apnsHost, null, null));

        return (proxyAddress != null)
                ? new HttpProxyHandlerFactory(proxyAddress, System.getProperty(PROXY_USERNAME_PROPERTY_KEY), System.getProperty(PROXY_PASSWORD_PROPERTY_KEY))
                : null;
    }

    /**
     * Returns the {@link SocketAddress} for any system-wide HTTP {@link Proxy} for a given {@link URI}.
     *
     * @param uri the URI for which to find a system-wide proxy
     * @return the {@link SocketAddress} of the first HTTP {@link Proxy} found, or {@code null} if there were none
     */
    private static SocketAddress getProxyAddressForUri(final URI uri) {
        final ProxySelector defaultProxySelector = ProxySelector.getDefault();
        final List<Proxy> proxiesForUri = defaultProxySelector.select(uri);

        for (final java.net.Proxy proxy : proxiesForUri) {
            if (proxy.type() == Proxy.Type.HTTP) {
                InetSocketAddress proxyAddress = (InetSocketAddress) proxy.address();

                if (proxyAddress.isUnresolved()) {
                    proxyAddress = new InetSocketAddress(proxyAddress.getHostString(), proxyAddress.getPort());
                }

                return proxyAddress;
            }
        }

        return null;
    }

    /**
     * Creates a new proxy handler factory that will create HTTP proxy handlers that use the proxy at the given
     * address and that will not perform authentication.
     *
     * @param proxyAddress the address of the HTTP proxy server
     */
    public HttpProxyHandlerFactory(final SocketAddress proxyAddress) {
        this(proxyAddress, null, null);
    }

    /**
     * Creates a new proxy handler factory that will create HTTP proxy handlers that use the proxy at the given
     * address and that will authenticate with the given username and password.
     *
     * @param proxyAddress the address of the HTTP proxy server
     * @param username the username to use when connecting to the given proxy server
     * @param password the password to use when connecting to the given proxy server
     */
    public HttpProxyHandlerFactory(final SocketAddress proxyAddress, final String username, final String password) {
        this.proxyAddress = proxyAddress;

        this.username = username;
        this.password = password;
    }

    /**
     * Logs a message if logging is enabled for this factory.
     *
     * @param data the message to log
     */
    private void log(String data){
        if (enableLogging){
            logger.log(data);
        }
    }

    /**
     * Sets extra headers for the {@code CONNECT} request. Not currently applied by
     * {@link #createProxyHandler()}.
     *
     * @param headers the headers to send
     */
    public void setHttpHeaders(HttpHeaders headers){
        this.headers=headers;
    }

    /**
     * Sets whether default ports are left out of the {@code Host} header of the
     * {@code CONNECT} request. Not currently applied by {@link #createProxyHandler()}.
     *
     * @param defaultports {@code true} to omit default ports
     */
    public void setDefaultPortsOption(boolean defaultports){
        this.ignoreDefaultPortsInConnectHostHeader =defaultports;
    }

    /**
     * Creates a HTTP proxy handler, authenticating only if both a username and a password
     * are set.
     * If the proxy cannot be reached, the connection attempt fails; it is never made without
     * the proxy.
     *
     * @return a new proxy handler, or {@code null} if no proxy address is set
     */
    @Override
    public ProxyHandler createProxyHandler() {
        final HttpProxyHandler handler;

        if (BuildConfig.LOGGING && enableLogging) log("HttpProxyHandlerFactory()->createProxyHandler() - proxyAddress=" + this.proxyAddress);
        if (this.proxyAddress==null){
            return null;
        }


        InetSocketAddress proxyAddress = (InetSocketAddress) this.proxyAddress;

        if (proxyAddress.isUnresolved()) {
            proxyAddress = new InetSocketAddress(proxyAddress.getHostString(), proxyAddress.getPort());
        }

        if (BuildConfig.LOGGING && enableLogging) log("HttpProxyHandlerFactory()->createProxyHandler() - proxyAddress=" + proxyAddress);





        if (BuildConfig.LOGGING && enableLogging) log("HttpProxyHandlerFactory()->createProxyHandler() got here");
        // https://netty.io/4.1/api/io/netty/handler/proxy/HttpProxyHandler.html

        // For reasons that are not immediately clear, HttpProxyHandler doesn't allow null usernames/passwords if
        // specified. If we want them to be null, we have to use the constructor that doesn't take a username/password
        // at all.
        if (this.username != null && this.password != null) {
            handler = new HttpProxyHandler(proxyAddress, this.username, this.password);
        } else {
            handler = new HttpProxyHandler(proxyAddress);
        }

        return handler;
    }

    /**
     * Returns this factory.
     *
     * @return this factory
     */
    @Override
    public ProxyHandlerFactory createProxyHandlerFactory() {
        return this;
    }

    /**
     * Returns {@link ProxyType#HTTP}.
     *
     * @return {@link ProxyType#HTTP}
     */
    @Override
    public ProxyType getProxyType() {
        return ProxyType.HTTP;
    }

    /**
     * Sets the proxy credentials. A {@code null} argument leaves the corresponding current
     * value unchanged.
     *
     * @param username the proxy username
     * @param password the proxy password
     */
    @Override
    public void setProxyAuth(String username, String password) {
        if (username!=null){
            this.username=username;
        }

        if (password!=null){
            this.password=password;
        }
    }

    /** {@inheritDoc} */
    @Override
    public SocketAddress getSocketAddress() {
        return this.proxyAddress;
    }

    // Visible for testing
    /**
     * Returns the configured username.
     *
     * @return the username, or {@code null}
     */
    String getUsername() {
        return this.username;
    }

    // Visible for testing
    /**
     * Returns the configured password.
     *
     * @return the password, or {@code null}
     */
    String getPassword() {
        return this.password;
    }

    /**
     * Describes the proxy address and username, with the password masked.
     *
     * @return a string representation of this factory
     */
    @Override
    public String toString() {
        return "HttpProxyHandlerFactory {" +
                "proxyAddress=" + proxyAddress +
                ", username=" + username +
                ", password=*****"
                + "}";
    }
}
