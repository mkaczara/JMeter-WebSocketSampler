/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package JMeter.plugins.functional.samplers.websocket;

import org.apache.commons.lang3.StringUtils;
import org.apache.jmeter.config.Argument;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.http.control.CookieHandler;
import org.apache.jmeter.protocol.http.control.CookieManager;
import org.apache.jmeter.protocol.http.control.Header;
import org.apache.jmeter.protocol.http.control.HeaderManager;
import org.apache.jmeter.protocol.http.util.EncoderCache;
import org.apache.jmeter.protocol.http.util.HTTPArgument;
import org.apache.jmeter.protocol.http.util.HTTPConstants;
import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testelement.TestElement;
import org.apache.jmeter.testelement.TestStateListener;
import org.apache.jmeter.testelement.property.PropertyIterator;
import org.apache.jmeter.testelement.property.StringProperty;
import org.apache.jmeter.testelement.property.TestElementProperty;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.jorphan.reflect.ClassTools;
import org.apache.jorphan.util.JMeterException;
import org.apache.jorphan.util.JOrphanUtils;
import org.apache.log.Logger;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.util.HttpCookieStore;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.api.WebSocketTimeoutException;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.HttpCookie;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author Maciej Zaleski
 */
public class WebSocketSampler extends AbstractSampler implements TestStateListener {
    private static final long serialVersionUID = 5859387434748163229L;

    public static final int DEFAULT_CONNECTION_TIMEOUT = 20000; //20 sec
    public static final int DEFAULT_RESPONSE_TIMEOUT = 20000; //20 sec
    public static final int MESSAGE_BACKLOG_COUNT = 3;

    private static final Logger log = LoggingManager.getLoggerForClass();

    private static final String ARG_VAL_SEP = "="; // $NON-NLS-1$
    private static final String QRY_SEP = "&"; // $NON-NLS-1$
    private static final String WS_PREFIX = "ws://"; // $NON-NLS-1$
    private static final String WSS_PREFIX = "wss://"; // $NON-NLS-1$
    private static final String DEFAULT_PROTOCOL = "ws";

    private static Map<String, ServiceSocket> connectionsMap = Collections.synchronizedMap(new HashMap<String, ServiceSocket>());
    private static WebSocketClient webSocketClient;

    private static Map<String, ScheduledExecutorService> schedulers = new HashMap<>();

    private static ScheduledExecutorService getScheduler(String name, int threads) {

        synchronized (schedulers) {
            if (schedulers.containsKey(name)) {
                return schedulers.get(name);
            }

            log.info("Creating websocket ping executor " + name + " with " + threads + " threads");
            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(threads);
            schedulers.put(name, scheduler);
            return scheduler;
        }
    }

    private ScheduledFuture<?> pingerFuture;

    private HeaderManager headerManager;
    private CookieManager cookieManager;
    private CookieHandler cookieHandler;

    public WebSocketSampler() {
        super();
    }

    private ServiceSocket getConnectionSocket() throws Exception {
        URI uri = getUri();
        boolean addSocketToMap = false;

        String connectionId = getConnectionIdForConnectionsMap();
        ServiceSocket socket;

        if (isStreamingConnection()) {
            socket = connectionsMap.get(connectionId);
            if(null != socket) {
                socket.initialize(this);
                return socket;
            }
        }

        if(isStreamingConnection()){
            socket = new ServiceSocket(this);
            addSocketToMap = true;
        } else {
            socket = new ServiceSocket(this);
        }

        //Start WebSocket client thread and upgrage HTTP connection
        ClientUpgradeRequest request = new ClientUpgradeRequest();
        if (headerManager != null) {
            for (int i = 0; i < headerManager.size(); i++) {
                Header header = headerManager.get(i);
                request.setHeader(header.getName(), header.getValue());
            }
        }
        if (cookieManager != null) {
            HttpCookieStore cookieStore = new HttpCookieStore();
            for (int i = 0; i < cookieManager.getCookieCount(); i++) {
                HttpCookie cookie = new HttpCookie(cookieManager.get(i).getName(), cookieManager.get(i).getValue());
                cookie.setVersion(cookieManager.get(i).getVersion());
                cookieStore.add(
                        new URI(null,
                                cookieManager.get(i).getDomain(),
                                cookieManager.get(i).getPath(),
                                null),
                        cookie
                );
            }
            request.setRequestURI(uri);
            request.setCookiesFrom(cookieStore);
        }
        webSocketClient.connect(socket, uri, request);

        //Get connection timeout or use the default value
        socket.awaitOpen(getPropertyAsInt("connectionTimeoutInt"), TimeUnit.MILLISECONDS);

        if(!socket.isConnected()){
            throw new WebSocketTimeoutException("Connection timeout!");
        }

        int pingFrequency = Integer.parseInt(getPingFrequency());
        final ServiceSocket socketFinal = socket;
        if (pingFrequency > 0) {
            ScheduledExecutorService scheduler = getScheduler(getPingThreadPoolName(), Integer.parseInt(getPingThreadPoolSize()));
            pingerFuture = scheduler.scheduleAtFixedRate(
                    new Runnable() {

                        @Override
                        public void run() {
                            log.debug("pinging");
                            socketFinal.ping();
                        }

                    },
                    0, pingFrequency, TimeUnit.SECONDS);
        }

        if (cookieManager != null && cookieHandler != null) {
            String setCookieHeader = socket.getSession().getUpgradeResponse().getHeader("set-cookie");
            if (setCookieHeader != null) {
                cookieHandler.addCookieFromHeader(cookieManager, true, setCookieHeader, new URL(
                        uri.getScheme() == null || "ws".equalsIgnoreCase(uri.getScheme()) ? "HTTP" : "HTTPS",
                        uri.getHost(),
                        uri.getPort(),
                        uri.getQuery() != null ? uri.getPath() + "?" + uri.getQuery() : uri.getPath()
                ));
            }
        }

        if(addSocketToMap) {
            connectionsMap.put(connectionId, socket);
        }
        return socket;
    }

    @Override
    public SampleResult sample(Entry entry) {
        ServiceSocket socket = null;
        SampleResult sampleResult = new SampleResult();
        sampleResult.setSampleLabel(getName());
        sampleResult.setDataEncoding(getContentEncoding());

        //This StringBuilder will track all exceptions related to the protocol processing
        StringBuilder errorList = new StringBuilder();
        errorList.append("\n\n[Problems]\n");

        boolean isOK = false;
        boolean isTimeout = false;

        //Set the message payload in the Sampler
        String payloadMessage = getRequestPayload();
        sampleResult.setSamplerData(payloadMessage);

        log.info("Read payload message: " + payloadMessage);

        //Could improve precission by moving this closer to the action
        sampleResult.sampleStart();
        try {
            socket = getConnectionSocket();
            if (socket == null) {
                //Couldn't open a connection, set the status and exit
                sampleResult.setResponseCode("500");
                sampleResult.setSuccessful(false);
                sampleResult.sampleEnd();
                sampleResult.setResponseMessage(errorList.toString());
                errorList.append(" - Connection couldn't be opened\n");
                return sampleResult;
            }

            //Send message only if it is not empty
            if (!payloadMessage.isEmpty()) {
                socket.sendMessage(payloadMessage);
            }

            //Wait for any of the following:
            // - Response matching response pattern is received
            // - Response matching connection closing pattern is received
            // - Timeout is reached
            socket.awaitClose(getPropertyAsInt("responseTimeoutInt"), TimeUnit.MILLISECONDS);
            sampleResult.sampleEnd();

            if (pingerFuture != null) {
                log.info("removing pinger");
                pingerFuture.cancel(true);
            }

            //If no response matching response pattern is reveived
            // and no response matching connection closing pattern is received
            // it seems that timeout is reached
            String responseMessage = socket.getResponseMessage();
            isTimeout = responseMessage == null || responseMessage.isEmpty() || !socket.isExpressionMatched();

            //Set sampler response code
            if (socket.getError() != 0 || isTimeout) {
                sampleResult.setResponseCode(isTimeout ? String.valueOf(HttpStatus.NO_CONTENT_204) : socket.getError().toString());
                isOK = false;
            } else {
                sampleResult.setResponseCodeOK();
                isOK = true;
            }

            //set sampler response
            sampleResult.setResponseData(socket.getResponseMessage(), getContentEncoding());
            if (getClearBacklog()) {
                socket.clearBacklog();
            }
        } catch (URISyntaxException e) {
            isOK = false;
            errorList.append(" - Invalid URI syntax: ").append(e.getMessage()).append("\n").append(StringUtils.join(e.getStackTrace(), "\n")).append("\n");
        } catch (IOException e) {
            isOK = false;
            errorList.append(" - IO Exception: ").append(e.getMessage()).append("\n").append(StringUtils.join(e.getStackTrace(), "\n")).append("\n");
        } catch (NumberFormatException e) {
            isOK = false;
            errorList.append(" - Cannot parse number: ").append(e.getMessage()).append("\n").append(StringUtils.join(e.getStackTrace(), "\n")).append("\n");
        } catch (InterruptedException e) {
            isOK = false;
            errorList.append(" - Execution interrupted: ").append(e.getMessage()).append("\n").append(StringUtils.join(e.getStackTrace(), "\n")).append("\n");
        } catch (Exception e) {
            isOK = false;
            errorList.append(" - Unexpected error: ").append(e.getMessage()).append("\n").append(StringUtils.join(e.getStackTrace(), "\n")).append("\n");
        } finally {
            if(sampleResult.getEndTime() == 0L){
                sampleResult.sampleEnd();
            }
            if(sampleResult.getResponseCode().isEmpty()){
                sampleResult.setResponseCode(String.valueOf(HttpStatus.BAD_REQUEST_400));
            }
            if (socket != null) {
                if (!isOK) {
                    socket.close();
                }
                if (!socket.isConnected()) {
                    connectionsMap.remove(getConnectionIdForConnectionsMap());
                }
            }
            sampleResult.setSuccessful(isOK);
            String logMessage = (socket != null) ? socket.getLogMessage() : "";
            sampleResult.setResponseMessage(logMessage + errorList);
        }
        return sampleResult;
    }

    @Override
    public void setName(String name) {
        if (name != null) {
            setProperty(TestElement.NAME, name);
        }
    }

    @Override
    public String getName() {
        return getPropertyAsString(TestElement.NAME);
    }

    @Override
    public void setComment(String comment) {
        setProperty(new StringProperty(TestElement.COMMENTS, comment));
    }

    @Override
    public String getComment() {
        return getProperty(TestElement.COMMENTS).getStringValue();
    }

    public URI getUri() throws URISyntaxException {
        String path = this.getContextPath();
        // Hack to allow entire URL to be provided in host field
        if (path.startsWith(WS_PREFIX)
                || path.startsWith(WSS_PREFIX)) {
            return new URI(path);
        }
        String domain = getServerAddress();
        String protocol = getProtocol();
        // HTTP URLs must be absolute, allow file to be relative
        if (!path.startsWith("/")) { // $NON-NLS-1$
            path = "/" + path; // $NON-NLS-1$
        }

        String queryString = getQueryString(getContentEncoding());
        if (isProtocolDefaultPort()) {
            return new URI(protocol, null, domain, -1, path, queryString, null);
        }
        return new URI(protocol, null, domain, Integer.parseInt(getServerPort()), path, queryString, null);
    }

    /**
     * Tell whether the default port for the specified protocol is used
     *
     * @return true if the default port number for the protocol is used, false
     * otherwise
     */
    public boolean isProtocolDefaultPort() {
        final int port = Integer.parseInt(getServerPort());
        final String protocol = getProtocol();
        return ("ws".equalsIgnoreCase(protocol) && port == HTTPConstants.DEFAULT_HTTP_PORT)
                || ("wss".equalsIgnoreCase(protocol) && port == HTTPConstants.DEFAULT_HTTPS_PORT);
    }

    public String getServerPort() {
        final String portAsString = getPropertyAsString("serverPort", "0");
        Integer port;
        String protocol = getProtocol();

        try {
            port = Integer.valueOf(portAsString);
        } catch (Exception ex) {
            return portAsString;
        }

        if (port == 0) {
            if ("wss".equalsIgnoreCase(protocol)) {
                return String.valueOf(HTTPConstants.DEFAULT_HTTPS_PORT);
            } else if ("ws".equalsIgnoreCase(protocol)) {
                return String.valueOf(HTTPConstants.DEFAULT_HTTP_PORT);
            }
        }
        return port.toString();
    }

    public void setServerPort(String port) {
        setProperty("serverPort", port);
    }

    public String getResponseTimeout() {
        return getPropertyAsString("responseTimeout", "20000");
    }

    public void setResponseTimeout(String responseTimeout) {
        setProperty("responseTimeout", responseTimeout);
    }


    public String getConnectionTimeout() {
        return getPropertyAsString("connectionTimeout", "5000");
    }

    public void setConnectionTimeout(String connectionTimeout) {
        setProperty("connectionTimeout", connectionTimeout);
    }

    public void setProtocol(String protocol) {
        setProperty("protocol", protocol);
    }

    public String getProtocol() {
        String protocol = getPropertyAsString("protocol");
        if (protocol == null || protocol.isEmpty()) {
            return DEFAULT_PROTOCOL;
        }
        return protocol;
    }

    public void setServerAddress(String serverAddress) {
        setProperty("serverAddress", serverAddress);
    }

    public String getServerAddress() {
        return getPropertyAsString("serverAddress");
    }


    public void setImplementation(String implementation) {
        setProperty("implementation", implementation);
    }

    public String getImplementation() {
        return getPropertyAsString("implementation");
    }

    public void setContextPath(String contextPath) {
        setProperty("contextPath", contextPath);
    }

    public String getContextPath() {
        return getPropertyAsString("contextPath");
    }

    public void setContentEncoding(String contentEncoding) {
        setProperty("contentEncoding", contentEncoding);
    }

    public String getContentEncoding() {
        return getPropertyAsString("contentEncoding", "UTF-8");
    }

    public void setRequestPayload(String requestPayload) {
        setProperty("requestPayload", requestPayload);
    }

    public String getRequestPayload() {
        return getPropertyAsString("requestPayload");
    }

    public void setIgnoreSslErrors(Boolean ignoreSslErrors) {
        setProperty("ignoreSslErrors", ignoreSslErrors);
    }

    public Boolean isIgnoreSslErrors() {
        return getPropertyAsBoolean("ignoreSslErrors");
    }

    public void setStreamingConnection(Boolean streamingConnection) {
        setProperty("streamingConnection", streamingConnection);
    }

    public Boolean isStreamingConnection() {
        return getPropertyAsBoolean("streamingConnection");
    }

    public void setConnectionId(String connectionId) {
        setProperty("connectionId", connectionId);
    }

    public String getConnectionId() {
        return getPropertyAsString("connectionId");
    }

    public void setResponsePattern(String responsePattern) {
        setProperty("responsePattern", responsePattern);
    }

    public String getResponsePattern() {
        return getPropertyAsString("responsePattern");
    }

    public void setCloseConncectionPattern(String closeConncectionPattern) {
        setProperty("closeConncectionPattern", closeConncectionPattern);
    }

    public String getCloseConncectionPattern() {
        return getPropertyAsString("closeConncectionPattern");
    }

    public void setProxyAddress(String proxyAddress) {
        setProperty("proxyAddress", proxyAddress);
    }

    public String getProxyAddress() {
        return getPropertyAsString("proxyAddress");
    }

    public void setProxyPassword(String proxyPassword) {
        setProperty("proxyPassword", proxyPassword);
    }

    public String getProxyPassword() {
        return getPropertyAsString("proxyPassword");
    }

    public void setProxyPort(String proxyPort) {
        setProperty("proxyPort", proxyPort);
    }

    public String getProxyPort() {
        return getPropertyAsString("proxyPort");
    }

    public void setProxyUsername(String proxyUsername) {
        setProperty("proxyUsername", proxyUsername);
    }

    public String getProxyUsername() {
        return getPropertyAsString("proxyUsername");
    }

    public void setPingFrequency(String pingFrequency) {
        setProperty("pingFrequency", pingFrequency);
    }

    public String getPingFrequency() {
        return getPropertyAsString("pingFrequency", "0");
    }

    public void setPingThreadPoolName(String pingThreadPoolName) {
        setProperty("pingThreadPoolName", pingThreadPoolName);
    }

    public String getPingThreadPoolName() {
        return getPropertyAsString("pingThreadPoolName", "default");
    }

    public void setPingThreadPoolSize(String pingThreadPoolSize) {
        setProperty("pingThreadPoolSize", pingThreadPoolSize);
    }

    public String getPingThreadPoolSize() {
        return getPropertyAsString("pingThreadPoolSize", "10");
    }

    public void setMessageBacklog(String messageBacklog) {
        setProperty("messageBacklog", messageBacklog);
    }

    public String getMessageBacklog() {
        return getPropertyAsString("messageBacklog", "3");
    }

    public void setClearBacklog(Boolean clearBacklog) {
        setProperty("clearBacklog", clearBacklog);
    }

    public Boolean getClearBacklog() {
        return getPropertyAsBoolean("clearBacklog", true);
    }

    public String getQueryString(String contentEncoding) {
        // Check if the sampler has a specified content encoding
        if (JOrphanUtils.isBlank(contentEncoding)) {
            // We use the encoding which should be used according to the HTTP spec, which is UTF-8
            contentEncoding = EncoderCache.URL_ARGUMENT_ENCODING;
        }
        StringBuilder buf = new StringBuilder();
        PropertyIterator iter = getQueryStringParameters().iterator();
        boolean first = true;
        while (iter.hasNext()) {
            HTTPArgument item;
            Object objectValue = iter.next().getObjectValue();
            try {
                item = (HTTPArgument) objectValue;
            } catch (ClassCastException e) {
                item = new HTTPArgument((Argument) objectValue);
            }
            final String encodedName = item.getEncodedName();
            if (encodedName.length() == 0) {
                continue; // Skip parameters with a blank name (allows use of optional variables in parameter lists)
            }
            if (!first) {
                buf.append(QRY_SEP);
            } else {
                first = false;
            }
            buf.append(encodedName);
            if (item.getMetaData() == null) {
                buf.append(ARG_VAL_SEP);
            } else {
                buf.append(item.getMetaData());
            }

            // Encode the parameter value in the specified content encoding
            try {
                buf.append(item.getEncodedValue(contentEncoding));
            } catch (UnsupportedEncodingException e) {
                log.warn("Unable to encode parameter in encoding " + contentEncoding + ", parameter value not included in query string");
            }
        }
        return buf.toString();
    }

    public void setQueryStringParameters(Arguments queryStringParameters) {
        setProperty(new TestElementProperty("queryStringParameters", queryStringParameters));
    }

    public Arguments getQueryStringParameters() {
        return (Arguments) getProperty("queryStringParameters").getObjectValue();
    }


    @Override
    public void testStarted() {
        testStarted("unknown");
    }

    @Override
    public void testStarted(String host) {
        if(null == webSocketClient || webSocketClient.isStopped()) {
            webSocketClient = createWebSocketClient();
            try {
                webSocketClient.start();
                log.info("WebSocketClient started");
            } catch (Exception e) {
                log.error("WebSocketClient has not been started", e);
            }
        }

        try {
            setProperty("connectionTimeoutInt", Integer.parseInt(getConnectionTimeout()));
        } catch (NumberFormatException ex) {
            log.warn("Connection timeout is not a number; using the default connection timeout of " + DEFAULT_CONNECTION_TIMEOUT + " ms");
            setProperty("connectionTimeoutInt", DEFAULT_RESPONSE_TIMEOUT);
        }
        try {
            setProperty("responseTimeoutInt", Integer.parseInt(getResponseTimeout()));
        } catch (NumberFormatException ex) {
            log.warn("Response timeout is not a number; using the default response timeout of " + DEFAULT_RESPONSE_TIMEOUT + " ms");
            setProperty("responseTimeoutInt", DEFAULT_RESPONSE_TIMEOUT);
        }
    }

    @Override
    public void testEnded() {
        testEnded("unknown");
    }

    @Override
    public void testEnded(String host) {
        for (ServiceSocket socket : connectionsMap.values()) {
            socket.close();
        }
        connectionsMap.clear();
        for(ScheduledExecutorService executor : schedulers.values()){
            executor.shutdownNow();
        }
        schedulers.clear();
        stopWebSocketClient();
    }

    @Override
    public void addTestElement(TestElement el) {
        if (el instanceof HeaderManager) {
            headerManager = (HeaderManager) el;
        } else if (el instanceof CookieManager) {
            cookieManager = (CookieManager) el;
            try {
                cookieHandler = (CookieHandler) ClassTools.construct(cookieManager.getImplementation(), cookieManager.getPolicy());
            } catch (JMeterException e) {
                log.error("Failed to construct cookie handler ", e);
            }
        } else {
            super.addTestElement(el);
        }
    }

    String getConnectionIdForConnectionsMap(){
        return getThreadName() + getConnectionId();
    }

    private WebSocketClient createWebSocketClient(){
        SslContextFactory sslContexFactory = new SslContextFactory();
        sslContexFactory.setTrustAll(isIgnoreSslErrors());
        return new WebSocketClient(sslContexFactory, Executors.newCachedThreadPool());
    }

    private void stopWebSocketClient(){
        //Stoping WebSocket client; thanks m0ro
        if(!webSocketClient.isStopped() && !webSocketClient.isStopping()) {
            try {
                webSocketClient.stop();
                ((ExecutorService) webSocketClient.getExecutor()).shutdownNow();
                log.info("WebSocket client closed");
            } catch (Exception e) {
                log.error("WebSocket client wasn't started (...that's odd)");
            }
        }
    }
}
