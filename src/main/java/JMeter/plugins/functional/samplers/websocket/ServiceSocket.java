/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package JMeter.plugins.functional.samplers.websocket;

import org.apache.jmeter.engine.util.CompoundVariable;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.StatusCode;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 *
 * @author Maciej Zaleski
 */
@WebSocket(maxTextMessageSize = 256 * 1024 * 1024)
public class ServiceSocket {
    private static final Logger log = LoggingManager.getLoggerForClass();

    protected WebSocketSampler sampler;
    protected Queue<String> responseBacklog = new ConcurrentLinkedQueue<>();
    protected Integer error = 0;
    protected StringBuilder logMessage = new StringBuilder();
    protected CountDownLatch openLatch = new CountDownLatch(1);
    protected CountDownLatch closeLatch = new CountDownLatch(1);
    protected Session session;
    protected Pattern responseExpression;
    protected Pattern disconnectExpression;
    protected boolean connected = false;
    protected boolean expressionMatched = false;

    public ServiceSocket(WebSocketSampler sampler) {
        this.sampler = sampler;

        logMessage.append("\n\n[Execution Flow]\n");
        logMessage.append(" - Opening new connection\n");
        initializePatterns();
    }

    @OnWebSocketMessage
    public void onMessage(String msg) {
        log.debug("Received message: " + msg);
        String length = " (" + msg.length() + " bytes)";
        logMessage.append(" - Received message ").append(length);
        addResponseMessage(postProcessMessage(msg));

        if (responseExpression == null || responseExpression.matcher(msg).find()) {
            logMessage.append("; matched response pattern").append('\n');
            expressionMatched = true;
            closeLatch.countDown();
        } else if (disconnectExpression != null && disconnectExpression.matcher(msg).find()) {
            logMessage.append("; matched connection close pattern").append('\n');
            expressionMatched = true;
            close(StatusCode.NORMAL, "JMeter closed session.");
        } else {
            logMessage.append("; didn't match any pattern").append('\n');
        }
    }

    @OnWebSocketConnect
    public void onOpen(Session session) {
        logMessage.append(" - WebSocket conection has been opened").append('\n');
        log.debug("Connect " + session.isOpen());
        this.session = session;
        connected = true;
        openLatch.countDown();
    }

    @OnWebSocketClose
    public synchronized void onClose(int statusCode, String reason) {
        if(connected) {
            if (statusCode != 1000) {
                log.error("Disconnect " + statusCode + ": " + reason);
                logMessage.append(" - WebSocket conection closed unexpectedly by the server: [").append(statusCode).append("] ").append(reason).append('\n');
                error = statusCode;
            } else {
                logMessage.append(" - WebSocket conection has been successfully closed by the server").append('\n');
                log.debug("Disconnect " + statusCode + ": " + reason);
            }

            //Notify connection opening and closing latches of the closed connection
            connected = false;
            openLatch.countDown();
            closeLatch.countDown();
        }
    }

    /**
     * @return response message made of messages saved in the responseBacklog cache
     */
    public String getResponseMessage() {
        StringBuilder responseMessageBuilder = new StringBuilder();

        //Iterate through response messages saved in the responseBacklog cache
        for (String aResponseBacklog : responseBacklog) {
            responseMessageBuilder.append(aResponseBacklog);
        }

        return responseMessageBuilder.toString();
    }

    public void clearBacklog() {
        responseBacklog.clear();
    }

    public boolean awaitClose(int duration, TimeUnit unit) throws InterruptedException {
        logMessage.append(" - Waiting for messages for ").append(duration).append(" ").append(unit).append('\n');
        boolean res = this.closeLatch.await(duration, unit);

        if (!sampler.isStreamingConnection()) {
            close(StatusCode.NORMAL, "JMeter closed session.");
        } else {
            if(connected) {
                logMessage.append(" - Leaving streaming connection open").append('\n');
            }
        }

        return res;
    }

    public boolean awaitOpen(int duration, TimeUnit unit) throws InterruptedException {
        logMessage.append(" - Waiting for the server connection for ").append(duration).append(" ").append(unit).append('\n');
        boolean res = this.openLatch.await(duration, unit);

        if (connected) {
            logMessage.append(" - Connection established").append('\n');
        } else {
            logMessage.append(" - Cannot connect to the remote server").append('\n');
        }

        return res;
    }

    /**
     * @return the session
     */
    public Session getSession() {
        return session;
    }

    public void sendMessage(String message) throws IOException {
        expressionMatched = false;
        session.getRemote().sendString(preProcessMessage(message));
    }

    public void close() {
        close(StatusCode.NORMAL, "JMeter closed session.");
    }

    public synchronized void close(int statusCode, String statusText) {
        //Closing WebSocket session
        if (session != null) {
            session.close(statusCode, statusText);
            logMessage.append(" - WebSocket session closed by the client").append('\n');
        } else {
            logMessage.append(" - WebSocket session wasn't started (...that's odd)").append('\n');
        }
        connected = false;
        closeLatch.countDown();
    }

    public void ping() {
        try {
            session.getRemote().sendPing(null);
        } catch (IOException e) {
            log.warn("PING FAILED", e);
        }
    }

    public boolean isExpressionMatched(){
        return expressionMatched;
    }
    
    /**
     * @return the error
     */
    public Integer getError() {
        return error;
    }

    /**
     * @return the logMessage
     */
    public String getLogMessage() {
        return logMessage.toString();
    }

    private void initializePatterns() {
        //Evaluate response matching patterns in case thay contain JMeter variables (i.e. ${var})
        String responsePattern = new CompoundVariable(sampler.getResponsePattern()).execute();
        String disconnectPattern = new CompoundVariable(sampler.getCloseConncectionPattern()).execute();

        try {
            logMessage.append(" - Using response message pattern \"").append(responsePattern).append("\"\n");
            responseExpression = (responsePattern != null && !responsePattern.isEmpty()) ? Pattern.compile(responsePattern) : null;
        } catch (Exception ex) {
            logMessage.append(" - Invalid response message regular expression pattern: ").append(ex.getLocalizedMessage()).append('\n');
            log.error("Invalid response message regular expression pattern: " + ex.getLocalizedMessage());
            responseExpression = null;
        }

        try {
            logMessage.append(" - Using disconnect pattern \"").append(disconnectPattern).append("\"\n");
            disconnectExpression = (disconnectPattern != null && !disconnectPattern.isEmpty()) ? Pattern.compile(disconnectPattern) : null;
        } catch (Exception ex) {
            logMessage.append(" - Invalid disconnect regular expression pattern: ").append(ex.getLocalizedMessage()).append('\n');
            log.error("Invalid disconnect regular regular expression pattern: " + ex.getLocalizedMessage());
            disconnectExpression = null;
        }
    }

    /**
     * @return the connected
     */
    public boolean isConnected() {
        return connected;
    }

    public void initialize(WebSocketSampler sampler) {
        this.sampler = sampler;

        this.logMessage = new StringBuilder();
        this.logMessage.append("\n\n[Execution Flow]\n");
        this.logMessage.append(" - Reusing existing connection: ").append(sampler.getConnectionIdForConnectionsMap()).append('\n');
        this.error = 0;

        initializePatterns();

        this.closeLatch = new CountDownLatch(1);
    }

    private void addResponseMessage(String message) {
        int messageBacklog;
        try {
            messageBacklog = Integer.parseInt(sampler.getMessageBacklog());
        } catch (Exception ex) {
            logMessage.append(" - Message backlog value not set; using default ").append(WebSocketSampler.MESSAGE_BACKLOG_COUNT).append("\n");
            messageBacklog = WebSocketSampler.MESSAGE_BACKLOG_COUNT;
        }

        while (responseBacklog.size() >= messageBacklog) {
            responseBacklog.poll();
        }
        responseBacklog.add(message);
    }

    private String preProcessMessage(String message){
        if(!message.isEmpty()){
            message = message.length() + "|" + message;
        }
        return message;
    }

    private String postProcessMessage(String message){
        return message.replaceFirst("[0-9]+\\|", "");
    }
}
