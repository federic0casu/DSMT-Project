package it.unipi.dsmt.javaee.webapp.websockets;

import javax.websocket.Session;


public interface EventEndpoint {
    public void onOpen(Session session);
    public void onClose(Session session);
    public void sendEventUpdate(String timestamp, String severity, String event, String description);
}

