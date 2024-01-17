package it.unipi.dsmt.websockets;

import jakarta.websocket.Session;


public interface EventEndpoint {
    public void onOpen(Session session);
    public void onClose(Session session);
}

