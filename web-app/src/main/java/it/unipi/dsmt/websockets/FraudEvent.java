package it.unipi.dsmt.websockets;

import java.io.IOException;

import jakarta.websocket.OnOpen;
import jakarta.websocket.OnClose;
import jakarta.websocket.Session;
import jakarta.websocket.server.ServerEndpoint;

import java.util.concurrent.CopyOnWriteArrayList;

@ServerEndpoint(value = "/events/fraud")
public class FraudEvent implements EventEndpoint {
    private static final CopyOnWriteArrayList<Session> sessions = new CopyOnWriteArrayList<>();
    @OnOpen
    public void onOpen(Session session) {
        // Add the new session to the list
        sessions.add(session);
    }
    @OnClose
    public void onClose(Session session) {
        // Remove the closed session from the list
        sessions.remove(session);
    }
    public void sendFraudEventUpdate(String timestamp, String severity, String event, String description) {
        String message = String.format("<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>", timestamp, severity, event, description);

        for (Session session : sessions) {
            try {
                session.getBasicRemote().sendText(message);
            } catch (IOException e) {
                System.err.println(e.getMessage());
            }
        }
    }
}

