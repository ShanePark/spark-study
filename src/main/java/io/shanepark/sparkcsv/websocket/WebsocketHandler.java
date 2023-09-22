package io.shanepark.sparkcsv.websocket;

import io.shanepark.sparkcsv.database.ResultDB;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Slf4j
@RequiredArgsConstructor
public class WebsocketHandler extends TextWebSocketHandler {

    private final ConcurrentHashMap<WebSocketSession, Long> sessionMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, Set<WebSocketSession>> topicMap = new ConcurrentHashMap<>();
    private final ResultDB resultDB;

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        String clientMessage = message.getPayload();
        long id = Long.parseLong(clientMessage);
        addSession(session, id);
        if (resultDB.hasFinished(id)) {
            sendComplete(id);
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        log.info("Connection closed: {}", session.getId());
        removeSession(session);
    }

    public void sendComplete(Long id) {
        log.info("Sending complete message for topic: {}", id);
        Set<WebSocketSession> sessions = topicMap.get(id);
        if (sessions == null) {
            return;
        }
        sessions.forEach(session -> {
            try {
                session.sendMessage(new TextMessage("complete"));
            } catch (Exception e) {
                log.warn("Failed to send message to session: {}", session, e);
            }
        });
    }

    private void addSession(WebSocketSession session, long topicId) {
        if (sessionMap.containsKey(session)) {
            removeSession(session);
        }
        sessionMap.put(session, topicId);
        topicMap.computeIfAbsent(topicId, k -> ConcurrentHashMap.newKeySet()).add(session);
    }

    private void removeSession(WebSocketSession session) {
        Long topic = sessionMap.remove(session);
        if (topic == null) {
            return;
        }
        Set<WebSocketSession> sessions = topicMap.get(topic);
        if (sessions == null) {
            return;
        }
        sessions.remove(session);
    }

}
