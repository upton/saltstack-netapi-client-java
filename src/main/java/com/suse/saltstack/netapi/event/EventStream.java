package com.suse.saltstack.netapi.event;

import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.ProxyServer;
import com.ning.http.util.StringUtils;
import com.suse.saltstack.netapi.config.ClientConfig;
import org.atmosphere.wasync.Client;
import org.atmosphere.wasync.ClientFactory;
import org.atmosphere.wasync.Decoder;
import org.atmosphere.wasync.Encoder;
import org.atmosphere.wasync.Event;
import org.atmosphere.wasync.Function;
import org.atmosphere.wasync.OptionsBuilder;
import org.atmosphere.wasync.Request;
import org.atmosphere.wasync.RequestBuilder;
import org.atmosphere.wasync.Socket;
import org.atmosphere.wasync.impl.DefaultOptionsBuilder;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;


/**
 * Event stream implementation based on wAsync:
 * (A WebSockets/HTTP Client Library for Asynchronous Communication)
 */
public class EventStream implements AutoCloseable {

    /**
     * Listeners that are notified of a new events.
     */
    private final List<EventListener> listeners = new ArrayList<>();

    /**
     * The ClientConfig used to create the GET request for /events
     */
    private ClientConfig config;

    /**
     * wAsync implementation
     */
    private RequestBuilder request;
    private Socket socket;

    /**
     * Constructor used to create this object.  Automatically starts
     * event processing.
     * @param config Contains the necessary details such as endpoint URL and
     *               authentication token required to create the request to obtain
     *               the event stream.
     */
    public EventStream(ClientConfig config) {
        this.config = config;
        initializeStream();
    }

    /**
     * Implementation of {@link EventStream#addEventListener(EventListener)}
     * @param listener Reference to the class that implements {@link EventListener}.
     */
    public void addEventListener(EventListener listener) {
        synchronized (listeners) {
            listeners.add(listener);
        }
    }

    /**
     * Implementation of {@link EventStream#removeEventListener(EventListener)}
     * @param listener
     */
    public void removeEventListener(EventListener listener) {
        synchronized (listeners) {
            listeners.remove(listener);
        }
    }

    /**
     * Helper method that returns the current number of subscribed
     * listeners.
     * @return The current number listeners.
     */
    public int getListenerCount() {
        return listeners.size();
    }

    /**
     * Closes the backing event stream and notifies all subscribed listeners that
     * the event stream has been closed via {@link EventListener#eventStreamClosed()}.
     * Upon exit from this method, all subscribed listeners will be removed.
     */
    public void close() {
        if (!isEventStreamClosed()) {
            socket.close();
        }
    }

    /**
     * Helper method to determine whether the backing event stream is closed.
     * @return Whether the stream is closed.
     */
    public boolean isEventStreamClosed() {
        return socket.status() == Socket.STATUS.CLOSE ||
                socket.status() == Socket.STATUS.ERROR;
    }

    /**
     * Perform the REST GET call to /events and set up the event stream.  If
     * a proxy is configured be sure to account for it.
     */
    private void initializeStream() {

        AsyncHttpClientConfig.Builder httpConfigBuilder = new AsyncHttpClientConfig.Builder();
        String proxyHost = config.get(ClientConfig.PROXY_HOSTNAME);
        if (proxyHost != null && !proxyHost.isEmpty()) {

            ProxyServer proxyConfig = new ProxyServer(
                    proxyHost,
                    config.get(ClientConfig.PROXY_PORT),
                    config.get(ClientConfig.PROXY_USERNAME),
                    config.get(ClientConfig.PROXY_PASSWORD)
            );
            httpConfigBuilder.setProxyServer(proxyConfig);
        }
        AsyncHttpClient ahClient = new AsyncHttpClient(httpConfigBuilder.build());
        Client client = ClientFactory.getDefault().newClient();
        OptionsBuilder optionsBuilder = client.newOptionsBuilder();
        optionsBuilder.runtime(ahClient);

        request = client.newRequestBuilder()
                .method(Request.METHOD.GET)
                .uri(config.get(ClientConfig.URL) + "/events")
                .encoder(new Encoder<String, Reader>() {
                    @Override
                    public Reader encode(String s) {
                        return new StringReader(s);
                    }
                })
                .decoder(new Decoder<String, String>() {
                    @Override
                    public String decode(Event type, String s) {
                        return s;
                    }
                })
                .transport(Request.TRANSPORT.SSE)
                .header("X-Auth-Token", config.get(ClientConfig.TOKEN));

        socket = client.create(optionsBuilder.build());

        socket.on(Event.MESSAGE, new Function<String>() {
            @Override
            public void on(String s) {
                synchronized (listeners) {
                    for (EventListener listener : listeners) {
                        listener.notify(s);
                    }
                }
            }
        }).on(Event.CLOSE, new Function<String>() {
            @Override
            public void on(String s) {
                synchronized (listeners) {
                    for (EventListener listener : listeners) {
                        listener.eventStreamClosed();
                    }
                }
            }
        }).on(Event.HEADERS, new Function<String>() {
            @Override
            public void on(String s) {
                System.out.println("HEader: " + s);
            }
        });

        try {
            socket.open(request.build());
        } catch (IOException e) {
            close();
        }

    }
}
