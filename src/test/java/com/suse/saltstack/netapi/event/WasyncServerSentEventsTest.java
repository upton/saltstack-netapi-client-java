package com.suse.saltstack.netapi.event;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.suse.saltstack.netapi.client.SaltStackClient;
import com.suse.saltstack.netapi.utils.ClientUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;

/**
 * SaltStack events API Jersey SSE implementation test cases.
 */
public class WasyncServerSentEventsTest {

    private static final int MOCK_HTTP_PORT = 8888;

    /**
     * Note: {@link org.atmosphere.wasync.Socket} expects the raw event
     * stream to be \n\n delimited.  From the source code "SSE event chunk parser - SSE
     * chunks are delimited with a fixed "\n\n" delimiter in the response stream."  The
     * events_stream.txt file below is in the correct format.  If modifying the file to
     * add additional events etc, please take note.
     */
    static final String TEXT_EVENT_STREAM_RESPONSE = ClientUtils.streamToString(
            WasyncServerSentEventsTest.class.getResourceAsStream("/events_stream.txt"));

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(MOCK_HTTP_PORT);

    private SaltStackClient client;

    @Before
    public void init() {
        URI uri = URI.create("http://localhost:" + Integer.toString(MOCK_HTTP_PORT));
        client = new SaltStackClient(uri);
    }

    /**
     * Tests: listener insertion, listener notification, stream closed,
     * stream content
     */
    @Test
    public void shouldFireNotifyMultipleTimes() {
        stubFor(get(urlEqualTo("/events"))
                .willReturn(aResponse()
                .withHeader("Content-Type", "text/event-stream")
                .withHeader("Connection", "keep-alive")
                .withBody(TEXT_EVENT_STREAM_RESPONSE)));

        try (EventStream serverSentEvents =
                new EventStream(client.getConfig())) {
            EventCountClient eventCountClient = new EventCountClient(6);
            serverSentEvents.addEventListener(eventCountClient);

            eventCountClient.await();
        }
    }

    /**
     * Event listener client used for testing.
     */
    private class EventClientBase implements EventListener {

       protected List<String> events = new ArrayList<String>();

        // return all received events
        public List<String> getEvents() {
            return events;
        }

        @Override
        public void notify(String event) {}

        @Override
        public void eventStreamClosed() {}
    }

    /**
     * Event listener client used for testing.
     */
    private class EventCountClient extends EventClientBase {

        private static final int TIMEOUT = 5;

        private CountDownLatch latch;
        private int targetCount;
        private int counter = 0;

        public EventCountClient(int targetCount) {
            this.targetCount = targetCount;
            this.latch = new CountDownLatch(targetCount);
        }

        @Override
        public void notify(String event) {
            this.events.add(event);
            latch.countDown();
            counter++;
        }

        @Override
        public void eventStreamClosed() {
            Assert.assertEquals(targetCount, counter);
        }

        public void await() {
            try {
                latch.await(TIMEOUT, TimeUnit.SECONDS);
                Assert.assertEquals(targetCount, counter);
            }
            catch (InterruptedException e) {
                Assert.fail("Timeout waiting for " + targetCount + " events");
            }
        }
    }

    /**
     * Event listener client used for testing.
     */
    private class EventContentClient extends EventClientBase {
        @Override
        public void notify(String event) {
            events.add(event);
        }

        @Override
        public void eventStreamClosed() {
            Assert.assertTrue(events.get(2).contains("\"jid\": \"20150505113307407682\""));
        }
    }

    /**
     * Simple Event ListenerClient
     */
    private class SimpleEventListenerClient implements EventListener {
        @Override
        public void notify(String event) { }

        @Override
        public void eventStreamClosed() { }
    }

    /**
     * Event listener client used for testing.
     */
    private class EventStreamClosedClient implements EventListener {
        EventStream eventSource;

        public EventStreamClosedClient(EventStream eventSource) {
            this.eventSource = eventSource;
        }

        @Override
        public void notify(String event) {
            Assert.assertFalse(eventSource.isEventStreamClosed());
        }

        @Override
        public void eventStreamClosed() {
        }
    }
}
