/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.module.http.functional.sse;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.lang.System.lineSeparator;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertThat;
import static org.mule.api.lifecycle.LifecycleUtils.startIfNeeded;
import static org.mule.module.http.functional.TestEventStreamServer.SSE_ERROR_RESPONSE;
import static org.mule.module.http.functional.TestEventStreamServer.SSE_MOVED_RESPONSE;
import static org.mule.module.http.functional.TestEventStreamServer.SSE_RESPONSE;
import static org.mule.module.http.internal.sse.DefaultHttpEventStreamListener.LAST_EVENT_ID_KEY;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mule.api.MuleException;
import org.mule.api.MuleMessage;
import org.mule.api.client.MuleClient;
import org.mule.api.lifecycle.LifecycleUtils;
import org.mule.module.http.functional.TestEventStreamServer;
import org.mule.tck.junit4.FunctionalTestCase;
import org.mule.tck.junit4.rule.DynamicPort;
import org.mule.tck.probe.JUnitProbe;
import org.mule.tck.probe.PollingProber;

public class HttpEventStreamTestCase extends FunctionalTestCase
{

    @Rule
    public DynamicPort serverPort = new DynamicPort("serverPort");

    @Rule
    public DynamicPort serverBehindProxyPort = new DynamicPort("serverBehindProxyPort");

    @Rule
    public DynamicPort serverRedirectPort = new DynamicPort("serverRedirectPort");

    @Rule
    public DynamicPort proxyPort = new DynamicPort("proxyPort");

    // We need a different server for each flow with an SSE source sothe different flow don interfere with eachother
    private TestEventStreamServer sseServer = new TestEventStreamServer(serverPort.getNumber());
    private TestEventStreamServer sseBehindProxyServer = new TestEventStreamServer(serverBehindProxyPort.getNumber());
    private TestEventStreamServer sseServerForRedirect = new TestEventStreamServer(serverRedirectPort.getNumber());

    private MuleClient client;

    @Override
    protected void doSetUpBeforeMuleContextCreation() throws Exception
    {
        sseServer.setEventIntervalMillis(RECEIVE_TIMEOUT);
        sseBehindProxyServer.setEventIntervalMillis(RECEIVE_TIMEOUT);
        sseServerForRedirect.setEventIntervalMillis(RECEIVE_TIMEOUT);
    }

    @Before
    public void before() throws Exception
    {
        client = muleContext.getClient();
    }

    @Override
    protected void doTearDownAfterMuleContextDispose() throws Exception
    {
        sseServer.stop();
        sseBehindProxyServer.stop();
        sseServerForRedirect.stop();
    }

    @Override
    protected String getConfigFile()
    {
        return "http-event-stream-config.xml";
    }

    @Test
    public void basicMultilineEvent() throws Exception
    {
        sseServer.setEventsToSend(asList(
                "data: MULE\n" + 
                "data: +2\r" + 
                "data: 10\r\n" +
                "\r\n"));
        sseServer.start();
        startIfNeeded(muleContext.getRegistry().lookupFlowConstruct("sseConsumer"));

        MuleMessage result = client.request("vm://out", RECEIVE_TIMEOUT);

        assertThat(result.getPayload().toString(), is("MULE" + lineSeparator() + "+2" + lineSeparator() + "10"));
        assertThat("The message should only have the properties set by the vm endpoint (MULE_ENDPOINT and MULE_SESSION)",
                result.getInboundPropertyNames(), hasSize(2));
    }

    @Test
    public void basicMultilineSplitEvent() throws Exception
    {
        sseServer.setEventsToSend(asList(
                "data: MULE\n" + 
                        "dat",
                        "a: +2\r" + 
                        "data: 10\r\n" +
                "\r\n"));
        sseServer.setEventIntervalMillis(1000);
        sseServer.start();
        startIfNeeded(muleContext.getRegistry().lookupFlowConstruct("sseConsumer"));
        
        MuleMessage result = client.request("vm://out", RECEIVE_TIMEOUT);
        
        assertThat(result.getPayload().toString(), is("MULE" + lineSeparator() + "+2" + lineSeparator() + "10"));
        assertThat("The message should only have the properties set by the vm endpoint (MULE_ENDPOINT and MULE_SESSION)",
                result.getInboundPropertyNames(), hasSize(2));
    }
    
    @Test
    public void blockWithCommentsId() throws Exception {
        sseServer.setEventsToSend(asList(
                ": test stream\r\n" + 
                "\r\n" + 
                "data: first event\r\n" + 
                "id: 1\r\n" + 
                "\r\n" + 
                "data:second event\r\n" + 
                "id\r\n" + 
                "\r\n" + 
                "data:  third event\r\n"+ 
                "\r\n"));
        sseServer.start();
        startIfNeeded(muleContext.getRegistry().lookupFlowConstruct("sseConsumer"));

        Set<MuleMessage> results = new HashSet<>();

        for (int i = 0; i < 3; ++i)
        {
            results.add(client.request("vm://out", RECEIVE_TIMEOUT));
        }

        assertThat(results, hasSize(3));

        MuleMessage firstEvent = results.stream().filter(new Predicate<MuleMessage>()
        {
            @Override
            public boolean test(MuleMessage m)
            {
                return "first event".equals(m.getPayload().toString());
            }
        }).findAny().get();
        assertThat(firstEvent.getInboundPropertyNames(), hasSize(3));
        assertThat(firstEvent.getInboundProperty("id").toString(), is("1"));

        MuleMessage secondEvent = results.stream().filter(new Predicate<MuleMessage>()
        {
            @Override
            public boolean test(MuleMessage m)
            {
                return "second event".equals(m.getPayload().toString());
            }
        }).findAny().get();
        assertThat(secondEvent.getInboundPropertyNames(), hasSize(3));
        assertThat(secondEvent.getInboundProperty("id").toString(), is(""));

        assertThat(results.stream().filter(new Predicate<MuleMessage>()
        {
            @Override
            public boolean test(MuleMessage m)
            {
                return " third event".equals(m.getPayload().toString());
            }
        }).findAny().get().getInboundPropertyNames(), hasSize(2));
    }

    @Test
    public void blocksWithEmptyDataAndNoEndingLine() throws Exception {
        sseServer.setEventsToSend(asList(
                "data\r\n" + 
                "\r\n" + 
                "data\r\n" + 
                "data\r\n" + 
                "\r\n" + 
                "data:"));
        sseServer.setEventIntervalMillis(RECEIVE_TIMEOUT * 2);
        sseServer.start();
        startIfNeeded(muleContext.getRegistry().lookupFlowConstruct("sseConsumer"));

        Set<MuleMessage> results = new HashSet<>();

        for (int i = 0; i < 2; ++i)
        {
            results.add(client.request("vm://out", RECEIVE_TIMEOUT));
        }
        assertThat(client.request("vm://out", RECEIVE_TIMEOUT), nullValue());

        assertThat(results, hasSize(2));

        results.stream().filter(new Predicate<MuleMessage>()
        {
            @Override
            public boolean test(MuleMessage m)
            {
                return "".equals(m.getPayload().toString());
            }
        }).findAny().get();

        results.stream().filter(new Predicate<MuleMessage>()
        {
            @Override
            public boolean test(MuleMessage m)
            {
                return System.lineSeparator().equals(m.getPayload().toString());
            }
        }).findAny().get();
    }

    @Test
    public void optionalSpacePrefix() throws Exception {
        sseServer.setEventsToSend(asList(
                "data:test\r\n" + 
                "\r\n" + 
                "data: test\r\n" + 
                "\r\n"));
        sseServer.start();
        startIfNeeded(muleContext.getRegistry().lookupFlowConstruct("sseConsumer"));

        Set<MuleMessage> results = new HashSet<>();

        for (int i = 0; i < 2; ++i)
        {
            results.add(client.request("vm://out", RECEIVE_TIMEOUT));
        }

        assertThat(results, hasSize(2));

        assertThat(results.stream().filter(new Predicate<MuleMessage>()
        {
            @Override
            public boolean test(MuleMessage m)
            {
                return "test".equals(m.getPayload().toString());
            }
        }).count(), is(2L));
    }

    @Test
    public void lastEventIdProperlyHandledOnReconnections() throws Exception {
        sseServer.setEventsToSend(asList(
                ": test stream\r\n" + 
                "\r\n"));
        sseServer.start();
        startIfNeeded(muleContext.getRegistry().lookupFlowConstruct("sseConsumer"));

        new PollingProber(5000, 500).check(new JUnitProbe()
        {
            @Override
            protected boolean test() throws Exception
            {
                assertThat(sseServer.getLastRequest(), not(nullValue()));
                assertThat(sseServer.getLastRequest(), not(containsString(LAST_EVENT_ID_KEY)));
                return true;
            }
        });

        sseServer.stop();
        sseServer.resetLastRequest();
        sseServer.setEventsToSend(asList(
                "data: first event\r\n" + 
                "id: 1\r\n" + 
                "\r\n"));
        sseServer.start();

        new PollingProber(5000, 500).check(new JUnitProbe()
        {
            @Override
            protected boolean test() throws Exception
            {
                assertThat(sseServer.getLastRequest(), not(nullValue()));
                assertThat(sseServer.getLastRequest(), not(containsString(LAST_EVENT_ID_KEY)));
                return true;
            }
        });

        sseServer.stop();
        sseServer.resetLastRequest();
        sseServer.setEventsToSend(asList(
                        "data:second event\r\n" + 
                        "id\r\n" + 
                "\r\n"));
        sseServer.start();

        new PollingProber(5000, 500).check(new JUnitProbe()
        {
            @Override
            protected boolean test() throws Exception
            {
                assertThat(sseServer.getLastRequest(), not(nullValue()));
                assertThat(sseServer.getLastRequest(), containsString(LAST_EVENT_ID_KEY + ": 1"));
                return true;
            }
        });

        sseServer.stop();
        sseServer.resetLastRequest();
        sseServer.start();

        new PollingProber(5000, 500).check(new JUnitProbe()
        {
            @Override
            protected boolean test() throws Exception
            {
                assertThat(sseServer.getLastRequest(), not(nullValue()));
                assertThat(sseServer.getLastRequest(), not(containsString(LAST_EVENT_ID_KEY)));
                return true;
            }
        });
    }

    @Test
    public void defaultReconnectTimeIsHonored() throws Exception {
        sseServer.setEventsToSend(asList(
                "data: 1\r\n" + 
                "\r\n"));
        sseServer.start();
        startIfNeeded(muleContext.getRegistry().lookupFlowConstruct("sseConsumer"));

        new PollingProber(5000, 500).check(new JUnitProbe()
        {
            @Override
            protected boolean test() throws Exception
            {
                assertThat(sseServer.getLastRequest(), not(nullValue()));
                return true;
            }
        });
        long firstConnection = currentTimeMillis();

        sseServer.stop();
        sseServer.resetLastRequest();
        sseServer.setEventsToSend(asList(
                "data: 2\r\n" + 
                "\r\n"));
        sseServer.start();

        new PollingProber(5000, 500).check(new JUnitProbe()
        {
            @Override
            protected boolean test() throws Exception
            {
                assertThat(sseServer.getLastRequest(), not(nullValue()));
                return true;
            }
        });
        long secondConnection = currentTimeMillis();
        
        // Use some error margin to account for the polling time of the pollers used above
        assertThat(secondConnection - firstConnection, greaterThanOrEqualTo(3000L - 1000L));

    }

    @Test
    public void retryTimeReceivedIsHonored() throws Exception {
        sseServer.setEventsToSend(asList(
                "retry: 10000\r\n" + 
                "data: 1\r\n" + 
                "\r\n"));
        sseServer.start();
        startIfNeeded(muleContext.getRegistry().lookupFlowConstruct("sseConsumer"));

        new PollingProber(5000, 500).check(new JUnitProbe()
        {
            @Override
            protected boolean test() throws Exception
            {
                assertThat(sseServer.getLastRequest(), not(nullValue()));
                return true;
            }
        });
        long firstConnection = currentTimeMillis();

        sseServer.stop();
        sseServer.resetLastRequest();
        sseServer.setEventsToSend(asList(
                "data: 2\r\n" + 
                "\r\n"));
        sseServer.start();

        new PollingProber(15000, 500).check(new JUnitProbe()
        {
            @Override
            protected boolean test() throws Exception
            {
                assertThat(sseServer.getLastRequest(), not(nullValue()));
                return true;
            }
        });
        long secondConnection = currentTimeMillis();
        
        // Use some error margin to account for the polling time of the pollers used above
        assertThat(secondConnection - firstConnection, greaterThanOrEqualTo(10000L - 1000L));
    }

    @Test
    public void multipleBatchsSeparatedInTimeSameConnection() throws Exception {
        sseServer.setEventsToSend(asList(
                "data: DATA\r\n" + 
                "\r\n"));
        sseServer.setEventIntervalMillis(RECEIVE_TIMEOUT * 2);
        sseServer.start();
        startIfNeeded(muleContext.getRegistry().lookupFlowConstruct("sseConsumer"));

        assertThat(client.request("vm://out", RECEIVE_TIMEOUT), not(nullValue()));

        // Only after the configured interval in the server will the second
        // event be received
        assertThat(client.request("vm://out", RECEIVE_TIMEOUT), nullValue());
        assertThat(client.request("vm://out", RECEIVE_TIMEOUT * 2), not(nullValue()));
    }

    @Test
    public void movedPermanently() throws Exception
    {
        sseServerForRedirect.setResponseToSend(format(SSE_MOVED_RESPONSE, serverPort.getValue()));
        sseServerForRedirect.start();
        sseServer.setEventsToSend(asList(
                "data: DATA\r\n" + 
                "\r\n"));
        sseServer.start();
        startIfNeeded(muleContext.getRegistry().lookupFlowConstruct("sseConsumerRedirected"));

        MuleMessage request = client.request("vm://outRedirect", RECEIVE_TIMEOUT);
        assertThat(request, not(nullValue()));
        
        // TODO GHC does not provide a way to know a 301 redirection took place in order to update the url to use in next reconnections
    }

    @Test
    public void reconnectsAfterServerErrorCode() throws Exception
    {
        sseServer.setResponseToSend(SSE_ERROR_RESPONSE);
        sseServer.start();
        startIfNeeded(muleContext.getRegistry().lookupFlowConstruct("sseConsumer"));

        MuleMessage request = client.request("vm://out", 1000);
        assertThat(request, nullValue());

        sseServer.setResponseToSend(SSE_RESPONSE);
        sseServer.setEventsToSend(asList(
                "data: DATA\r\n" + 
                "\r\n"));

        request = client.request("vm://out", RECEIVE_TIMEOUT);
        assertThat(request, not(nullValue()));
    }

    @Test
    public void invalidContentType() throws Exception
    {
        sseServer.setResponseToSend("HTTP/1.1 200 OK\r\nCache-Control: no-cache\r\nContent-Type: text/xml\r\n\r\n");
        sseServer.setEventsToSend(asList(
                "data: DATA\r\n" + 
                "\r\n"));
        sseServer.start();
        startIfNeeded(muleContext.getRegistry().lookupFlowConstruct("sseConsumer"));

        MuleMessage request = client.request("vm://out", RECEIVE_TIMEOUT);
        assertThat(request, nullValue());
    }

    @Test
    public void justEmptyLines() throws Exception {
        sseServer.setEventsToSend(asList(
                "\r\n" + 
                "\r\n" + 
                "\r\n" + 
                "\r\n" + 
                "\r\n" + 
                "\r\n" + 
                "\r\n" + 
                "\r\n" + 
                "\r\n"));
        sseServer.start();
        startIfNeeded(muleContext.getRegistry().lookupFlowConstruct("sseConsumer"));

        MuleMessage request = client.request("vm://out", RECEIVE_TIMEOUT);
        assertThat(request, nullValue());
    }

    @Test
    public void randomInvalidBody() throws Exception
    {
        sseServer.setEventsToSend(asList(RandomStringUtils.random(1024)));
        sseServer.start();
        startIfNeeded(muleContext.getRegistry().lookupFlowConstruct("sseConsumer"));

        MuleMessage request = client.request("vm://out", RECEIVE_TIMEOUT);
        assertThat(request, nullValue());
    }

    @Test
    public void randomDataWithLineFeeds() throws Exception {
        sseServer.setEventsToSend(asList(
                RandomStringUtils.random(1024) + 
                "\r\n" + 
                "\r\n"));
        sseServer.start();
        startIfNeeded(muleContext.getRegistry().lookupFlowConstruct("sseConsumer"));

        MuleMessage request = client.request("vm://out", RECEIVE_TIMEOUT);
        assertThat(request, nullValue());
    }

    @Test
    public void basicMultilineEventThroughProxy() throws Exception
    {
        String data = "data: MULE\n" + 
                "data: +2\r" + 
                "data: 10\r\n" +
                "\r\n";
        sseBehindProxyServer.setResponseToSend(String.format("HTTP/1.1 200 OK\r\nCache-Control: no-cache\r\nContent-Type: text/event-stream\r\nContent-Length: %d\r\n\r\n", data.length()));
        sseBehindProxyServer.setEventsToSend(asList(
                data));
        sseBehindProxyServer.start();

        startIfNeeded(muleContext.getRegistry().lookupFlowConstruct("sseConsumerThroughProxy"));

        MuleMessage result = client.request("vm://outProxy", RECEIVE_TIMEOUT);

        assertThat(result.getPayload().toString(), is("MULE" + lineSeparator() + "+2" + lineSeparator() + "10"));
        assertThat("The message should only have the properties set by the vm endpoint (MULE_ENDPOINT and MULE_SESSION)",
                result.getInboundPropertyNames(), hasSize(2));
    }

    // oauth?

}
