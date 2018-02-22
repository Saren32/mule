/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.module.http.functional;

import static java.lang.System.lineSeparator;
import static java.lang.Thread.interrupted;
import static java.util.Collections.emptyList;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of an HTTP server for testing purposes that just responds with
 * SSE data.
 * <p>
 * If stopped will close active threads and connectionHandlers.
 */
public class TestEventStreamServer
{
    private static final Logger logger = LoggerFactory.getLogger(TestEventStreamServer.class);

    public static final String SSE_MOVED_RESPONSE = "HTTP/1.1 301 Moved Permanently\r\nLocation: http://localhost:%s/redirected\r\n\r\n";
    public static final String SSE_ERROR_RESPONSE = "HTTP/1.1 500 Internal Server Error\r\n\r\n";
    public static final String SSE_RESPONSE = "HTTP/1.1 200 OK\r\nCache-Control: no-cache\r\nContent-Type: text/event-stream\r\n\r\n";

    private int listenPort;
    private ServerSocket serverSocket;
    private Thread serverThread;
    private String responseToSend = SSE_RESPONSE;
    ArrayList<Thread> connectionHandlers;
    
    private String lastRequest;

    private List<String> eventsToSend = emptyList();
    private int eventIntervalMillis = 5000;
    
    public TestEventStreamServer(int listenPort)
    {
        this.listenPort = listenPort;
        this.connectionHandlers = new ArrayList<>();
    }

    public void start() throws Exception
    {
        serverSocket = new ServerSocket(listenPort);

        serverThread = new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    while (true)
                    {
                        final Socket clientSocket = serverSocket.accept();
                        Thread handlerThread = new Thread(new Runnable()
                        {
                            @Override
                            public void run()
                            {
                                try
                                {
                                    handleRequest(clientSocket);
                                }
                                catch (Exception e)
                                {
                                    // ignore exception
                                }
                            }
                        });
                        connectionHandlers.add(handlerThread);
                        handlerThread.start();
                    }
                }
                catch (SocketException e)
                {
                    // stop execution when closed from parent
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
            }
        });

        serverThread.start();
    }

    public void stop() throws Exception
    {
        if (serverSocket != null)
        {
            serverSocket.close();
        }
        if (serverThread != null)
        {
            serverThread.join();
        }

        for (Thread c : connectionHandlers)
        {
            c.interrupt();
            c.join();
        }
    }

    private void handleRequest(final Socket clientSocket) throws Exception
    {
        BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()), 1);

        StringBuilder request = new StringBuilder();
        String header;
        do
        {
            header = reader.readLine().trim();
            request.append(header + lineSeparator());
        } while (!header.isEmpty());
        lastRequest = request.toString();

        OutputStream os = clientSocket.getOutputStream();
        os.write(responseToSend.getBytes());
        
        while(!eventsToSend.isEmpty())
        {
            for (String event : eventsToSend)
            {
                os.write((event).getBytes());
                os.flush();
                try
                {
                    Thread.sleep(eventIntervalMillis);
                }
                catch(InterruptedException e)
                {
                    os.close();
                    interrupted();
                    return;
                }
            }
        }
        os.flush();
        os.close();
    }

    public void setResponseToSend(String responseToSend)
    {
        this.responseToSend = responseToSend;
    }
    
    public void setEventsToSend(List<String> eventsToSend)
    {
        this.eventsToSend = eventsToSend;
    }
    
    public void setEventIntervalMillis(int eventIntervalMillis)
    {
        this.eventIntervalMillis = eventIntervalMillis;
    }
    
    public String getLastRequest()
    {
        return lastRequest;
    }
    
    public void resetLastRequest()
    {
        lastRequest = null;
    }
}
