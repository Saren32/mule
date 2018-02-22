/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.module.http.api.sse;

import org.mule.api.MuleException;
import org.mule.api.lifecycle.InitialisationException;
import org.mule.api.lifecycle.Lifecycle;
import org.mule.api.processor.MessageProcessor;
import org.mule.api.source.MessageSource;

public interface HttpServerSent extends MessageSource, Lifecycle
{

    @Override
    public void setListener(MessageProcessor listener);

    @Override
    public void initialise() throws InitialisationException;
    
    @Override
    public void start() throws MuleException;
    
    @Override
    public void stop() throws MuleException;

    @Override
    public void dispose();

}
