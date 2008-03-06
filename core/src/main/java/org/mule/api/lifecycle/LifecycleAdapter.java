/*
 * $Id$
 * --------------------------------------------------------------------------------------
 * Copyright (c) MuleSource, Inc.  All rights reserved.  http://www.mulesource.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package org.mule.api.lifecycle;

import org.mule.api.interceptor.Interceptor;
import org.mule.api.service.Service;

/**
 * <code>LifecycleAdapter</code> is a wrapper around a pojo service that adds Lifecycle methods to the pojo. It also
 * associates the pojo service with its {@link Service} object.
 *
 * @see Service
 */
public interface LifecycleAdapter extends Lifecycle, Interceptor
{
    boolean isStarted();

    boolean isDisposed();

    Service getService();
}
