/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.module.http.internal.request;

public class SuccessStatusCodeValidator extends RangeStatusCodeValidator
{

    /**
     * An status code validator that allows any status code.
     */
    public static SuccessStatusCodeValidator NULL_VALIDATOR = new SuccessStatusCodeValidator("0..599");

    public SuccessStatusCodeValidator()
    {
    }

    public SuccessStatusCodeValidator(String values)
    {
        setValues(values);
    }

    @Override
    public boolean isValid(int value)
    {
        return belongs(value);
    }
}
