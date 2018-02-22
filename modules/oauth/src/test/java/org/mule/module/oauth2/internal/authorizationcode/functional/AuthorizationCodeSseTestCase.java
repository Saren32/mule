/*
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.module.oauth2.internal.authorizationcode.functional;

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.mule.api.lifecycle.LifecycleUtils.startIfNeeded;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mule.module.oauth2.internal.authorizationcode.state.ConfigOAuthContext;
import org.mule.module.oauth2.internal.authorizationcode.state.ResourceOwnerOAuthContext;
import org.mule.module.oauth2.internal.tokenmanager.TokenManagerConfig;
import org.mule.tck.junit4.rule.DynamicPort;
import org.mule.tck.probe.JUnitProbe;
import org.mule.tck.probe.PollingProber;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit.WireMockRule;

public class AuthorizationCodeSseTestCase extends AbstractAuthorizationCodeRefreshTokenConfigTestCase
{

    public static final String SINGLE_TENANT_OAUTH_CONFIG = "oauthConfig";
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Rule
    public DynamicPort serverPort = new DynamicPort("serverPort");

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().port(serverPort.getNumber()));

    @Override
    protected String getConfigFile()
    {
        return "authorization-code/authorization-code-sse-minimal-config.xml";
    }

    @Test
    public void sseRequestAuthenticated() throws Exception
    {
        final ConfigOAuthContext configOAuthContext = muleContext.getRegistry().<TokenManagerConfig>lookupObject("tokenManagerConfig").getConfigOAuthContext();
        final ResourceOwnerOAuthContext resourceOwnerOauthContext = configOAuthContext.getContextForResourceOwner(ResourceOwnerOAuthContext.DEFAULT_RESOURCE_OWNER_ID);
        resourceOwnerOauthContext.setAccessToken(ACCESS_TOKEN);
        resourceOwnerOauthContext.setRefreshToken(REFRESH_TOKEN);
        configOAuthContext.updateResourceOwnerOAuthContext(resourceOwnerOauthContext);

        startIfNeeded(muleContext.getRegistry().lookupFlowConstruct("testFlow"));
        
        new PollingProber(5000, 500).check(new JUnitProbe()
        {
            @Override
            protected boolean test() throws Exception
            {
                wireMockRule.verify(getRequestedFor(urlEqualTo("/sse"))
                        .withHeader("Authorization", equalTo("Bearer " + ACCESS_TOKEN)));
                return true;
            }
        });
    }
}
