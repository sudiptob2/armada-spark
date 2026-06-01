/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.armadaproject.spark.connect.auth;

import com.auth0.jwk.JwkProvider;
import com.auth0.jwk.JwkProviderBuilder;
import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.JWTVerificationException;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.auth0.jwt.interfaces.RSAKeyProvider;
import com.auth0.jwt.interfaces.Verification;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class JwtValidator {

    private static final Logger LOG = LoggerFactory.getLogger(JwtValidator.class);

    private final JWTVerifier verifier;
    private final String issuerUrl;
    private final String audience;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Package-private constructor for tests: pass a fully-built verifier. */
    JwtValidator(JWTVerifier verifier, String issuerUrl, String audience) {
        this.verifier  = verifier;
        this.issuerUrl = issuerUrl;
        this.audience  = audience;
    }

    /** Reflection-friendly constructor. Reads all configuration from environment variables. */
    public JwtValidator() {
        String issuer = System.getenv("OIDC_ISSUER_URL");
        if (issuer == null || issuer.isBlank()) {
            throw new IllegalStateException("OIDC_ISSUER_URL must be set");
        }
        String jwksOverride = System.getenv("OIDC_JWKS_URL");
        String aud          = System.getenv("OIDC_AUDIENCE");

        String jwksUrl = resolveJwksUrl(issuer, jwksOverride);

        JwkProvider jwkProvider;
        try {
            jwkProvider = new JwkProviderBuilder(new URL(jwksUrl))
                    .cached(10, 24, TimeUnit.HOURS)
                    .rateLimited(10, 1, TimeUnit.MINUTES)
                    .build();
        } catch (MalformedURLException e) {
            throw new IllegalStateException("Invalid jwks URL: " + jwksUrl, e);
        }

        Algorithm algorithm = Algorithm.RSA256(new RSAKeyProviderFromJwks(jwkProvider));
        Verification builder = JWT.require(algorithm).withIssuer(issuer);
        if (aud != null && !aud.isBlank()) {
            builder.withAudience(aud);
        }

        this.verifier  = builder.build();
        this.issuerUrl = issuer;
        this.audience  = aud;
        LOG.info("JwtValidator initialized: issuer={}, jwks={}, audience={}",
                issuer, jwksUrl, aud == null ? "<none>" : aud);
    }

    public DecodedJWT verify(String token) throws JWTVerificationException {
        return verifier.verify(token);
    }

    public String issuerUrl() { return issuerUrl; }
    public String audience()  { return audience;  }

    /**
     * Resolve the JWKS endpoint URL. Returns {@code overrideJwksUrl} when set, otherwise
     * fetches {@code <issuer>/.well-known/openid-configuration} and reads its
     * {@code jwks_uri} field.
     */
    static String resolveJwksUrl(String issuerUrl, String overrideJwksUrl) {
        if (overrideJwksUrl != null && !overrideJwksUrl.isBlank()) {
            return overrideJwksUrl;
        }
        String discoveryUrl = trimTrailingSlash(issuerUrl) + "/.well-known/openid-configuration";
        try {
            HttpClient client = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(5))
                    .build();
            HttpRequest req = HttpRequest.newBuilder(URI.create(discoveryUrl))
                    .timeout(Duration.ofSeconds(10))
                    .header("Accept", "application/json")
                    .GET()
                    .build();
            HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() / 100 != 2) {
                throw new IllegalStateException(
                        "OIDC discovery returned HTTP " + resp.statusCode() + " from " + discoveryUrl);
            }
            return parseJwksUri(resp.body());
        } catch (IOException e) {
            throw new IllegalStateException("OIDC discovery failed for " + discoveryUrl, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("OIDC discovery interrupted for " + discoveryUrl, e);
        }
    }

    static String parseJwksUri(String discoveryJson) {
        try {
            JsonNode root = MAPPER.readTree(discoveryJson);
            JsonNode field = root.get("jwks_uri");
            if (field == null || !field.isTextual() || field.asText().isBlank()) {
                throw new IllegalStateException("OIDC discovery doc has no jwks_uri");
            }
            return field.asText();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to parse OIDC discovery doc", e);
        }
    }

    private static String trimTrailingSlash(String s) {
        return (s != null && s.endsWith("/")) ? s.substring(0, s.length() - 1) : s;
    }

    private static final class RSAKeyProviderFromJwks implements RSAKeyProvider {
        private final JwkProvider jwkProvider;
        RSAKeyProviderFromJwks(JwkProvider jwkProvider) {
            this.jwkProvider = jwkProvider;
        }

        @Override
        public RSAPublicKey getPublicKeyById(String keyId) {
            try {
                return (RSAPublicKey) jwkProvider.get(keyId).getPublicKey();
            } catch (Exception e) {
                throw new RuntimeException("Failed to fetch public key for kid=" + keyId, e);
            }
        }

        @Override public RSAPrivateKey getPrivateKey() { return null; }
        @Override public String getPrivateKeyId() { return null; }
    }
}
