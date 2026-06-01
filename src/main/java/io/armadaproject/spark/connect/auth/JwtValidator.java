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

import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.exceptions.JWTVerificationException;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public class JwtValidator {

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
}
