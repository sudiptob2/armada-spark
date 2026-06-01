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

import com.auth0.jwt.exceptions.JWTVerificationException;
import com.auth0.jwt.interfaces.DecodedJWT;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JwtAuthInterceptor implements ServerInterceptor {

    private static final Logger LOG = LoggerFactory.getLogger(JwtAuthInterceptor.class);

    private static final Metadata.Key<String> AUTH_KEY =
            Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER);
    private static final String BEARER_PREFIX = "Bearer ";

    private final JwtValidator validator;
    private final String owner;
    private final String userClaim;

    /** Reflection-friendly constructor. Reads owner + claim name from env. */
    public JwtAuthInterceptor() {
        String configuredOwner = System.getenv("SPARK_ARMADA_CONNECT_OWNER");
        if (configuredOwner == null || configuredOwner.isBlank()) {
            throw new IllegalStateException(
                    "SPARK_ARMADA_CONNECT_OWNER must be set to use JwtAuthInterceptor");
        }
        this.owner = configuredOwner;
        this.userClaim = envOrDefault("OIDC_USER_CLAIM", "sub");
        this.validator = new JwtValidator();
        LOG.info("JwtAuthInterceptor initialized: owner={}, userClaim={}", owner, userClaim);
    }

    /** Package-private constructor for tests. */
    JwtAuthInterceptor(JwtValidator validator, String owner, String userClaim) {
        this.validator = validator;
        this.owner = owner;
        this.userClaim = userClaim;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call,
            Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {

        String authHeader = headers.get(AUTH_KEY);
        if (authHeader == null) {
            return reject(call, Status.UNAUTHENTICATED, "Missing Authorization header");
        }
        String token = stripBearer(authHeader);
        if (token == null) {
            return reject(call, Status.UNAUTHENTICATED, "Authorization header is not a bearer token");
        }

        DecodedJWT jwt;
        try {
            jwt = validator.verify(token);
        } catch (JWTVerificationException e) {
            LOG.debug("JWT verification failed: {}", e.getMessage());
            return reject(call, Status.UNAUTHENTICATED, "JWT verification failed");
        }

        String identity = readIdentity(jwt);
        if (!owner.equals(identity)) {
            LOG.warn("Access denied: identity '{}' does not match owner '{}'", identity, owner);
            return reject(call, Status.PERMISSION_DENIED,
                    "Caller is not the owner of this Spark Connect server");
        }

        LOG.debug("Authenticated request from owner '{}'", identity);
        return next.startCall(call, headers);
    }

    private String readIdentity(DecodedJWT jwt) {
        if ("sub".equals(userClaim)) {
            return jwt.getSubject();
        }
        return jwt.getClaim(userClaim).asString();
    }

    private static String stripBearer(String header) {
        if (header.length() < BEARER_PREFIX.length()) return null;
        String prefix = header.substring(0, BEARER_PREFIX.length());
        if (!prefix.equalsIgnoreCase(BEARER_PREFIX)) return null;
        return header.substring(BEARER_PREFIX.length()).trim();
    }

    private static <ReqT, RespT> ServerCall.Listener<ReqT> reject(
            ServerCall<ReqT, RespT> call, Status status, String message) {
        call.close(status.withDescription(message), new Metadata());
        return new ServerCall.Listener<ReqT>() {};
    }

    private static String envOrDefault(String key, String defaultValue) {
        String val = System.getenv(key);
        return (val != null && !val.isBlank()) ? val : defaultValue;
    }
}
