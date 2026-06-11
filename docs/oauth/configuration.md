# Configuration Reference

All `spark.armada.oauth.*` keys, grouped. Source: [`Config.scala`](../../src/main/scala/org/apache/spark/deploy/armada/Config.scala) lines 477-710.

## Master switch

| Key                              | Type    | Default | oauth2-proxy flag |
|----------------------------------|---------|---------|-------------------|
| `spark.armada.oauth.enabled`     | Boolean | `false` | (gates all others)|

When `false`, no sidecar is built and the Spark UI is reachable directly through the Ingress (or via port-forward).

## Identity (required when enabled)

| Key                                       | Type   | Default          | oauth2-proxy flag                                |
|-------------------------------------------|--------|------------------|--------------------------------------------------|
| `spark.armada.oauth.clientId`             | String | (required)       | `--client-id`                                    |
| `spark.armada.oauth.clientSecret`         | String | (none)           | env: `OAUTH2_PROXY_CLIENT_SECRET` (inline)       |
| `spark.armada.oauth.clientSecretK8s`      | String | (none)           | env: `OAUTH2_PROXY_CLIENT_SECRET` (K8s secret)   |
| `spark.armada.oauth.clientSecretKey`      | String | `client-secret`  | (key within the K8s secret)                      |

Either `clientSecret` or `clientSecretK8s` is required. Prefer `clientSecretK8s` outside a sandbox: inline secrets are visible via `kubectl describe`.

## OIDC: discovery vs explicit endpoints

| Key                                                | Type    | Default          | oauth2-proxy flag                |
|----------------------------------------------------|---------|------------------|----------------------------------|
| `spark.armada.oauth.issuerUrl`                     | String  | (none)           | `--oidc-issuer-url`              |
| `spark.armada.oauth.skipProviderDiscovery`         | Boolean | `false`          | `--skip-oidc-discovery true`     |
| `spark.armada.oauth.loginUrl`                      | String  | (none)           | `--login-url`                    |
| `spark.armada.oauth.redeemUrl`                     | String  | (none)           | `--redeem-url`                   |
| `spark.armada.oauth.validateUrl`                   | String  | (none)           | `--validate-url`                 |
| `spark.armada.oauth.jwksUrl`                       | String  | (none)           | `--oidc-jwks-url`                |
| `spark.armada.oauth.redirectUrl`                   | String  | auto             | `--redirect-url`                 |
| `spark.armada.oauth.codeChallengeMethod`           | String  | `S256`           | `--code-challenge-method`        |
| `spark.armada.oauth.extraAudiences`                | String  | (none)           | `--oidc-extra-audience` (per token) |
| `spark.armada.oauth.providerDisplayName`           | String  | `OAuth Provider` | `--provider-display-name`        |
| `spark.armada.oauth.emailDomain`                   | String  | `*`              | `--email-domain`                 |

- **Discovery (default):** set `issuerUrl`. The proxy fetches `<issuer>/.well-known/openid-configuration` and discovers all endpoints.
- **Explicit (`skipProviderDiscovery=true`):** set all of `loginUrl`, `redeemUrl`, `validateUrl`, `jwksUrl`. Useful when the provider doesn't support discovery, or when the browser-facing endpoint differs from the cluster-internal ones.

`redirectUrl` must match what's registered with the OIDC client. Pin a stable hostname or set this explicitly. `extraAudiences` is comma-separated. `emailDomain=*` accepts any authenticated user; restrict it for production.

## Security knobs

| Key                                                          | Type    | Default | oauth2-proxy flag                          |
|--------------------------------------------------------------|---------|---------|--------------------------------------------|
| `spark.armada.oauth.skipVerify`                              | Boolean | `false` | `--ssl-insecure-skip-verify`               |
| `spark.armada.oauth.sslUpstreamInsecureSkipVerify`           | Boolean | `false` | `--ssl-upstream-insecure-skip-verify`      |
| `spark.armada.oauth.insecureSkipIssuerVerification`          | Boolean | `false` | `--insecure-oidc-skip-issuer-verification` |
| `spark.armada.oauth.insecureAllowUnverifiedEmail`            | Boolean | `false` | `--insecure-oidc-allow-unverified-email`   |
| `spark.armada.oauth.skipJwtBearerTokens`                     | Boolean | `false` | `--skip-jwt-bearer-tokens`                 |
| `spark.armada.oauth.skipProviderButton`                      | Boolean | `false` | `--skip-provider-button`                   |
| `spark.armada.oauth.skipAuthPreflight`                       | Boolean | `false` | `--skip-auth-preflight`                    |
| `spark.armada.oauth.passHostHeader`                          | Boolean | `false` | `--pass-host-header`                       |

Most of these have "insecure" or "skip" in the name for a reason: dev/test escape hatches.

## Cookies

| Key                                          | Type    | Default          | oauth2-proxy flag             |
|----------------------------------------------|---------|------------------|-------------------------------|
| `spark.armada.oauth.cookieName`              | String  | `_oauth2_proxy`  | `--cookie-name`               |
| `spark.armada.oauth.cookiePath`              | String  | `/`              | `--cookie-path`               |
| `spark.armada.oauth.cookieSecure`            | Boolean | `false`          | `--cookie-secure true`        |
| `spark.armada.oauth.cookieSamesite`          | String  | (none)           | `--cookie-samesite`           |
| `spark.armada.oauth.cookieCsrfPerRequest`    | Boolean | `false`          | `--cookie-csrf-per-request true` |
| `spark.armada.oauth.cookieCsrfExpire`        | String  | (none)           | `--cookie-csrf-expire`        |
| `spark.armada.oauth.whitelistDomain`         | String  | (none)           | `--whitelist-domain`          |

For production: `cookieSecure=true` (requires HTTPS at the Ingress) and `cookieSamesite=lax` or `strict`. `whitelistDomain` controls allowed redirect domains on `?rd=` (open-redirect protection).

## TLS for outbound calls (private CAs)

| Key                                          | Type   | Default | Effect                                                                |
|----------------------------------------------|--------|---------|-----------------------------------------------------------------------|
| `spark.armada.oauth.tls.caCertPath`          | String | (none)  | Mount path inside the sidecar (volume `ca-certificates`).             |
| `spark.armada.oauth.tls.caCertSubPath`       | String | (none)  | `subPath` of the `ca-certificates` volume.                            |
| `spark.armada.oauth.tls.caBundlePath`        | String | (none)  | E.g. `/etc/ssl/certs/ca-certificates.crt` (volume `ca-bundle`).       |
| `spark.armada.oauth.tls.caBundleSubPath`     | String | (none)  | `subPath` of the `ca-bundle` volume.                                  |

The volumes must already exist on the pod (typically supplied via a job template); the builder only adds the read-only mounts.

## Sidecar resources / image

| Key                                          | Type   | Default                                       | Effect                              |
|----------------------------------------------|--------|-----------------------------------------------|-------------------------------------|
| `spark.armada.oauth.proxy.image`             | String | `quay.io/oauth2-proxy/oauth2-proxy:v7.5.1`    | Container image.                    |
| `spark.armada.oauth.proxy.port`              | Int    | `4180`                                        | Listen port.                        |
| `spark.armada.oauth.resources.cpu`           | String | `100m`                                        | CPU request and limit (equal).      |
| `spark.armada.oauth.resources.memory`        | String | `128Mi`                                       | Memory request and limit (equal).   |

`requests == limits` is enforced by Armada.

## Examples

### Quick start with OIDC discovery

```
spark.armada.oauth.enabled=true
spark.armada.oauth.clientId=spark-oauth-client
spark.armada.oauth.clientSecretK8s=spark-oauth-secret
spark.armada.oauth.issuerUrl=https://keycloak.example.com/realms/spark
spark.armada.driver.ui.ingress.enabled=true
spark.armada.driver.ui.ingress.tls.enabled=true
spark.armada.driver.ui.ingress.certName=my-tls-cert
```

Plus the K8s secret:

```bash
kubectl create secret generic spark-oauth-secret -n spark-jobs \
  --from-literal=client-secret=YOUR_OAUTH2_CLIENT_SECRET
```

### Manual endpoints (no OIDC discovery)

For providers that don't expose `/.well-known/openid-configuration`, or when the browser-facing endpoint differs from the cluster-internal ones (e.g., the IdP sits behind a reverse proxy and the sidecar can short-circuit it via an internal Service):

```
spark.armada.oauth.enabled=true
spark.armada.oauth.clientId=spark-oauth-client
spark.armada.oauth.clientSecretK8s=spark-oauth-secret
spark.armada.oauth.skipProviderDiscovery=true
spark.armada.oauth.loginUrl=https://idp.example.com/auth
spark.armada.oauth.redeemUrl=http://idp.cluster.local/token
spark.armada.oauth.validateUrl=http://idp.cluster.local/userinfo
spark.armada.oauth.jwksUrl=http://idp.cluster.local/certs
spark.armada.driver.ui.ingress.enabled=true
```

**Pick the right vantage point per URL:**

- `loginUrl` and `redirectUrl` are **browser-facing**: must be reachable from the user, must be the URL registered with the OIDC client. Usually external (HTTPS, public DNS).
- `redeemUrl`, `validateUrl`, `jwksUrl` are **pod-facing**: called from inside the OAuth sidecar. Prefer cluster-internal Service URLs (faster, no public hop, plain HTTP is fine if the network is trusted).

All four explicit endpoint keys are required when `skipProviderDiscovery=true`; otherwise the sidecar fails at startup with `missing required setting: jwks-url`.

### Production hardening

```
spark.armada.oauth.enabled=true
spark.armada.oauth.clientId=spark-oauth-client
spark.armada.oauth.clientSecretK8s=spark-oauth-secret
spark.armada.oauth.issuerUrl=https://idp.corp.com/realms/spark
spark.armada.oauth.emailDomain=corp.com
spark.armada.oauth.cookieSecure=true
spark.armada.oauth.cookieSamesite=lax
spark.armada.oauth.cookieCsrfPerRequest=true
spark.armada.oauth.redirectUrl=https://spark-ui-pinned.corp.com/oauth2/callback
spark.armada.driver.ui.ingress.enabled=true
spark.armada.driver.ui.ingress.tls.enabled=true
spark.armada.driver.ui.ingress.certName=corp-tls-cert
```
