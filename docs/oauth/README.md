# OAuth2 Authentication for the Spark Driver UI

When `spark.armada.oauth.enabled=true`, an oauth2-proxy sidecar runs inside the Spark driver pod. The Ingress routes to the proxy (port 4180), which authenticates users via OIDC and reverse-proxies to the Spark UI on `127.0.0.1:4040`.

## Quick start

```bash
/opt/spark/bin/spark-submit \
  --master armada://localhost:50051 \
  --deploy-mode cluster \
  --name my-secure-job \
  --class org.apache.spark.examples.SparkPi \
  --conf spark.armada.container.image=armada-spark \
  --conf spark.armada.driver.ui.ingress.enabled=true \
  --conf spark.armada.driver.ui.ingress.tls.enabled=true \
  --conf spark.armada.driver.ui.ingress.certName=my-tls-cert \
  --conf spark.armada.oauth.enabled=true \
  --conf spark.armada.oauth.clientId=spark-oauth-client \
  --conf spark.armada.oauth.clientSecret=your-secret \
  --conf spark.armada.oauth.issuerUrl=https://keycloak.example.com/realms/spark \
  local:///opt/spark/examples/jars/spark-examples.jar
```

See [Configuration](configuration.md) for variations (K8s secrets, manual endpoints, cookie hardening).

## Further reading

- [Architecture](architecture.md) — system design and component responsibilities
- [Runtime Flow](runtime-flow.md) — the OIDC redirect dance and session lifecycle
- [Networking](networking.md) — Service, Ingress, and port wiring
- [Configuration](configuration.md) — all `spark.armada.oauth.*` keys with defaults and examples
- [Troubleshooting](troubleshooting.md) — common failure modes
