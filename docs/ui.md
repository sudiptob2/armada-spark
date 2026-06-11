# Spark Driver UI Access

## Direct Access

### Port-forward (simplest)

```bash
kubectl -n <namespace> port-forward <driver-pod-name> 4040:4040
```

Then open: `http://localhost:4040`

**Finding the pod:** Check Lookout UI for job details. Pod name is typically `armada-<job-id>-0`.

### Basic Ingress (no auth)

```bash
--conf spark.armada.driver.ui.ingress.enabled=true
```

**Warning:** Exposes UI publicly without authentication!

---

## OAuth2-Protected Access

See [`docs/oauth/`](./oauth/README.md) for setup, configuration, runtime flow, and troubleshooting.

---

## Resources

- [oauth2-proxy docs](https://oauth2-proxy.github.io/oauth2-proxy/)
- [Spark UI docs](https://spark.apache.org/docs/latest/web-ui.html)
