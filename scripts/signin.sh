#!/bin/bash
#
# Get a JWT id_token from an OIDC IdP via device-code flow.
#
# Prints the id_token to stdout (status/prompts go to stderr) so it can be
# captured for use as a Spark Connect bearer token:
#
#     export SPARK_CONNECT_TOKEN="$(./scripts/signin.sh)"
#     ./scripts/runJupyter.sh -C
#
# Configuration is read from env (set in scripts/config.sh or your shell):
#   OIDC_ISSUER_URL       (required)  - e.g. http://192.168.2.12:5556/dex
#   OIDC_CLIENT_ID        (required)  - e.g. spark-ui
#   OIDC_CLIENT_SECRET    (required)  - the client secret
#   OIDC_SCOPES           (optional)  - default: "openid email profile"
#
set -euo pipefail

scripts="$(cd "$(dirname "$0")"; pwd)"
[ -f "$scripts/config.sh" ] && source "$scripts/config.sh"

err() { echo "$@" >&2; }

: "${OIDC_ISSUER_URL:?OIDC_ISSUER_URL must be set}"
: "${OIDC_CLIENT_ID:?OIDC_CLIENT_ID must be set}"
: "${OIDC_CLIENT_SECRET:?OIDC_CLIENT_SECRET must be set}"
OIDC_SCOPES="${OIDC_SCOPES:-openid email profile}"

err "Requesting device code from $OIDC_ISSUER_URL..."
RESP=$(curl -sS "$OIDC_ISSUER_URL/device/code" \
    -d "client_id=$OIDC_CLIENT_ID" \
    -d "client_secret=$OIDC_CLIENT_SECRET" \
    -d "scope=$OIDC_SCOPES")

# Extract fields. Use python3 so we don't depend on jq.
DEVICE_CODE=$(echo "$RESP" | python3 -c 'import json,sys;print(json.load(sys.stdin)["device_code"])')
USER_CODE=$(echo "$RESP"   | python3 -c 'import json,sys;print(json.load(sys.stdin)["user_code"])')
VERIFY_URL=$(echo "$RESP"  | python3 -c 'import json,sys;print(json.load(sys.stdin)["verification_uri_complete"])')
INTERVAL=$(echo "$RESP"    | python3 -c 'import json,sys;print(json.load(sys.stdin).get("interval",5))')

err ""
err "Open this URL in your browser:"
err "    $VERIFY_URL"
err "(if asked for a code, paste: $USER_CODE)"
err ""

# macOS convenience: auto-open in the default browser. Harmless if missing.
if command -v open >/dev/null 2>&1; then
    open "$VERIFY_URL" >/dev/null 2>&1 || true
elif command -v xdg-open >/dev/null 2>&1; then
    xdg-open "$VERIFY_URL" >/dev/null 2>&1 || true
fi

err "Polling for token..."
while true; do
    sleep "$INTERVAL"
    TR=$(curl -sS "$OIDC_ISSUER_URL/token" \
        -d "grant_type=urn:ietf:params:oauth:grant-type:device_code" \
        -d "device_code=$DEVICE_CODE" \
        -d "client_id=$OIDC_CLIENT_ID" \
        -d "client_secret=$OIDC_CLIENT_SECRET")
    ERROR=$(echo "$TR" | python3 -c 'import json,sys;print(json.load(sys.stdin).get("error",""))' 2>/dev/null || true)
    case "$ERROR" in
        authorization_pending|slow_down)
            err "  (waiting...)"
            continue
            ;;
        "")
            echo "$TR" | python3 -c 'import json,sys;print(json.load(sys.stdin)["id_token"])'
            err "Token acquired."
            exit 0
            ;;
        *)
            err "Token endpoint returned error: $ERROR"
            err "$TR"
            exit 1
            ;;
    esac
done
