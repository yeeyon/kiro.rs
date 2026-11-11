#!/bin/sh
# usage: KIRO_URL=http://127.0.0.1:8990 KIRO_KEY=<apiKey> ADMIN_KEY=<adminApiKey> MODEL=<model> sh live-probe.sh [rounds]
# Sends the same short prompt through all four inference APIs (stream + non-stream),
# then prints the timing summary of each request from /api/admin/traces.
set -eu
url="${KIRO_URL:-http://127.0.0.1:8990}"
rounds="${1:-3}"
prompt='Write exactly five short sentences about the sea.'
msg="{\"model\":\"$MODEL\",\"max_tokens\":400,\"messages\":[{\"role\":\"user\",\"content\":\"$prompt\"}]"
chat="{\"model\":\"$MODEL\",\"messages\":[{\"role\":\"user\",\"content\":\"$prompt\"}]"
resp="{\"model\":\"$MODEL\",\"input\":\"$prompt\""

post() { # path body
  curl -sS -N -o /dev/null -w "%{http_code} first_byte=%{time_starttransfer}s total=%{time_total}s\n" \
    -H "x-api-key: $KIRO_KEY" -H "authorization: Bearer $KIRO_KEY" -H 'content-type: application/json' \
    -H 'anthropic-version: 2023-06-01' "$url$1" -d "$2"
}

i=1
while [ "$i" -le "$rounds" ]; do
  for s in true false; do
    printf 'round %s stream=%s messages    ' "$i" "$s"; post /v1/messages "$msg,\"stream\":$s}"
    printf 'round %s stream=%s cc          ' "$i" "$s"; post /cc/v1/messages "$msg,\"stream\":$s}"
    printf 'round %s stream=%s chat        ' "$i" "$s"; post /v1/chat/completions "$chat,\"stream\":$s}"
    printf 'round %s stream=%s responses   ' "$i" "$s"; post /v1/responses "$resp,\"stream\":$s}"
  done
  i=$((i + 1))
done

n=$((rounds * 8))
curl -sS -H "x-api-key: $ADMIN_KEY" "$url/api/admin/traces?limit=$n" | jq -r '
  .records[] |
  [ .endpoint, (.isStream|tostring), .finalStatus, .outputTokensSource,
    (.timing.phases.upstreamHeadersUs // 0 | ./1000 | floor),
    (.timing.phases.firstEmitTextUs // 0 | ./1000 | floor),
    (.timing.spans.refreshLockWait.totalUs // 0 | ./1000 | floor),
    (.timing.spans.tokenRefresh.totalUs // 0 | ./1000 | floor),
    (.throughput | tostring) ] | @tsv' |
  (echo "endpoint	stream	status	tokens	headers_ms	first_text_ms	lock_wait_ms	refresh_ms	throughput"; cat)
