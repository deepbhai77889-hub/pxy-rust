#!/usr/bin/env bash
# Quick helper to get the latest active 24/7 Rust Gateway URL

REPO="deepbhai77889-hub/pxy-rust"
URL=$(gh api "repos/$REPO/commits/$(gh api repos/$REPO/commits/main --jq '.sha')/statuses" --jq '.[] | select(.context=="gateway/live-url") | .target_url' 2>/dev/null | head -n 1)

if [ -z "$URL" ] || [ "$URL" = "null" ]; then
  # Fallback to run log summary
  RUN_ID=$(gh run list --repo "$REPO" --workflow="deploy.yml" --limit 1 --json databaseId --jq '.[0].databaseId')
  URL=$(gh run view "$RUN_ID" --repo "$REPO" --log 2>&1 | grep -oE "https://[a-zA-Z0-9-]+\.trycloudflare\.com" | tail -n 1)
fi

if [ -n "$URL" ]; then
  echo "🚀 Current Active Gateway URL: $URL"
  echo "OpenAI Endpoint:    $URL/v1/chat/completions"
  echo "Anthropic Endpoint: $URL/v1/messages"
  echo "Models List:        $URL/v1/models"
else
  echo "⚠️ Unable to fetch live URL. Check workflow status on GitHub Actions."
fi
