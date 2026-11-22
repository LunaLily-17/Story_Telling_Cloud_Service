#!/usr/bin/env bash
set -euo pipefail

if kubectl -n storytelling get secret story-db --ignore-not-found -o name | grep -q .; then
  printf '%s\n' 'story-db already exists. Reusing its credentials and volume.'
  exit 0
fi

admin_password=$(openssl rand -hex 24)
app_password=$(openssl rand -hex 24)
printf 'admin-password=%s\napp-password=%s\ndatabase-url=postgresql+psycopg://story:%s@postgres:5432/story\n' \
  "$admin_password" "$app_password" "$app_password" |
  kubectl -n storytelling create secret generic story-db --from-env-file=/dev/stdin
