#!/usr/bin/env bash
# Generate encrypted PKCS#8 key pairs for the Snowflake service users into ./secrets (git-ignored).
# Prints the one-line public keys to paste into .env (AIRBYTE_/DBT_/BI_RSA_PUBLIC_KEY).
set -euo pipefail
mkdir -p secrets && chmod 700 secrets
for svc in airbyte dbt bi; do
  if [[ ! -f "secrets/svc_${svc}.p8" ]]; then
    openssl genrsa 2048 | openssl pkcs8 -topk8 -v2 aes256 -inform PEM -out "secrets/svc_${svc}.p8"
    openssl rsa -in "secrets/svc_${svc}.p8" -pubout -out "secrets/svc_${svc}.pub"
    chmod 600 "secrets/svc_${svc}.p8"
  fi
  echo "$(echo "$svc" | tr a-z A-Z)_RSA_PUBLIC_KEY=$(grep -v '^-----' "secrets/svc_${svc}.pub" | tr -d '\n')"
done
