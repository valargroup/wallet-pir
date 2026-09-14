#!/usr/bin/env bash
# Run on the coordinator using its encrypted systemd runtime credential.
set -euo pipefail
umask 077
if [[ ! -d /opt/enhance-pir/infra/production ]]; then
  echo "Run this wrapper on the Enhance coordinator." >&2
  exit 1
fi
# These variables expand after runtime credential injection.
# shellcheck disable=SC2016
exec flock -n /run/lock/enhance-production.lock \
  systemd-run --quiet --wait --pipe --collect --working-directory="$PWD" \
    --property=UMask=0077 \
    --property=LoadCredentialEncrypted=runtime:/etc/credstore.encrypted/enhance-pir-runtime \
    /usr/bin/python3 /opt/enhance-pir/ops/enhance-runtime.py \
  bash -c '
    export TF_VAR_digitalocean_token="$DO_TOKEN_NEW_ORG"
    export TF_VAR_cloudflare_api_token="$CF_API_TOKEN"
    export AWS_ACCESS_KEY_ID="$ENHANCE_TF_STATE_ACCESS_KEY"
    export AWS_SECRET_ACCESS_KEY="$ENHANCE_TF_STATE_SECRET_KEY"
    export AWS_EC2_METADATA_DISABLED=true
    if [[ "${1:-}" == plan ]]; then
      if [[ -f /srv/enhance-pir/autoscale/state.json ]]; then
        count="$(jq -er ".desired_groups" /srv/enhance-pir/autoscale/state.json)"
      else
        count="$(terraform -chdir=/opt/enhance-pir/infra/production output -json worker_groups | jq length)"
      fi
      [[ "$count" =~ ^[1-4]$ ]] || exit 1
      set -- "$@" -var-file=/etc/enhance-pir/production.tfvars -var="enhance_group_count=$count"
    elif [[ "${1:-}" == apply ]]; then
      if [[ $# != 2 || ! -f "$2" ]]; then
        echo "Apply requires exactly one reviewed saved plan file." >&2
        exit 1
      fi
      set -- apply -input=false "$2"
    fi
    exec terraform -chdir=/opt/enhance-pir/infra/production "$@"
  ' enhance-terraform "$@"
