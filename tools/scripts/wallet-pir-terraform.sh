#!/usr/bin/env bash
# Run on the coordinator using its encrypted systemd runtime credential.
set -euo pipefail
umask 077
terraform_root=/opt/enhance-pir/infra/production
if [[ "${1:-}" == --root ]]; then
  if [[ $# -lt 3 || "$2" != /* || ! -f "$2/backend.tf" ]]; then
    echo "--root requires an absolute staged fleet root with backend.tf and a command." >&2
    exit 1
  fi
  terraform_root="$2"
  shift 2
fi
if [[ ! -d /opt/enhance-pir/infra/production ]]; then
  echo "Run this wrapper on the Enhance coordinator." >&2
  exit 1
fi
# These variables expand after runtime credential injection.
# shellcheck disable=SC2016
exec flock -n /run/lock/wallet-pir-production.lock \
  systemd-run --quiet --wait --pipe --collect --working-directory="$PWD" \
    --property=UMask=0077 \
    --property=LoadCredentialEncrypted=runtime:/etc/credstore.encrypted/enhance-pir-runtime \
    /usr/bin/python3 /opt/enhance-pir/ops/wallet-pir-runtime.py \
  bash -c '
    terraform_root="$1"
    shift
    export TF_VAR_digitalocean_token="$DO_TOKEN_NEW_ORG"
    export TF_VAR_cloudflare_api_token="$CF_API_TOKEN"
    export AWS_ACCESS_KEY_ID="$WALLET_PIR_TF_STATE_ACCESS_KEY"
    export AWS_SECRET_ACCESS_KEY="$WALLET_PIR_TF_STATE_SECRET_KEY"
    export AWS_EC2_METADATA_DISABLED=true
    if [[ "${1:-}" == plan ]]; then
      if [[ -f /srv/enhance-pir/autoscale/state.json ]]; then
        count="$(jq -er ".desired_groups" /srv/enhance-pir/autoscale/state.json)"
      else
        count="$(terraform -chdir="$terraform_root" output -json enhance_worker_groups | jq length)"
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
    exec terraform -chdir="$terraform_root" "$@"
  ' enhance-terraform "$terraform_root" "$@"
