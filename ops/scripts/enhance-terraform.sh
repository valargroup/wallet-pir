#!/usr/bin/env bash
# Run on the coordinator with its Infisical machine identity configured.
set -euo pipefail
if [[ ! -d /opt/enhance-pir/infra/production ]]; then
  echo "Run this wrapper on the Enhance coordinator." >&2
  exit 1
fi
# These variables expand in the child shell after Infisical injection.
# shellcheck disable=SC2016
exec flock -n /run/lock/enhance-production.lock \
  infisical run --env=prod --projectId=40862c6d-a089-4355-b405-0477be0ee3b1 -- \
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
