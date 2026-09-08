#!/usr/bin/env bash
# Validate relative Markdown links across the repository's documentation.
#
# Checks every `[text](target)` in the listed Markdown files: a relative target
# must exist on disk (file or directory), and a `#fragment` on a Markdown
# target must name a heading in that file. Absolute URLs are not fetched.
# Exits nonzero and lists every broken link, so `make check` fails before a
# deleted document leaves a dangling reference behind.
set -euo pipefail

cd "$(dirname "$0")/.."

files=()
while IFS= read -r f; do files+=("$f"); done < <(
  git ls-files --cached --others --exclude-standard -- '*.md' ':!docs/archive/**' ':!demos/**' ':!target/**' \
  | grep -v '/node_modules/' | sort
)

slug() {
  # GitHub-style heading anchor: lowercase, drop punctuation, spaces to dashes.
  printf '%s' "$1" \
    | tr '[:upper:]' '[:lower:]' \
    | sed -E 's/`//g; s/[^a-z0-9 _-]//g; s/ /-/g'
}

heading_anchors() {
  grep -E '^#{1,6} ' "$1" | sed -E 's/^#{1,6} +//' | while IFS= read -r h; do slug "$h"; done
}

broken=0
for file in "${files[@]}"; do
  dir=$(dirname "$file")
  # One link per line: strip images' leading '!' the same way, they still need a target.
  while IFS= read -r target; do
    [ -n "$target" ] || continue
    case "$target" in
      http://*|https://*|mailto:*) continue ;;
    esac
    path=${target%%#*}
    frag=""
    [[ "$target" == *#* ]] && frag=${target#*#}
    if [ -z "$path" ]; then
      resolved=$file
    else
      resolved="$dir/$path"
    fi
    if [ ! -e "$resolved" ]; then
      echo "$file: missing target '$target'"
      broken=$((broken + 1))
      continue
    fi
    if [ -n "$frag" ] && [[ "$resolved" == *.md ]]; then
      if ! heading_anchors "$resolved" | grep -qx "$frag"; then
        echo "$file: no heading '#$frag' in '$path'"
        broken=$((broken + 1))
      fi
    fi
  done < <(grep -oE '\]\([^)]+\)' "$file" | sed -E 's/^\]\(//; s/\)$//; s/ "[^"]*"$//' | sed -E 's/^<(.*)>$/\1/')
done

if [ "$broken" -ne 0 ]; then
  echo "check-doc-links: $broken broken link(s)" >&2
  exit 1
fi
echo "check-doc-links: ${#files[@]} files, no broken links"
