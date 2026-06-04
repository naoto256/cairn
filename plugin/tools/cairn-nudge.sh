#!/bin/sh

# Cairn PreToolUse nudge.
#
# Fires before every Bash invocation. When the cwd belongs to a
# cairn-registered repo AND the command starts with a code-search
# binary (grep / rg / ag / ack), block the bash and steer the agent
# toward the matching cairn tool. The block message names the
# replacement so the agent's next turn starts from the index instead
# of from raw text.
#
# Non-grep commands and non-registered cwds pass through silently.
# Any failure (jq missing, cairn missing, parse error) is a no-op —
# hooks must never break a turn.

if ! command -v jq >/dev/null 2>&1; then
  exit 0
fi
if ! command -v cairn >/dev/null 2>&1; then
  exit 0
fi

input="$(cat)"

cmd="$(printf '%s' "$input" | jq -r '.tool_input.command // empty' 2>/dev/null)"
[ -z "$cmd" ] && exit 0

cwd="$(printf '%s' "$input" | jq -r '.cwd // empty' 2>/dev/null)"
[ -z "$cwd" ] && cwd="$(pwd)"

# Strip leading whitespace and common prefixes (env=val, sudo, time)
# so we recognise grep even when wrapped.
prog="$(printf '%s' "$cmd" | sed -E 's/^[[:space:]]*//; s/^[A-Z_]+=[^ ]+ //; s/^(sudo|time|nice|ionice|nohup) +//' | awk '{print $1}')"
case "$prog" in
  grep|rg|ag|ack|egrep|fgrep) : ;;
  *) exit 0 ;;
esac

# Check whether cwd is a cairn-registered repo. The data-plane
# command is cheap and short-circuits on missing daemon / no repos.
repos="$(cairn query repos --json 2>/dev/null)" || exit 0
# Extract just the repo roots so we can prefix-match the cwd against them
# rather than substring-matching the whole JSON dump (which would false-
# positive on alias strings, branch names, etc.).
match="$(printf '%s' "$repos" | jq -r --arg cwd "$cwd" '
  .repos // []
  | map(.root)
  | map(select(. as $r | ($cwd == $r) or ($cwd | startswith($r + "/"))))
  | first // empty
' 2>/dev/null)" || exit 0
[ -z "$match" ] && exit 0

# Map the grep pattern to the closest cairn tool. Crude but cheap.
suggest="find_symbols"
hint="returns location + signature + kind directly without dragging file bodies into context"
case "$cmd" in
  *"^use "*|*"^import "*|*"^from "*)
    suggest="find_imports"
    hint='returns the dotted module + imported name + alias for every "use" statement, sourced from the syn semantic layer'
    ;;
  *"impl "*"for "*|*"impl "*"for'"*|*"impl "*'for"'*)
    suggest="find_impls"
    hint='returns every "impl Trait for Foo" block in the workspace, including cross-repo'
    ;;
  *"("*)
    suggest="find_references"
    hint="returns who calls a symbol (direction=incoming) or what a symbol calls (direction=outgoing), with enclosing function attributed"
    ;;
esac

reason="cwd is a cairn-registered repo. \`${suggest}\` is usually the better next call for this query — it ${hint}. \`grep\` is the right reach for free-form text inside symbol bodies or in files cairn does not understand."

# Both Claude Code and Codex accept the same non-blocking advisory
# shape on PreToolUse: hookSpecificOutput.additionalContext lets the
# tool run normally and surfaces the text to the agent's next-turn
# context. Returning no permissionDecision means "defer" (= use the
# normal permission flow), so the grep / rg / ack call executes
# unchanged.
jq -cn --arg reason "$reason" '{
  hookSpecificOutput: {
    hookEventName: "PreToolUse",
    additionalContext: $reason
  }
}'
