#!/bin/sh

# Cairn PreToolUse nudge.
#
# Fires before every Bash invocation. When the cwd belongs to a
# cairn-registered repo AND the command starts with a code-search
# binary (grep / rg / ag / ack / egrep / fgrep), let the bash run
# and emit a hookSpecificOutput.additionalContext advisory naming
# the matching cairn tool. The advisory surfaces in the agent's
# next-turn context so the *next* call defaults to the index; the
# current grep is not interrupted.
#
# Non-grep commands and non-registered cwds pass through silently.
# Any failure (jq missing, cairn missing, parse error) is a no-op —
# hooks must never break a turn.

[ "${CAIRN_NUDGE_DISABLED:-0}" = "1" ] && exit 0

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
  *"impl "*"for "*|*"impl "*"for'"*|*"impl "*'for"'*|*"extends "*|*"implements "*)
    suggest="find_subtypes / find_supertypes"
    hint='returns type-relation edges from either side — `find_subtypes` for "who implements / extends `name`" and `find_supertypes` for "what `name` extends / implements / mixes in"'
    ;;
  *"("*)
    suggest="find_callers / find_callees"
    hint='returns who calls a symbol (`find_callers`) or what a symbol calls (`find_callees`), with enclosing function attributed; reach for `find_references` for type / import / read / write / annotation refs'
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
