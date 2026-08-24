# Homebrew service update for Cairn 0.8.4

Cairn 0.8.4 moves macOS daemon tracing to the process-owned
`<data-dir>/daemon.log`. The application log rotates at 20 MiB and retains the
current file plus four archives. The daemon also holds
`<data-dir>/daemon.log.lock` so a second daemon falls back to stderr instead of
sharing the rotating writer.

The Homebrew service should therefore treat launchd stdout and stderr as small
supervisor fallbacks, not as the application log. Apply the following change in
`naoto256/homebrew-cairn` when publishing the 0.8.4 formula:

```diff
diff --git a/Formula/cairn.rb b/Formula/cairn.rb
@@
-    log_path var/"log/cairn-daemon.log"
-    error_log_path var/"log/cairn-daemon.log"
+    log_path var/"log/cairn-daemon.stdout.log"
+    error_log_path var/"log/cairn-daemon.stderr.log"
```

This separation prevents launchd from holding the same append-only inode for
both streams and keeps early startup, panic, and rotating-writer fallback output
available independently. Formula version and archive checksums should be
updated from the published 0.8.4 assets in the same Homebrew change.

The in-repository `contrib/cairn-daemon.plist` already uses distinct stdout and
stderr fallback paths, so it requires no service-sink change for 0.8.4.
