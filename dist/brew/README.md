# cairn Homebrew tap maintenance

> **Note**: this is the tap maintainer's reference. End users should run
> `brew tap naoto256/cairn`, then `brew install cairn` — see the top-level
> [README Installation section](../../README.md#installation).

The Homebrew tap repository `naoto256/homebrew-cairn` is the source of truth
for the live formula. This directory keeps maintainer notes and the
post-release checksum bump scaffold for that tap.

Do not keep a live `Formula/cairn.rb` in this repository.

## Supported release assets

The tap formula installs the matching release archive for each supported
target:

- `aarch64-apple-darwin`
- `x86_64-unknown-linux-gnu`

macOS x86_64 is intentionally not in the release matrix; users on that target
should build from source with `cargo install --git https://github.com/naoto256/cairn cairn`.

The formula should:

- set `version` to the published release version
- install the `cairn` binary into `bin`
- define a `service do` block that runs `cairn daemon`
- include caveats for first setup:
  `cairn ctl register-repo --alias <name> /path/to/repo`, then optionally
  `brew services start cairn` for daemon auto-start
- include the Claude Code plugin registration hint:
  `/plugin marketplace add naoto256/cairn`, then
  `/plugin install cairn@naoto256-cairn`

## Formula template

```ruby
class Cairn < Formula
  desc "Local, symbol-aware code index. Daemon-backed structural code search across registered repos"
  homepage "https://github.com/naoto256/cairn"
  version "0.1.0"
  license any_of: ["MIT", "Apache-2.0"]

  on_macos do
    if Hardware::CPU.arm?
      url "https://github.com/naoto256/cairn/releases/download/v#{version}/cairn-v#{version}-aarch64-apple-darwin.tar.gz"
      sha256 "0000000000000000000000000000000000000000000000000000000000000000"
    end
  end

  on_linux do
    if Hardware::CPU.intel?
      url "https://github.com/naoto256/cairn/releases/download/v#{version}/cairn-v#{version}-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "0000000000000000000000000000000000000000000000000000000000000000"
    end
  end

  def install
    bin.install "cairn"
    pkgshare.install "README.md", "LICENSE-APACHE", "LICENSE-MIT"
  end

  service do
    run [opt_bin/"cairn", "daemon"]
    keep_alive true
    log_path var/"log/cairn-daemon.log"
    error_log_path var/"log/cairn-daemon.log"
  end

  def caveats
    <<~EOS
      To register a repo with cairn:
        cairn ctl register-repo --alias <name> /path/to/repo

      To start the daemon automatically:
        brew services start cairn

      For the Claude Code plugin integration:
        /plugin marketplace add naoto256/cairn
        /plugin install cairn@naoto256-cairn
    EOS
  end

  test do
    assert_match "cairn #{version}", shell_output("#{bin}/cairn --version")
  end
end
```

## Post-release checksum bump

After a new release publishes its assets, run from the cairn repo:

```sh
dist/brew/scripts/bump-brew-formula.sh vX.Y.Z [tap-repo-dir]
```

The script clones `naoto256/homebrew-cairn` (or reuses the supplied
directory), updates `version` + `sha256` for each platform asset, commits the
change, pushes a branch, and opens a PR in the tap repo via `gh`.

Set `CAIRN_BREW_DRY_RUN=1` to update files without committing or opening a PR
— useful for verifying the script against a tag before merge.
