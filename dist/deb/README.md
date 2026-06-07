# Debian package — maintainer notes

`cargo deb` produces `cairn_<version>-1_amd64.deb` containing:

- `/usr/bin/cairn` — the multi-mode entry point
- `/usr/lib/systemd/user/cairn.service` — user-unit (see `systemd/cairn.service`)
- `/usr/share/doc/cairn/{README.md,LICENSE-APACHE,LICENSE-MIT}`

End-user install (after downloading from a release):

```sh
sudo apt install ./cairn_*.deb
```

Then enable the user service:

```sh
systemctl --user enable --now cairn
```

Generation runs in CI under `.github/workflows/release-binaries.yml` job `deb`
on every published GitHub Release.
