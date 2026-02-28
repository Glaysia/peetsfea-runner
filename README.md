# peetsfea-runner

Minimal 5% bootstrap of a daemon service for `.aedt` queue intake.

## Scope in this bootstrap
- Folder queue watcher (`inbox/staging/done/failed`)
- Persistent job state in DuckDB
- Daemon main loop with graceful shutdown (SIGTERM/SIGINT)
- systemd `--user` unit template

Out of scope for now:
- Remote SSH/Slurm dispatch
- HFSS analyze/export execution

## Run
```bash
python -m peetsfea_runner.main
```

The runtime directories are created automatically under `var/`:
- `var/inbox`
- `var/staging`
- `var/done`
- `var/failed`
- `var/runner.duckdb`

## systemd user service
Template file:
- `deploy/systemd/user/peetsfea-runner.service`

Typical flow:
```bash
mkdir -p ~/.config/systemd/user
cp deploy/systemd/user/peetsfea-runner.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now peetsfea-runner
journalctl --user -u peetsfea-runner -f
```
