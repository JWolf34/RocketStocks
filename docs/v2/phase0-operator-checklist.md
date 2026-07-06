# Phase-0 operator checklist — home-lab VM evidence

> The four pre-flight boxes only the operator can verify (deploy target confirmed
> 2026-07-06: home-lab VM, Proxmox/ESXi class). Each item is yes/no with a line of
> evidence; tick the matching roadmap box when done. Estimates below are inputs, not
> the checks themselves.

## 1. Box capacity (roadmap: "Box capacity check")

Burn-in stack = everything running at once: v1 (Postgres + bot) **plus** v2
(TimescaleDB Postgres, `ingest`, `serve`, `bot`, reverse proxy, Prometheus/Grafana —
ADR-0015).

Working estimates (basis stated):

- **RAM:** v1 Postgres ~1 GB · TimescaleDB ~2 GB · three v2 services ~0.5 GB ·
  observability ~1 GB · OS/headroom ~1.5 GB → **VM ≥ 8 GB comfortable**.
- **Disk growth:** grounded in today's spike capture (single-name chain snapshot
  ≈ 420 KB gz; bounded SPY ≈ 475 KB): curated 5-min tier stores ATM±5/≤30-DTE *rows*,
  not raw chains (~400 underlyings × ~78 cycles/day); EOD full tier ~5–6k chains/night;
  1-min equity ~10k symbols/night. With Timescale compression, plan **~20–40 GB/month**
  conservatively → **≥ 250 GB free** covers burn-in plus the first few months.

- [ ] VM allocation recorded: ____ vCPU / ____ GB RAM / ____ GB disk free
- [ ] Allocation ≥ the estimates above (or the gap is stated and accepted)

## 2. Backup conditions (roadmap: "Backup conditions check", ADR-0031)

- [ ] **Block-level & atomic** — snapshots are hypervisor-level (e.g. Proxmox vzdump
      snapshot mode / ZFS snapshot), point-in-time, and cover **every virtual disk
      attached to the VM** in one atomic operation. A file-level copy/rsync inside the
      guest is NOT acceptable (torn Postgres files).
- [ ] **Off-host** — the snapshot/backup target lives on **different physical
      hardware** than the VM's disks (PBS or NAS on another box). A snapshot on the
      same ZFS pool/disk fails this condition — it dies with the hardware.
- [ ] **Cadence scheduled** — daily + weekly jobs actually exist (not intentions);
      retention noted.
- Evidence: hypervisor ____ · backup tool ____ · target host ____ · schedule ____

## 3. Ingress (roadmap: "Ingress check", ADR-0012 as amended)

- [ ] A **TLS-terminating reverse proxy** exists (or is committed) in front of the VM
      for `serve` — which: ____ (nginx / Caddy / Traefik / NPM) · hostname ____ ·
      cert method ____ (Let's Encrypt HTTP-01 / DNS-01)
- [ ] Only `serve`'s port is proxied; **Prometheus side-ports and `/metrics` are not
      forwarded** beyond the host/proxy.
- (G8's consumer test runs through the real proxy URL — that part is burn-in, not
  Phase 0; this box is about the proxy existing.)

## 4. Private dev channel (roadmap: "Private dev channel")

- [ ] Private Discord channel created for burn-in alert routing.
- [ ] Its ID recorded as **`RS_DEV_CHANNEL_ID`** (env-var name pinned now; consumed by
      S004's bot skeleton — record in the deploy box's `.env`, and in `.env.example`
      when S004 creates one).
