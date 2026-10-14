# iroh-remote plugin

A **remote gateway plugin** for Hermes.

`iroh` does what `https://` cannot: connects two machines with 
**no port-forwarding, no public URL, no DNS** — QUIC over relays,
`relay.agentbox.tech` by default (ours), n0 fallback.

## What lives where

```
hermes-iroh-remote plugin/
├── plugin.yaml              # manifest (provides_remote_plugins: [iroh])
├── remote_plugin.py           # SERVER half: hermes remote serve — bind endpoint,
│                          # publish ticket (~/.hermes/iroh-ticket.txt), accept
│                          # loops that pipe every bidi stream to the gateway
├── desktop/
│   └── remote.mjs      # DESKTOP MAIN-PROCESS half: loaded by the
│                          # remote plugin loader from
│                          # <desktop-plugins-root>/iroh-remote plugin/remote.mjs;
│                          # owns iroh:// end-to-end (ticket parse, dial,
│                          # deterministic loopback ports, teardown)
└── README.md
```

## Wire format (ours to own)

One bidi iroh stream per TCP connection; raw HTTP/WS bytes both directions;
ALPN `hermes-http/0`. Byte-identical between `remote_plugin.py` and
`desktop/remote.mjs`, and to the historical
`tools/iroh/iroh_bridge_{server,client}.py` — so an old reference-branch box
can talk to a new seam box during migration.

## Native binding

`@number0/iroh` is **not** a dependency of the desktop app (that was the
whole point of the seam). The release pipeline stages it next to
`remote.mjs` (or under `./native/iroh/`); the loader resolves spec-first
with the bare-specifier fallback — the published tarball's `main` field is
broken (`iroh-js/index.js` does not exist), bare node tolerates it via
DEP0128 fallback, packaged Electron does not.

Server half needs the Python bindings (`pip install iroh==1.1.0`, glibc —
musl hosts dial from inside the glibc container).

## Usage

Server side (any box running a gateway):

```bash
pip install iroh==1.1.0          # once
hermes remote serve              # prints ticket, keeps it live in ~/.hermes/iroh-ticket.txt
hermes remote list
```

Desktop side: install this plugin's `desktop/remote.mjs` into the
desktop-plugins root, restart the app, paste the ticket into the remote-setup
dialog. Downstream of the tunnel everything is plain HTTP on a deterministic
loopback port — the app never learns iroh exists.

## Status

- [x] `remote_plugin.py` — serve path implemented against the uniffi bindings;
      normalize/claims/registration smoke-tested (bindings not required for
      those).
- [x] **Server-side e2e proven on pxl (2026-10-13):** plugin serve() in
      the glibc container, untouched reference client dials the live
      ticket → dashboard HTML (200, 10KB) through the pipe in ~26ms.
      Wire-format compat with the reference bridges confirmed in the field.
- [ ] Desktop-side dial through the seam (`desktop/remote.mjs` loaded by
      the loader in the forked desktop build) — next gate.
- [ ] `connect()` server-half dialer (headless automation) — deferred,
      raises `NotImplementedError` honestly for now.

### Binding lessons (field, 2026-10-13)
- `Endpoint.watch_addr` never worked in Python: bare callable →
  `TypeError: expected AddrChangeCallback subclass`; a real subclass →
  `InternalError: there is no reactor running` from inside the FFI.
  The reference bridge silently ran on **static tickets** because of this.
  The remote plugin uses a plain async poll of `endpoint.addr()` instead —
  live tickets, no FFI callbacks.
