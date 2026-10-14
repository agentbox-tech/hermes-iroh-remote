# iroh-remote

A **remote gateway plugin** for Hermes.

`iroh` does what `https://` cannot: connects two machines with
**no port-forwarding, no public URL, no DNS** — QUIC over relays,
n0's public relays by default; point your own at
`plugins.entries.iroh-remote.relays` if you run one.

## What lives where

```
hermes-iroh-remote/
├── plugin.yaml              # manifest (provides_remote_plugins: [iroh])
├── remote_plugin.py       # SERVER half: hermes remote serve — bind endpoint,
│                          # publish ticket (~/.hermes/iroh-ticket.txt), accept
│                          # loops that pipe every bidi stream to the gateway
├── desktop/
│   └── remote.mjs         # DESKTOP MAIN-PROCESS half: loaded by the
│                          # remote plugin loader from
│                          # <desktop-plugins-root>/iroh-remote/remote.mjs;
│                          # owns iroh:// end-to-end (ticket parse, dial,
│                          # deterministic loopback ports, teardown)
└── README.md
```

## Wire format

One bidi iroh stream per TCP connection; raw HTTP/WS bytes both directions;
ALPN `hermes-http/0`. Byte-identical between `remote_plugin.py` and
`desktop/remote.mjs`.

## Native binding

`@number0/iroh` is **not** a dependency of the desktop app (that's the
point of the seam). The plugin's release pipeline stages it next to
`remote.mjs` (or under `./native/iroh/`); the loader resolves spec-first
with the bare-specifier fallback — the published tarball's `main` field is
broken (`iroh-js/index.js` does not exist), bare node tolerates it via
DEP0128 fallback, packaged Electron does not.

Server half needs the Python bindings (`pip install iroh==1.1.0`, glibc —
musl hosts dial from inside a glibc container).

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

- [x] `remote_plugin.py` — serve path implemented against the uniffi
      bindings; normalize/claims/registration smoke-tested (bindings not
      required for those).
- [x] **Server-side e2e proven on an arm64 gateway host:** plugin serve()
      in a glibc container, untouched reference client dials the live
      ticket → dashboard HTML (200, 10KB) through the pipe in ~26 ms.
- [ ] Desktop-side dial through the seam (`desktop/remote.mjs` loaded by
      the loader in the forked desktop build) — next gate.
- [ ] `connect()` server-half dialer (headless automation) — deferred,
      raises `NotImplementedError` honestly for now.

### Binding lessons (field notes)

- `Endpoint.watch_addr` never worked in Python: bare callable →
  `TypeError: expected AddrChangeCallback subclass`; a real subclass →
  `InternalError: there is no reactor running` from inside the FFI.
  A naive implementation silently runs on **static tickets** because of
  this. This plugin uses a plain async poll of `endpoint.addr()`
  instead — live tickets, no FFI callbacks.
