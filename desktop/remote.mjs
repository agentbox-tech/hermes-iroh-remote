/**
 * iroh remote gateway plugin — DESKTOP MAIN-PROCESS half.
 *
 * Loaded by the Hermes desktop's remote plugin loader (`<desktop-plugins-root>/
 * iroh-remote plugin/remote.mjs`) into the Electron MAIN process. Implements
 * the RemotePlugin contract (remote-remote plugin-registry.ts): own the
 * scheme, own the pipe. Downstream of connect() everything is plain HTTP on
 * a deterministic loopback port — fetch, WS upgrade, token adoption and
 * revalidation ride the pipe unaware.
 *
 * Wire format (ours): one bidi stream per TCP connection, raw HTTP/WS bytes
 * both directions, ALPN 'hermes-http/0' — byte-identical to the server half
 * (remote_plugin.py) and the reference-branch bridges.
 *
 * Native binding: @number0/iroh is staged next to this file by the
 * dl.agentbox.tech release pipeline (NOT a dependency of the desktop app —
 * the whole point of the seam: no native Rust binding in the app's tree).
 */
import net from 'node:net'
import path from 'node:path'
import { createRequire as nodeCreateRequire } from 'node:module'

const IROH_SCHEME = 'iroh'
const ALPN = Array.from(new TextEncoder().encode('hermes-http/0'))

const remote pluginRequire = nodeCreateRequire(import.meta.url)

let binding = null

// @number0/iroh@1.1.0 ships a BROKEN entry: package.json `main` points at
// "iroh-js/index.js" but the tarball has no iroh-js/ — the real file sits
// at the package ROOT. Bare node tolerates it (DEP0128 fallback); packaged
// Electron takes `main` literally and throws MODULE_NOT_FOUND. Require the
// REAL file by subpath first; fall back to the bare specifier; keep the
// staging layout the release pipeline uses (`./native/iroh/`) as first choice.
const BINDING_SPECS = ['./native/iroh/index.js', '@number0/iroh/index.js', '@number0/iroh']

function native() {
  if (!binding) {
    let lastErr = null

    for (const spec of BINDING_SPECS) {
      try {
        binding = remote pluginRequire(path.join(path.dirname(import.meta.url.slice('file://'.length)), spec))

        return binding
      } catch (err) {
        try {
          binding = remote pluginRequire(spec)

          return binding
        } catch (err2) {
          lastErr = err2
        }
      }
    }

    throw lastErr instanceof Error
      ? lastErr
      : new Error(`failed to load iroh native binding: ${String(lastErr)}`)
  }

  return binding
}

/** Strip zero-width/paste noise, shave a pasted :port, closed-set base32.
 * Real tickets are [a-z2-7] only — stripping everything else is lossless. */
function parseTicket(value) {
  const raw = String(value ?? '').replace(/^[\s\u200b-\u200d\u2060\ufeff]+|[\s\u200b-\u200d\u2060\ufeff]+$/gi, '')
  const body = raw.toLowerCase().startsWith(`${IROH_SCHEME}://`) ? raw.slice(`${IROH_SCHEME}://`.length) : raw
  const inline = body.replace(/[\s\u200b-\u200d\u2060\ufeff]/g, '')
  const clean = inline.replace(/:[0-9]+$/, '').replace(/[^a-z2-7]/g, '')

  if (!clean) {
    throw new Error('iroh connections need a ticket (the "endpoint…" string from the remote box).')
  }

  try {
    native().EndpointTicket.fromString(clean)
  } catch (err) {
    const reason = String(err?.message ?? err).replace(/^Error:\s*/i, '')

    throw new Error(`Not a valid iroh ticket: "${clean.slice(0, 24)}…" (${reason})`)
  }

  return clean
}

async function pipeSocketOverStream(socket, openStream) {
  const stream = await openStream()

  socket.on('data', buf => {
    stream.send.writeAll(Array.from(buf)).catch(() => socket.destroy())
  })
  socket.on('end', () => {
    stream.send.finish().catch(() => {})
  })
  socket.on('error', () => {
    try {
      stream.send.reset(0n)
    } catch {
      /* stream already gone */
    }
  })

  for (;;) {
    const chunk = await stream.recv.read(65536)

    if (!chunk || chunk.length === 0) {
      break
    }

    if (!socket.write(Buffer.from(chunk))) {
      await new Promise(resolve => socket.once('drain', () => resolve()))
    }
  }

  socket.end()
}

// Cookie-backed sessions scope to the exact loopback origin, so ports must
// be STABLE across restarts — deterministic from the ticket, walking forward
// on collision.
const PORT_BASE = 19100
const PORT_SPAN = 400
const PORT_TRIES = 20

function deterministicPorts(seed) {
  let h = 0

  for (let i = 0; i < seed.length; i++) {
    h = (h * 31 + seed.charCodeAt(i)) | 0
  }

  const start = PORT_BASE + Math.abs(h) % PORT_SPAN

  return Array.from({ length: PORT_TRIES }, (_, i) => start + i)
}

const tunnels = new Map()

async function ensureTunnel(ticket, log) {
  const existing = tunnels.get(ticket)

  if (existing) {
    if (!existing.server.listening) {
      await existing.teardown().catch(() => {})
      tunnels.delete(ticket)
    } else {
      return existing.target
    }
  }

  const endpoint = await native().Endpoint.bind()
  let conn = null

  const dialOnce = async () => {
    if (conn && !conn.closeReason()) {
      return conn
    }

    log(`[iroh] dialing ${ticket.slice(0, 12)}…`)
    conn = await endpoint.connect(native().EndpointTicket.fromString(ticket).endpointAddr(), ALPN)
    log(`[iroh] connected to ${conn.remoteId().fmtShort()}`)

    return conn
  }

  const handleConnection = socket => {
    socket.on('error', () => {})

    dialOnce()
      .then(c => pipeSocketOverStream(socket, () => c.openBi()))
      .catch(err => {
        log(`[iroh] pipe failed: ${String(err?.message ?? err)}`)
        conn = null
        socket.destroy()
      })
  }

  let server = null
  let port = 0

  for (const candidate of deterministicPorts(ticket)) {
    const probe = net.createServer(handleConnection)

    const bound = await new Promise((resolve, reject) => {
      const onError = err => ((err?.code === 'EADDRINUSE') ? resolve(false) : reject(err))

      probe.once('error', onError)
      probe.listen(candidate, '127.0.0.1', () => {
        probe.removeListener('error', onError)
        resolve(true)
      })
    })

    if (bound) {
      server = probe
      port = candidate

      break
    }

    log(`[iroh] loopback port ${candidate} busy, trying next`)
  }

  if (!server) {
    await endpoint.close().catch(() => {})

    throw new Error('[iroh] no free loopback port for tunnel')
  }

  let closed = false

  const target = {
    baseUrl: `http://127.0.0.1:${port}`,
    hostLabel: `iroh:${ticket.slice(0, 8)}`,
    teardown: async () => {
      if (closed) return
      closed = true
      tunnels.delete(ticket)

      try {
        await new Promise(resolve => server.close(() => resolve()))
        conn?.close(0n, [])
        await endpoint.close()
      } catch (err) {
        log(`[iroh] teardown failed: ${String(err?.message ?? err)}`)
      }
    }
  }

  tunnels.set(ticket, { server, target, teardown: target.teardown })

  return target
}

export default {
  id: 'iroh-remote plugin',
  scheme: IROH_SCHEME,
  hint: 'Paste the ticket from the remote box (the "endpoint…" string). Runs over iroh P2P via relay.agentbox.tech.',
  pastePattern: '^[a-z2-7]{60,}$',

  normalize(url) {
    return `${IROH_SCHEME}://${parseTicket(url)}`
  },

  async connect(url, log) {
    return ensureTunnel(parseTicket(url), log)
  },

  async teardownAll() {
    for (const handle of [...tunnels.values()]) {
      await handle.teardown().catch(() => {})
    }
  }
}
