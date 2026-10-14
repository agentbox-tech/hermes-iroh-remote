"""iroh remote gateway plugin — the accept/dial transport for Hermes.

Server half: ``serve()`` exposes the local gateway over iroh (persistent
identity key, live ticket file, accept-loop piping every bidi stream to the
gateway). ``connect()`` dials a saved ``iroh://<ticket>`` to a loopback
access point (headless CLI automation; the desktop remote plugin in
``desktop/remote.mjs`` is the interactive twin).

Wire format (remote plugin-internal): one bidi stream per TCP
connection, raw HTTP/WS bytes both directions, ALPN ``hermes-http/0`` —
byte-identical to the desktop remote plugin in ``desktop/remote.mjs``.

Requires the ``iroh`` Python bindings (pip iroh==1.1.0, glibc — musl hosts
dial from inside a glibc container). Registration is cheap and always succeeds;
missing bindings surface at serve()/connect() with an install hint.
"""

from __future__ import annotations

import asyncio
import os
import pathlib
from typing import Any

from gateway.remote_plugins import RemotePlugin, RemoteTarget, ServeHandle

ALPN = b"hermes-http/0"
READ_CHUNK = 64 * 1024


def _import_iroh():
    try:
        import iroh  # type: ignore

        return iroh
    except ImportError as exc:  # pragma: no cover - environment-dependent
        raise RuntimeError(
            "iroh-remote needs the iroh bindings: pip install iroh==1.1.0 "
            "(glibc; on musl hosts run inside the glibc container)"
        ) from exc


def load_secret_key(keyfile: pathlib.Path) -> bytes:
    if keyfile.exists():
        return keyfile.read_bytes()
    iroh = _import_iroh()
    sk = iroh.SecretKey.generate().to_bytes()
    keyfile.parent.mkdir(parents=True, exist_ok=True)
    keyfile.write_bytes(sk)
    keyfile.chmod(0o600)
    return sk


class IrohConnector(RemotePlugin):
    scheme = "iroh"
    hint = "Paste the ticket from the remote machine (the 'endpoint…' string), or pair by node id."
    paste_pattern = r"[a-z2-7]{60,}"

    def normalize(self, saved_url: str) -> str:
        """iroh://<ticket> canonical: invisible chars stripped, pasted
        :port shaved, closed-set base32 sanitize."""
        import re

        value = saved_url.strip().translate(str.maketrans("", "", "\u200b\u200c\u200d\u2060\ufeff"))

        if value.lower().startswith("iroh://"):
            body = value[len("iroh://"):]
        elif self.claims(value):
            body = value
        else:
            raise ValueError("iroh connections need a ticket (the 'endpoint…' string from the remote machine).")

        inline = body.replace(" ", "").replace("\t", "")
        shaved = re.sub(r":[0-9]+$", "", inline)
        clean = re.sub(r"[^a-z2-7]", "", shaved)

        if not clean:
            raise ValueError("iroh connections need a ticket (the 'endpoint…' string from the remote machine).")

        return f"iroh://{clean}"

    async def serve(self, gateway_url: str = "http://127.0.0.1:9119", config: dict[str, Any] | None = None) -> ServeHandle:
        iroh = _import_iroh()
        home = pathlib.Path(os.environ.get("HERMES_HOME", str(pathlib.Path.home() / ".hermes")))
        keyfile = home / "iroh-http.key"
        ticket_file = home / "iroh-ticket.txt"
        id_file = home / "iroh-endpoint-id.txt"

        host, _, port = gateway_url.rpartition("//")[2].rpartition(":")
        target_host = host or "127.0.0.1"
        target_port = int(port) if port.isdigit() else 9119

        builder = iroh.EndpointBuilder()
        builder.apply_n0()
        # Relays: n0 defaults from apply_n0 unless the plugin config
        # (plugins.entries.<id>.relays) points elsewhere. Custom relays
        # are assigned AFTER apply_n0 (custom-alone dies: "Missing or
        # incompatible rustls crypto provider").
        relays = (config or {}).get("relays")
        if relays:
            builder.relay_mode = iroh.RelayMode.custom_from_urls(relays)
        builder.alpns([ALPN])
        builder.secret_key(load_secret_key(keyfile))
        endpoint = await builder.bind()

        def write_ticket(addr) -> None:
            ticket_file.write_text(str(iroh.EndpointTicket.from_addr(addr)), encoding="utf-8")

        write_ticket(endpoint.addr())
        node_id = getattr(endpoint.addr(), "node_id", None)

        if node_id is not None:
            id_file.write_text(str(node_id), encoding="utf-8")

        # No uniffi AddrChangeCallback: it raises "no reactor running" from
        # inside watch_addr (and the bare-callable form TypeErrors — the
        # reference bridge ran static tickets because of it). A plain async
        # poll of endpoint.addr() gets live tickets with no FFI callbacks.
        async def ticket_loop() -> None:
            last = ""
            while True:
                try:
                    ticket = str(iroh.EndpointTicket.from_addr(endpoint.addr()))
                    if ticket and ticket != last:
                        ticket_file.write_text(ticket, encoding="utf-8")
                        last = ticket
                except Exception as exc:
                    print(f"[iroh-remote] ticket refresh failed: {exc}", flush=True)
                await asyncio.sleep(60)

        async def pipe_stream(stream) -> None:
            try:
                target_reader, target_writer = await asyncio.open_connection(target_host, target_port)
            except Exception as exc:
                print(f"[iroh-remote] pipe: cannot reach {target_host}:{target_port}: {exc}", flush=True)
                try:
                    await stream.send.reset(0)
                except Exception:
                    pass
                return

            rs, ss = stream.recv(), stream.send()

            async def iroh_to_tcp():
                try:
                    while True:
                        data = await rs.read(READ_CHUNK)
                        if not data:
                            break
                        target_writer.write(data)
                        await target_writer.drain()
                finally:
                    target_writer.close()
                    try:
                        await target_writer.wait_closed()
                    except Exception:
                        pass

            async def tcp_to_iroh():
                try:
                    while True:
                        data = await target_reader.read(READ_CHUNK)
                        if not data:
                            break
                        await ss.write(data)
                finally:
                    await ss.finish()

            await asyncio.gather(iroh_to_tcp(), tcp_to_iroh(), return_exceptions=True)

        async def accept_loop() -> None:
            while True:
                incoming = await endpoint.accept_next()
                if incoming is None:
                    return
                try:
                    accepted = await incoming.accept()
                    conn = await accepted.connect()
                except Exception as exc:
                    print(f"[iroh-remote] accept failed: {exc}", flush=True)
                    continue

                async def serve_conn(conn=conn) -> None:
                    while True:
                        try:
                            stream = await conn.accept_bi()
                        except Exception as exc:
                            print(f"[iroh-remote] connection closed: {exc}", flush=True)
                            return
                        asyncio.create_task(pipe_stream(stream))

                asyncio.create_task(serve_conn())

        loop_task = asyncio.create_task(accept_loop())
        tick_task = asyncio.create_task(ticket_loop())

        async def teardown() -> None:
            loop_task.cancel()
            tick_task.cancel()
            try:
                await endpoint.close()
            except Exception:
                pass

        return ServeHandle(ticket=(await asyncio.to_thread(ticket_file.read_text)).strip(),
                           target_url=gateway_url, teardown=teardown)

    async def connect(self, saved_url: str) -> RemoteTarget:
        """Dial a saved ``iroh://<ticket>`` to a loopback access point.

        One persistent QUIC connection; one bidi stream per accepted TCP
        connection (mirror of the serve side's accept-loop). Loopback port
        is deterministic from the ticket so cookie-scoped sessions survive
        restarts, walking forward on collision."""
        iroh = _import_iroh()
        ticket = self.normalize(saved_url)[len("iroh://"):]

        builder = iroh.EndpointBuilder()
        builder.apply_n0()
        endpoint = await builder.bind()

        try:
            addr = iroh.EndpointTicket.from_string(ticket).endpoint_addr()
            conn = await endpoint.connect(addr, ALPN)
        except Exception:
            await endpoint.close()
            raise

        async def handle_client(reader, writer) -> None:
            try:
                stream = await conn.open_bi()
            except Exception as exc:
                print(f"[iroh-remote] open_bi failed: {exc}", flush=True)
                writer.close()
                return

            rs, ss = stream.recv(), stream.send()

            async def iroh_to_tcp():
                try:
                    while True:
                        data = await rs.read(READ_CHUNK)
                        if not data:
                            break
                        writer.write(data)
                        await writer.drain()
                finally:
                    writer.close()
                    try:
                        await writer.wait_closed()
                    except Exception:
                        pass

            async def tcp_to_iroh():
                try:
                    while True:
                        data = await reader.read(READ_CHUNK)
                        if not data:
                            break
                        await ss.write(data)
                finally:
                    await ss.finish()

            await asyncio.gather(iroh_to_tcp(), tcp_to_iroh(), return_exceptions=True)

        server = None
        last_err = None

        for port in self._candidate_ports(ticket):
            try:
                server = await asyncio.start_server(handle_client, "127.0.0.1", port)
                break
            except OSError as exc:
                last_err = exc

        if server is None:
            await endpoint.close()
            raise RuntimeError(f"iroh-remote: no free loopback port ({last_err})")

        async def teardown() -> None:
            server.close()
            await server.wait_closed()
            try:
                conn.close(0, [])
            except Exception:
                pass
            await endpoint.close()

        return RemoteTarget(base_url=f"http://127.0.0.1:{port}",
                            teardown=teardown, host_label=f"iroh:{ticket[:8]}")

    @staticmethod
    def _candidate_ports(ticket: str) -> list[int]:
        h = 0
        for ch in ticket:
            h = (h * 31 + ord(ch)) & 0x7FFFFFFF
        start = 19100 + h % 400
        return [start + i for i in range(20)]


def register(ctx) -> None:
    ctx.register_remote_plugin(IrohConnector())
