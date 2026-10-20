# Data API shape for LLM clients (issue #44)

Researched 2026-10-19 against primary sources only.

## Answer

Build one query layer and expose it through two thin adapters in the existing `serve` process: the REST routes that already exist, plus a small curated MCP server mounted at `/mcp` over Streamable HTTP.
REST+OpenAPI stays the contract and is free from FastAPI.
MCP is added because it is how Claude Code discovers and calls tools natively, and with one query layer it costs one module and one dependency.
Do not auto-generate MCP from OpenAPI, and do not use stdio or OAuth for this deployment.

## Comparison

### How LLM clients consume each

Claude Code connects to MCP servers over HTTP (recommended for remote servers), stdio, or deprecated SSE, and accepts static credentials as headers (`claude mcp add --transport http NAME URL --header "Authorization: Bearer ..."`) (https://code.claude.com/docs/en/mcp).
MCP tool definitions are deferred behind tool search by default, so only tool names and server instructions cost context until a tool is needed (https://code.claude.com/docs/en/mcp).
MCP resources are referenced with `@server:uri` and fetched as attachments (https://code.claude.com/docs/en/mcp).
I found no first-party doc describing OpenAPI import in Claude Code, so for REST it would call endpoints through shell tools; that is my inference, not a documented path.
Local scripts using the Anthropic SDK can wrap REST with hand-written tool schemas, or use the `anthropic[mcp]` helpers (`async_mcp_tool`, `mcp_resource_to_content`) against an MCP client session (https://github.com/anthropics/anthropic-sdk-python/blob/main/helpers.md).
The Messages API MCP connector is not usable here: it requires a publicly exposed HTTP server (https://platform.claude.com/docs/en/agents-and-tools/mcp-connector).
Claude Desktop and claude.ai custom connectors also connect from Anthropic's cloud and need public reachability, so a LAN server is unreachable that way (https://support.claude.com/en/articles/11175166-get-started-with-custom-connectors-using-remote-mcp).
The same page calls `claude_desktop_config.json` servers a separate, local mechanism; the official local-server guide shows only command-launched (stdio) entries (https://modelcontextprotocol.io/docs/develop/connect-local-servers).
Whether Desktop can take a LAN HTTP URL from that file is unverified (see Questions for John).

### Transports on a home-lab VM

With stdio the client launches the server as a subprocess and talks over stdin/stdout (https://modelcontextprotocol.io/specification/2025-11-25/basic/transports).
That would put DB credentials or a REST shim on each client machine, which fights the "one key per client, server on the VM" guardrail.
Streamable HTTP is the fit: one POST endpoint, JSON or SSE replies (https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http).
Revision 2026-07-28 removed protocol-level sessions and the GET stream, so a plain request/response server is stateless (same URL).
Claude Code's v2 runtime supports revision 2026-07-28 and falls back for older servers (https://code.claude.com/docs/en/mcp).
The spec requires servers to validate `Origin` (403 on mismatch) and says they SHOULD authenticate all connections (same transport URL).
The Python SDK answers 421 behind a real hostname until a `transport_security` allowlist is configured (https://py.sdk.modelcontextprotocol.io/run/asgi/).

### Auth on a LAN

MCP authorization is OPTIONAL; HTTP servers SHOULD follow the OAuth 2.1 flow, stdio SHOULD NOT (https://modelcontextprotocol.io/specification/2026-07-28/basic/authorization).
That flow needs protected-resource metadata and an authorization server, which is machinery the charter rules out.
The SDK's `TokenVerifier` accepts a static lookup, but still wants `AuthSettings` with an issuer URL and resource URL (https://py.sdk.modelcontextprotocol.io/run/authorization/).
Simplest fit: keep the existing `X-API-Key` / hashed `service_clients` check as ASGI middleware in front of both adapters, and have MCP clients send the key as a header.
This knowingly deviates from the MCP auth spec, which only matters to OAuth-discovering clients (claude.ai connectors), and those cannot reach the LAN anyway.
Note the existing middleware is `BaseHTTPMiddleware`, which the v2 audit already flags for cancellation edge cases (`docs/foundation/audit/v2-code-audit.md`); a pure-ASGI version is safer in front of SSE.

### Maintenance cost and package maturity

The official `mcp` SDK is at 2.3.0 (2026-10-02); v2.0.0 went stable 2026-07-28 and v1.x is security-fix-only (https://github.com/modelcontextprotocol/python-sdk/releases, https://pypi.org/project/mcp/).
v2 renamed `FastMCP` to `MCPServer` and moved transport options into `run()` (https://py.sdk.modelcontextprotocol.io/whats-new/).
So the SDK is three months into a major version and has churned recently; pin it and expect migration work.
Mounting into FastAPI is documented: build `streamable_http_app()`, enter `session_manager.run()` in the host lifespan, then `app.mount(...)` (https://py.sdk.modelcontextprotocol.io/run/asgi/).
`fastapi-mcp` last released 0.4.0 on 2025-07-28 (https://pypi.org/project/fastapi-mcp/), so it is effectively stale.
FastMCP (PrefectHQ) is at 4.1.1 (https://pypi.org/project/fastmcp/) and its own docs say `from_fastapi` is for "bootstrapping and prototyping, not for mirroring your API to LLM clients" (https://gofastmcp.com/integrations/fastapi).
Auto-conversion would also mirror REST shapes and any route we later add, which breaks "data, not features"; hand-curate 5 to 10 tools instead.
Claude Code warns when a tool result exceeds 10,000 tokens and caps it at 25,000 by default (https://code.claude.com/docs/en/mcp), so the query layer must enforce row caps and pagination; `serve` has no row cap today (`docs/foundation/audit/v2-code-audit.md`, serve section).
Both adapters together cost one extra module, one dependency and one lifespan hook.
REST alone costs nothing new but leaves Claude sessions without tool discovery.
MCP alone loses the free OpenAPI doc and easy curl debugging.

## Recommendation

Launch shape: keep `serve` FastAPI with `/v1/...` REST routes and add `/mcp` (Streamable HTTP, `MCPServer`, hand-written tools) behind the same API-key check, bound to the LAN interface only.
The seam is a query module in a lib (not in `serve`): plain async functions such as `get_bars(session, ticker, start, end, limit) -> list[Bar]` that take a session, accept typed arguments, return pydantic or domain models, enforce caps and pagination, and know nothing about HTTP, MCP or Discord.
The REST router, the MCP tool module and the bot each call these functions directly and in-process, so the bot never goes through the API, and tool docstrings and OpenAPI summaries are the only per-adapter content.
Each service gets its own DB role; `serve` gets SELECT on derived-data tables only (the audit shows the shared DSN currently lets `serve` read `schwab_tokens`).

## Open risks

- MCP SDK v2 is young and the spec just changed transport behavior; pin versions and budget a migration pass.
- A static-key MCP server is off-spec for OAuth-discovering clients; fine only because all clients are John's.
- DNS-rebinding and Origin checks must be configured, or the SDK returns 421 on the LAN hostname.
- ADR-0012 describes `serve` as internet-facing behind a TLS proxy; the charter now says LAN only.
- Plain HTTP on a LAN sends keys in cleartext; decide whether a self-signed or internal-CA cert is worth it.

## Questions for John

- Which LLM clients do you actually use: Claude Code only, Claude Desktop, local Anthropic-SDK scripts, others?
- If Claude Desktop matters, are you willing to run a small local stdio-to-HTTP bridge, since Desktop's cloud connectors cannot reach the LAN?
