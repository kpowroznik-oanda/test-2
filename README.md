# Task 2: Code review

**Time: 20 minutes. No AI assistants.**

A teammate opened a PR with `src/price-broadcaster.ts`: a small service that
fans out prices from one upstream feed to many SSE clients (web and mobile apps,
thousands of concurrent connections, frequent reconnects on flaky mobile networks).

The service tracks each connection as a "session" with a periodic heartbeat
(to keep the SSE connection alive through proxies) and a periodic sweep that is
supposed to drop sessions that have gone idle.

The HTTP layer is out of scope on purpose — `connect()` takes an already-parsed
instrument list and a way to register a disconnect callback, so you can focus on
`PriceBroadcaster` itself.

Review it as if it were about to be deployed to production.

- Think out loud. Tell us what you'd flag, why it matters, and how you'd fix it.
- Prioritise: what would you **block the PR** on, and what's a nit?
- You may write notes in `REVIEW.md` or as comments in the code, but talking is enough.

The code compiles (`npm run typecheck`). There are no tests and nothing to run.
