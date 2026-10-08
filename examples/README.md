# Examples

Each example is a Python Worker (`src/entry.py`) with a React page (`app.jsx`) built by Vite into `dist/`. The Worker serves the page as static assets and sends `/agents/*` requests to the agent.

| Example | What it shows |
| --- | --- |
| [counter](./counter) | Shared state and a `@callable` method. Every open tab sees the same count. |
| [chat](./chat) | An `AIChatAgent` that streams replies from Workers AI to `useAgentChat`. |
| [reminders](./reminders) | `schedule()` runs a method 5 seconds later, and the agent updates its state. |

## Run one

You need [uv](https://docs.astral.sh/uv/) and Node.js.

```sh
cd counter
npm install
npm run dev     # builds the page, then starts the Worker on http://localhost:8787
```

`npm run deploy` deploys the example to your Cloudflare account.

The chat example calls Workers AI, which runs on Cloudflare even in local dev. Log in first with `npx wrangler login`.

The chat example installs `cf-agents` from PyPI. The counter and reminders examples install the SDK from this repository (`../..`). After you change the SDK, run `touch pyproject.toml` in one of those examples and restart `npm run dev`. pywrangler copies the SDK in again only when `pyproject.toml` changes.
