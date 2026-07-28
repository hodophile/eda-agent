# EDA Agent — Phase 1: Single-Agent Event-Driven ReAct Loop

Proof of the architectural mechanics from [ADR 001](adr/001-arch-discussion.md) /
[ADR 002](adr/002-single-agent-eda.md): a complete **Think → Act → Observe → Think**
loop executed entirely asynchronously through **Kafka + Redis** with a
self-hosted LLM (**Ollama**).

```
                 agent.tasks.in
       test client ──────────────► ┌──────────────┐
                                  │              │  agent.llm.requests   ┌────────────┐
                                  │              │ ────────────────────► │ LLM service│
                                  │ Orchestrator │ ◄──────────────────── │  (Ollama)  │
   Redis (state) ◄───────────────►│  (state mch) │  agent.llm.responses  └────────────┘
   task:{task_id}                 │              │
                                  │              │  agent.tool.requests  ┌────────────┐
                                  │              │ ────────────────────► │Tool service│
                                  │              │ ◄──────────────────── │(Calculator)│
                                  └──────────────┘  agent.tool.responses └────────────┘
                       agent.lifecycle.events
   UI / watcher  ◄────────────────  (async progress stream)
```

## Layout

```
agent/
├── docker-compose.yml     Kafka (KRaft) + Redis + Ollama (+ optional Phase 2 stores)
├── pyproject.toml         uv project: deps for all three services in one venv
├── Makefile               convenience commands
├── common/                shared config, Kafka helpers, Pydantic schemas, ReAct prompt+parser
├── orchestrator/          FastAPI state machine + /agent/run + /agent/status/{id}
├── llm_service/           stateless OpenAI-compatible consumer (Ollama)
├── tool_service/          Calculator tool (safe AST eval) + extensible registry
├── test_client/           triggers the loop and polls for the answer
└── scripts/watch_events.py  tails agent.lifecycle.events
```

## Topics & message keys

| Topic                     | Direction                    | Producer        |
|---------------------------|------------------------------|-----------------|
| `agent.tasks.in`          | external → orchestrator      | test client     |
| `agent.llm.requests`      | orchestrator → llm service   | orchestrator    |
| `agent.llm.responses`     | llm service → orchestrator   | llm service     |
| `agent.tool.requests`     | orchestrator → tool service  | orchestrator    |
| `agent.tool.responses`    | tool service → orchestrator   | tool service    |
| `agent.lifecycle.events`  | orchestrator → UI/watcher     | orchestrator    |

The **Kafka message key is always `task_id`**, so every event for one loop lands
on the same partition and is consumed in strict order (no per-loop races).

## Quickstart

```bash
cp .env.example .env          # optional: override model, ports, etc.
make install                  # uv sync -> .venv
make up                       # Kafka + Redis + Ollama, create topics, pull llama3
                              # (first model pull is ~4-5 GB; grab a coffee)
```

In **three separate terminals** start the services (each auto-reloads):

```bash
make run-orchestrator         # :8000  state machine + HTTP API
make run-llm MODEL=gemma2:2b   # :8001  calls Ollama (or set OLLAMA_MODEL in .env)
make run-tool                 # :8002  runs the Calculator
```

> No model downloaded yet? You can still prove the full loop plumbing with a
> deterministic mock LLM (no GPU/model required): run `make run-mock` instead of
> `make run-llm`. It emits valid ReAct JSON so the orchestrator + tool + Kafka +
> Redis loop runs end-to-end.

Optional, in a 4th terminal — watch the live event stream:

```bash
make watch
```

Run the end-to-end test (in a 5th terminal):

```bash
make test
# -> task_id=...
#    ...THINKING (step 1)
#    ...ACTING
#    ...THINKING (step 2)
#    [COMPLETED] steps=2
#    Answer: 53
```

Or hit the API directly and poll:

```bash
curl -sXPOST localhost:8000/agent/run -H 'content-type: application/json' \
  -d '{"prompt":"What is 12 * 7?"}'          # -> {"task_id":"...","status":"PENDING"}
curl -s localhost:8000/agent/status/<task_id> | jq .
```

## Config

All defaults work on localhost. Override via `.env`:

| Var                     | Default                          | Used by           |
|-------------------------|----------------------------------|-------------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092`               | all services      |
| `REDIS_URL`             | `redis://localhost:6379/0`       | orchestrator      |
| `OLLAMA_BASE_URL`       | `http://localhost:11434/v1`      | llm service       |
| `OLLAMA_MODEL`          | `llama3`                         | llm service       |
| `MAX_STEPS`             | `10`                             | orchestrator      |

Swap the model (e.g. `make up MODEL=qwen2.5` + set `OLLAMA_MODEL=qwen2.5` in
`.env`) to try stronger tool-callers.

## Loop guarantees (from ADR 001)

- **Stateless workers** — the LLM and tool services carry no memory; the full
  history is re-sent on every turn, so any instance can be killed and replaced.
- **Crash-safe state** — every transition is written to Redis under
  `task:{task_id}` before producing the next event; if a worker dies, the
  uncommitted Kafka offset is redelivered and the loop resumes from Redis.
- **Async observability** — frontends subscribe to `agent.lifecycle.events`
  instead of long-polling (try `make watch`).
- **Bounded** — `MAX_STEPS` prevents runaway loops; unparseable LLM output is
  corrected and retried rather than crashing.

## Troubleshooting

- **`make test` shows `THINKING` forever / step count climbs** — the model isn't
  emitting valid JSON. llama3 works; for trickier models keep
  `response_format={"type":"json_object"}` (already set in the LLM service) and
  try `qwen2.5` or `llama3.1`.
- **`connection refused` on 9092/6379/11434** — `docker compose ps`, wait for
  healthchecks, and re-run `make up`.
- **`make test` returns a wrong answer like `50 + 3` instead of `53`** — the
  loop is fine; the model is just too small to stay disciplined. Tiny models
  (gemma2:2b) sometimes return the expression instead of computing it. Pull a
  stronger tool-following model (`ollama pull qwen2.5:7b` or `llama3`) and set
  `OLLAMA_MODEL` accordingly.
- **`make up` fails on the `ollama` service (port 11434 in use)** — you already
  have Ollama running on the host. Either use it (just `make run-llm
  MODEL=<installed>`) or change the `11434:11434` port mapping in
  `docker-compose.yml`.
- **Want a clean slate** — `make clean` (stops containers and wipes volumes).

## What's next (Phase 2 / 3)

Phase 2 adds cross-session memory (State Archiver + Memory Compactor, keying
shifts to `user_id`, adds Mongo + Qdrant — already in the compose under the
`phase2` profile). Phase 3 introduces supervisor/specialist delegation with
namespaced topics. Neither requires changing this loop mechanics.
