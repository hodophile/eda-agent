# ADR: Event-Driven Multi-Agent Architecture using Apache Kafka

## Status
Proposed

## Context
We are building an AI agent system capable of complex reasoning, tool usage, and multi-agent collaboration. Traditional, synchronous agent implementations (monolithic Python scripts blocking on LLM API calls) suffer from several critical limitations:

1. **Brittleness:** A single API timeout or tool failure crashes the entire loop.
2. **Coupling:** The LLM "Brain" is tightly coupled to the "Hands" (tools), making it difficult to scale tools independently.
3. **Observability:** It is difficult to stream step-by-step progress to a UI asynchronously.
4. **Multi-Agent Coordination:** Synchronous frameworks struggle with parallel delegation, nested loops, and fault isolation when multiple agents interact.

We need an architecture that is resilient, decoupled, asynchronously observable, and naturally supports a transition from a single agent to a multi-agent hierarchy without requiring fundamental rewrites.

## Decision
We will adopt an event-driven architecture using Apache Kafka as the backbone, implementing a phased rollout. The core design relies on the **Orchestrator + State Store pattern**, where a stateless Orchestrator manages the control flow of an agent loop, and a fast Key-Value store (Redis) manages the ephemeral conversation state. 

The rollout will be executed in three distinct phases.

### Phase 1: Single Agent Event-Driven Loop
Establish the foundational ReAct (Reason + Act) loop for a single agent.

*   **Components:** 
    *   **Orchestrator:** State machine managing the loop logic (routing, state updates).
    *   **LLM Service:** Stateless consumer calling the LLM API.
    *   **Tool Services:** Stateless consumers executing specific actions (e.g., Web Search).
    *   **State Store (Redis):** Ephemeral scratchpad keyed by `task_id`.
*   **Topic Design:** Decoupled topics for distinct actions: `agent.tasks.in`, `agent.llm.requests`, `agent.llm.responses`, `agent.tool.requests`, `agent.tool.responses`, `agent.lifecycle.events`.
*   **Routing Strategy:** The Kafka Message Key is set to the `task_id`. This guarantees strict ordering of events for a specific loop, ensuring state isolation and preventing race conditions.
*   **Loop Mechanism:** The Orchestrator consumes tool responses, updates the Redis scratchpad, and produces a new prompt to the LLM request topic, effectively closing the asynchronous loop.

### Phase 2: Cross-Session State & Memory
Enable agents to remember users across sessions without exceeding LLM context limits.

*   **Components:**
    *   **State Archiver:** Consumer that moves ephemeral task data to persistent storage upon task completion.
    *   **Memory Compactor:** Consumer that summarizes past conversations and extracts semantic facts using a cheaper LLM.
    *   **Tiered Storage:** Ephemeral Scratchpad (Redis), Short-Term Memory (Postgres/Redis List), Long-Term Semantic Memory (Vector DB + Document DB).
*   **Routing Strategy Shift:** The Kafka Message Key shifts from `task_id` to `user_id`. This ensures all events (and subsequent memory compaction tasks) for a user are processed in strict chronological order.
*   **Memory Bootstrapping:** When a new task starts, the Orchestrator queries Short-Term and Long-Term memory to construct the initial System Prompt context before producing the first LLM request.

### Phase 3: Multi-Agent Delegation (Hierarchical)
Introduce multiple specialized agents (Supervisor + Specialists) without changing the core loop mechanics.

*   **Paradigm:** "Agents as Tools". A Supervisor agent delegates tasks by calling a specialist agent as if it were a tool.
*   **Topic Architecture:** Namespace isolation per agent (`agent.supervisor.*`, `agent.coder.*`, `agent.researcher.*`).
*   **Delegation Gateway:** A router consumer subscribes to specialist output topics (`agent.*.tasks.out`) and routes the final answers back into the Supervisor's `agent.supervisor.tool.responses` topic.
*   **Asynchronous Waiting:** When delegating, the Supervisor Orchestrator updates its state to `WAITING_FOR_SUB_AGENT` and pauses its loop. It resumes only when the Gateway routes the specialist's final observation back.
*   **Context Passing:** Specialists run isolated loops using their own Ephemeral Scratchpads. Only the final compressed summary is routed back to the Supervisor to preserve the Supervisor's context window.
*   **Zero-Downtime Agent Addition:** New agents are added seamlessly without restarting the Supervisor. The Orchestrator uses **Convention-based Routing** (e.g., LLM tool call `delegate_to_dbagent` dynamically maps to `agent.dbagent.tasks.in`) or a **Dynamic Tool Registry** (fetching available tools from a DB at task start), rather than hardcoded routing logic.

## Consequences

### Benefits
*   **Resilience:** If an LLM API or Tool Service crashes, the Kafka consumer offset remains. Another instance picks up the message, and the loop resumes using the State Store.
*   **Scalability:** Tool execution and LLM calls can be scaled independently via Kafka Consumer Groups.
*   **Asynchronous UI:** Frontends can subscribe to `agent.lifecycle.events` to stream real-time agent thoughts and actions without long-polling.
*   **Architectural Continuity:** The transition from single-agent to multi-agent requires zero fundamental changes to the loop mechanics. Adding a new agent is an operational deployment task, not an architectural redesign.

### Drawbacks
*   **Infrastructure Overhead:** Introduces Kafka, Redis, and potentially Vector DBs into the infrastructure stack, increasing operational complexity compared to a simple Python script.
*   **Latency:** The event-driven nature introduces network I/O latency between loop steps (Orchestrator -> Kafka -> LLM -> Kafka -> Orchestrator) compared to in-memory execution.
*   **Distributed Tracing:** Debugging a failed loop requires tracing `task_id` across multiple topics and services, necessitating robust distributed tracing implementation (e.g., OpenTelemetry).
