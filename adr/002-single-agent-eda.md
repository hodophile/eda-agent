```markdown
# Phase 1 Action Plan: Single Agent Event-Driven Loop

**Objective:** Prove the architectural mechanics. Successfully execute a ReAct loop (Think -> Act -> Observe -> Think) entirely asynchronously via Kafka and Redis, using a self-hosted LLM (Ollama/vLLM).

---

## Step 0: Infrastructure Bootstrapping
*Goal: Stand up the local backbone and verify connectivity.*

1.  **Create Project Structure:**
    ```text
    agent-system/
    ├── docker-compose.yml      # KRaft Kafka, Redis, Ollama setup
    ├── orchestrator/           # FastAPI + aiokafka service
    ├── llm_service/           # FastAPI + aiokafka service
    ├── tool_service/          # FastAPI + aiokafka service
    └── test_client/           # Simple script to trigger the loop
    ```
2.  **Launch Infrastructure:** Use the KRaft `docker-compose.yml` (containing Kafka, Redis, Ollama, MongoDB, Qdrant). Run `docker-compose up -d`.
3.  **Bootstrap the LLM:** Access Ollama and pull a model capable of tool calling (e.g., Llama3 or Mistral).
    ```bash
    docker exec -it ollama ollama pull llama3
    ```
4.  **Verify Stack:** Check that Kafka (`localhost:9092`), Redis (`localhost:6379`), and Ollama (`localhost:11434`) are responding.

---

## Step 1: The LLM Service (The Brain)
*Goal: Build a dumb, stateless service that translates Kafka prompts into LLM API calls.*

1.  **Setup Python Environment:** Inside the `llm_service` folder, setup `requirements.txt` (`fastapi`, `uvicorn`, `aiokafka`, `openai`). *Note: We use the `openai` library because Ollama exposes an OpenAI-compatible endpoint, making it easy to swap to vLLM/OpenAI later.*
2.  **Write the Consumer:**
    *   Create an `aiokafka` consumer subscribed to `agent.llm.requests`.
    *   Create an `aiokafka` producer.
    *   **CRITICAL:** Always set the Kafka Message Key to the `task_id` extracted from the payload.
3.  **Implement LLM Call:**
    *   Read the prompt from the consumed message.
    *   Initialize `AsyncOpenAI(base_url="http://localhost:11434/v1", api_key="ollama")`.
    *   Call `await client.chat.completions.create(...)`.
4.  **Produce Response:**
    *   Take the LLM's raw output (which might be a final text answer OR a JSON tool call).
    *   Produce it to `agent.llm.responses`.
5.  **Verify:** Manually produce a dummy JSON message to `agent.llm.requests` using a Kafka CLI tool (like `kcat` or Confluent Control Center). Confirm the LLM service picks it up, calls Ollama, and drops a response in `agent.llm.responses`.

---

## Step 2: The Tool Service (The Hands)
*Goal: Build a stateless service that executes a specific tool and returns the observation.*

1.  **Setup Python Environment:** Inside `tool_service`, setup similar dependencies.
2.  **Define a Mock Tool:** Don't build a complex web scraper yet. Build a simple "Calculator" tool that takes an expression (e.g., `2 + 2`) and returns `4`. This isolates infrastructure bugs from tool logic bugs.
3.  **Write the Consumer/Producer:**
    *   Subscribe to `agent.tool.requests`.
    *   Filter messages (or route via Kafka headers) for the "Calculator" tool.
4.  **Execute & Respond:**
    *   Parse the arguments from the message.
    *   Execute the mock calculation.
    *   Produce the result (Observation) to `agent.tool.responses` (Key = `task_id`).
5.  **Verify:** Manually produce a calculator tool request to `agent.tool.requests`. Confirm the tool service picks it up, calculates the result, and drops it in `agent.tool.responses`.

---

## Step 3: The Orchestrator (The State Machine)
*Goal: Build the central routing hub that connects the Brain and Hands via Redis state.*

1.  **Setup FastAPI + aiokafka + Redis:** Inside `orchestrator`, add `redis[hiredis]` to requirements for async performance.
2.  **Implement Multi-Topic Consumer:**
    *   The Orchestrator must listen to THREE topics: `agent.tasks.in`, `agent.llm.responses`, `agent.tool.responses`.
    *   *Implementation Detail:* `aiokafka` allows subscribing to multiple topics in one consumer group.
3.  **Implement Routing Logic (The Core):**
    *   Inside the async consumer loop, switch logic based on the `topic` of the incoming message:
        *   **IF `agent.tasks.in`:** Create initial state in Redis (Key: `task:{task_id}`). Route initial prompt to `agent.llm.requests`.
        *   **IF `agent.llm.responses`:** 
            *   Read the LLM output.
            *   *Does it contain a tool call?* -> Append LLM output to Redis history. Route tool call to `agent.tool.requests`. Update Redis status to `ACTING`.
            *   *Is it a final answer?* -> Append to Redis history. Update Redis status to `COMPLETED`. (Loop ends).
        *   **IF `agent.tool.responses`:**
            *   Read the Tool observation.
            *   Append observation to Redis history.
            *   Route the *entire updated history* back to `agent.llm.requests`. (Loop restarts!).
4.  **Verify:** Test the loop manually. Inject a message into `agent.tasks.in`. Watch the Orchestrator route to LLM, LLM route back, Orchestrator route to Tool, Tool route back, and Orchestrator route to LLM again. Check Redis after each step to ensure the history is appending correctly.

---

## Step 4: System Prompt & Tool Formatting
*Goal: Make the LLM actually use the loop correctly.*

1.  **Define the ReAct System Prompt:** The LLM needs strict instructions on how to output tool calls so your Orchestrator can parse them reliably. Format it to output JSON (e.g., `{"thought": "...", "action": "Calculator", "action_input": "2+2"}`) or use native OpenAI Tool Calling format if Ollama supports it for your chosen model.
2.  **Inject Prompt in Orchestrator:** When the Orchestrator bootstraps the initial Redis state from `agent.tasks.in`, prepend the System Prompt to the history array before sending it to the LLM Service.
3.  **Verify:** Send a real user prompt: *"What is 2 + 2?"* Ensure the LLM generates a tool call, the Orchestrator parses it perfectly, the Tool executes, and the LLM generates a final answer based on the observation.

---

## Step 5: API Gateway & Observability
*Goal: Make the system usable by a frontend and observable by a developer.*

1.  **Add FastAPI Endpoint:** Add a `POST /agent/run` endpoint to the Orchestrator service. It accepts a user prompt, generates a `task_id`, initializes Redis, and produces to `agent.tasks.in`. It returns the `task_id` to the caller immediately.
2.  **Add Status Endpoint:** Add a `GET /agent/status/{task_id}` endpoint that simply reads the current JSON state from Redis and returns it. This allows a frontend to poll for the final answer.
3.  **Add Lifecycle Events:** Modify the Orchestrator to produce UI-friendly events to `agent.lifecycle.events` (e.g., `{"status": "thinking", "message": "Using calculator..."}`, `{"status": "completed", "message": "The answer is 4"}`).
4.  **Final End-to-End Test:**
    *   Run all 3 Python services concurrently (`uvicorn orchestrator.main:app`, etc.).
    *   Use the `test_client` script to send a request to `POST /agent/run`.
    *   Poll `GET /agent/status/{task_id}` until status is `COMPLETED`.
    *   Verify the final answer is correct.

---

## Success Criteria

You can send a multi-step reasoning request (e.g., *"Calculate 5 * 10, then add 3 to the result"*) to the API. The system asynchronously loops through the LLM and Calculator tool via Kafka, persists the intermediate state in Redis, and returns the correct final answer without any synchronous blocking or crashes.
```