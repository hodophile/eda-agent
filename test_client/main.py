"""Test client: POST /agent/run then poll /agent/status until completion."""
from __future__ import annotations

import argparse
import asyncio
import sys

import httpx

ORCHESTRATOR_URL = "http://localhost:8000"
TERMINAL = {"COMPLETED", "FAILED"}


async def run(prompt: str, poll_interval: float = 0.5, timeout: float = 120.0) -> int:
    async with httpx.AsyncClient(base_url=ORCHESTRATOR_URL, timeout=30) as client:
        r = await client.post("/agent/run", json={"prompt": prompt})
        r.raise_for_status()
        task_id = r.json()["task_id"]
        print(f"-> task_id={task_id}")

        elapsed = 0.0
        while elapsed < timeout:
            await asyncio.sleep(poll_interval)
            elapsed += poll_interval
            r = await client.get(f"/agent/status/{task_id}")
            state = r.json()
            status = state["status"]
            if status in TERMINAL:
                print(f"\n[{status}] steps={state.get('step_count')}")
                if status == "COMPLETED":
                    print(f"Answer: {state.get('final_answer')}")
                else:
                    print(f"Error: {state.get('error')}")
                return 0 if status == "COMPLETED" else 1
            print(f"   ...{status} (step {state.get('step_count')})", flush=True)

    print("\nTIMEOUT")
    return 2


def main() -> None:
    ap = argparse.ArgumentParser(description="Trigger the Phase 1 agent loop.")
    ap.add_argument("prompt", nargs="?", default="Calculate 5 * 10, then add 3 to the result.")
    args = ap.parse_args()
    sys.exit(asyncio.run(run(args.prompt)))


if __name__ == "__main__":
    main()
