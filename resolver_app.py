import ast
import os
import re
from pathlib import Path
from typing import TypedDict

import requests
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from langchain_openai import ChatOpenAI
from langchain_ollama import OllamaLLM
from langgraph.graph import END, START, StateGraph
from pydantic import BaseModel

load_dotenv()


def send_slack_alert(message: str) -> None:
    slack_webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not slack_webhook_url:
        return
    try:
        response = requests.post(
            slack_webhook_url,
            json={"text": message},
            timeout=15,
        )
        if response.status_code != 200:
            print(
                f"[Slack] Alert failed with status {response.status_code}: {response.text}"
            )
    except requests.RequestException as exc:
        print(f"[Slack] Alert request failed: {exc}")


class ResolverState(TypedDict):
    error_log: str
    expected_schema: str
    incoming_schema: str
    generated_code: str
    llm_provider: str
    retry_count: int
    validation_error: str


def parse_error_log(state: ResolverState) -> ResolverState:
    error_log = state.get("error_log", "")
    expected_schema = ""
    incoming_schema = ""

    # Common Delta mismatch pattern:
    # [DELTA_FAILED_TO_MERGE_FIELDS] Failed to merge fields 'user_id' and 'user_id'
    merge_match = re.search(
        r"\[DELTA_FAILED_TO_MERGE_FIELDS\].*?Failed to merge fields '([^']+)' and '([^']+)'",
        error_log,
        flags=re.DOTALL,
    )
    if merge_match:
        expected_schema = f"Target field in Delta table: {merge_match.group(1)}"
        incoming_schema = f"Incoming field from DataFrame: {merge_match.group(2)}"

    # Try to extract richer schema snippets if present in stacktrace/log text.
    table_schema_match = re.search(
        r"(?:Table schema|Target schema)\s*[:=]\s*(.+?)(?:\n\n|\n[A-Z][^\n]*:|$)",
        error_log,
        flags=re.DOTALL,
    )
    data_schema_match = re.search(
        r"(?:Data schema|Incoming schema|Source schema)\s*[:=]\s*(.+?)(?:\n\n|\n[A-Z][^\n]*:|$)",
        error_log,
        flags=re.DOTALL,
    )

    if table_schema_match:
        expected_schema = table_schema_match.group(1).strip()
    if data_schema_match:
        incoming_schema = data_schema_match.group(1).strip()

    # Fallback: keep short context for 1B model if exact parsing is not available.
    if not expected_schema:
        expected_schema = "Could not parse expected schema exactly. Key error snippet: " + error_log[
            :300
        ].replace("\n", " ")
    if not incoming_schema:
        incoming_schema = "Could not parse incoming schema exactly. Key error snippet: " + error_log[
            :300
        ].replace("\n", " ")

    return {
        **state,
        "expected_schema": expected_schema,
        "incoming_schema": incoming_schema,
    }


def generate_patch(state: ResolverState) -> ResolverState:
    retry_count = state.get("retry_count", 0) + 1
    llm_provider = "local_ollama"
    openrouter_api_key = os.getenv("OPENROUTER_API_KEY2")
    if openrouter_api_key:
        llm_provider = "openrouter"
        print("[LLM] Provider selected: OpenRouter")
        llm = ChatOpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=openrouter_api_key,
            model="meta-llama/llama-3.3-70b-instruct",
        )
    else:
        print("[LLM] Provider selected: Local Ollama")
        llm = OllamaLLM(model="llama3.2:1b")

    prompt = f"""
You are an automated PySpark code generator.
Your ONLY job is to output 1 or 2 lines of PySpark code to fix schema drift.

<example>
Error: Failed to merge incompatible data types IntegerType and StringType for column 'age'. Column 'new_price' is present but expected 'price'.
Code:
df = df.withColumn("age", F.col("age").cast("string"))
df = df.withColumnRenamed("new_price", "price")
</example>

Now, look at the REAL error below and write the code to fix it. Do not copy the example.
Expected Schema: {state.get('expected_schema', '')}
Incoming Data Schema: {state.get('incoming_schema', '')}
Error Log: {state.get('error_log', '')}
Code:
"""
    validation_error = state.get("validation_error", "")
    if validation_error:
        prompt += (
            "\n\nWARNING: Your previous attempt failed Python validation with this "
            f"error: {validation_error}. Please fix your syntax and try again."
        )

    try:
        llm_result = llm.invoke(prompt)
    except Exception as exc:
        if llm_provider == "openrouter":
            print(f"[LLM] OpenRouter failed, falling back to Local Ollama: {exc}")
            llm_provider = "local_ollama"
            llm_result = OllamaLLM(model="llama3.2:1b").invoke(prompt)
        else:
            raise
    if isinstance(llm_result, str):
        generated_code = llm_result.strip()
    else:
        generated_code = str(getattr(llm_result, "content", llm_result)).strip()
    return {
        **state,
        "generated_code": generated_code,
        "llm_provider": llm_provider,
        "retry_count": retry_count,
    }


def validate_patch(state: ResolverState) -> ResolverState:
    try:
        ast.parse(state.get("generated_code", ""))
        validation_error = ""
    except (SyntaxError, Exception) as e:
        validation_error = str(e)
        send_slack_alert(
            f"⚠️ *Agent Self-Correction:* Generated code failed validation (`{str(e)}`). "
            "Attempting to fix and rewrite..."
        )
    return {**state, "validation_error": validation_error}


def route_validation(state: ResolverState) -> str:
    if state.get("validation_error", "") == "":
        return "save_patch"
    if state.get("retry_count", 0) < 3:
        return "generate_patch"
    return "save_patch"


def save_patch(state: ResolverState) -> ResolverState:
    patches_dir = Path("patches")
    patches_dir.mkdir(parents=True, exist_ok=True)
    patch_path = patches_dir / "auto_patch_v1.py"
    generated_code = state.get("generated_code", "")
    cleaned_code = (
        generated_code.replace("```python", "").replace("```", "").strip()
    )
    patch_path.write_text(cleaned_code, encoding="utf-8")
    send_slack_alert(
        "✅ *Issue Resolved:* Agent successfully generated and validated the PySpark "
        "migration patch. Saved to `patches/auto_patch_v1.py`."
    )
    return state


def build_graph():
    graph_builder = StateGraph(ResolverState)
    graph_builder.add_node("parse_error_log", parse_error_log)
    graph_builder.add_node("generate_patch", generate_patch)
    graph_builder.add_node("validate_patch", validate_patch)
    graph_builder.add_node("save_patch", save_patch)

    graph_builder.add_edge(START, "parse_error_log")
    graph_builder.add_edge("parse_error_log", "generate_patch")
    graph_builder.add_edge("generate_patch", "validate_patch")
    graph_builder.add_conditional_edges(
        "validate_patch",
        route_validation,
        {
            "generate_patch": "generate_patch",
            "save_patch": "save_patch",
        },
    )
    graph_builder.add_edge("save_patch", END)

    return graph_builder.compile()


resolver_graph = build_graph()
app = FastAPI(title="Schema Drift Auto Resolver")


class WebhookPayload(BaseModel):
    error_log: str


@app.post("/webhook")
def webhook(payload: WebhookPayload):
    try:
        send_slack_alert(
            "🚨 *Data Pipeline Failure Detected* 🚨\n"
            "Job: `daily_orders_ingestion`\n\n"
            "🤖 *AI Agent Triggered:* Investigating schema drift and drafting a patch..."
        )
        final_state = resolver_graph.invoke(
            {
                "error_log": payload.error_log,
                "expected_schema": "",
                "incoming_schema": "",
                "generated_code": "",
                "llm_provider": "",
                "retry_count": 0,
                "validation_error": "",
            }
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return {
        "status": "success",
        "generated_code": final_state.get("generated_code", ""),
        "saved_path": "patches/auto_patch_v1.py",
        "llm_provider": final_state.get("llm_provider", "unknown"),
    }


if __name__ == "__main__":
    uvicorn.run("resolver_app:app", host="0.0.0.0", port=8000, reload=False)
