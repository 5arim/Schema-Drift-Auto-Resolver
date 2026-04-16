import subprocess

import requests


def extract_schema_error_from_stdout(stdout_text: str) -> str:
    cleaned_stdout = (stdout_text or "").strip()
    if not cleaned_stdout:
        return ""

    marker = "======"
    parts = cleaned_stdout.split(marker)

    # Expected shape:
    # ... banner ...
    # ======
    # <actual exception text>
    # ======
    # ... optional trailing logs ...
    if len(parts) >= 3:
        extracted_error = parts[1].strip()
        if extracted_error:
            return extracted_error

    # Fallback when expected markers are missing.
    return cleaned_stdout


def run_orchestration() -> None:
    print("\n========== Starting Day 1 Pipeline (Expected: Success) ==========")
    subprocess.run(["python", "pipeline.py", "1"], check=False)

    print(
        "\n========== Starting Day 2 Pipeline (Expected: Failure / Schema Drift) =========="
    )
    day_2_result = subprocess.run(
        ["python", "pipeline.py", "2"],
        check=False,
        capture_output=True,
        text=True,
    )

    if day_2_result.returncode != 0:
        captured_error_output = extract_schema_error_from_stdout(day_2_result.stdout)
        if not captured_error_output:
            captured_error_output = (day_2_result.stderr or "").strip()

        try:
            webhook_response = requests.post(
                "http://localhost:8000/webhook",
                json={"error_log": captured_error_output},
                timeout=120,
            )
            webhook_response.raise_for_status()
        except requests.RequestException as exc:
            print("\nWebhook trigger failed:")
            print(exc)
            return

        print("\n✅ Webhook sent to AI resolver.")
        print("Pipeline Failure Detected on Day 2 (Schema Drift).")
        print("🤖 AI Auto-Resolver was triggered via /webhook.")
        print("📄 Patch path reported by resolver: patches/auto_patch_v1.py")
    else:
        print("\nDay 2 unexpectedly succeeded. No schema drift detected.")


if __name__ == "__main__":
    run_orchestration()
