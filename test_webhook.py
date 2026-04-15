import requests


def main() -> None:
    url = "http://localhost:8000/webhook"
    payload = {
        "error_log": "pyspark.sql.utils.AnalysisException: [DELTA_FAILED_TO_MERGE_FIELDS] Failed to merge fields 'user_id' and 'user_id'. Failed to merge incompatible data types IntegerType and StringType. Also, column 'transaction_amount' is present in the data but not in the schema (expected 'amount')."
    }

    print("🚀 Sending webhook to LangGraph AI...")
    try:
        response = requests.post(url, json=payload, timeout=30)
        response.raise_for_status()
        print("Response JSON:")
        print(response.json())
    except requests.exceptions.RequestException as exc:
        print("Request failed:")
        print(exc)


if __name__ == "__main__":
    main()
