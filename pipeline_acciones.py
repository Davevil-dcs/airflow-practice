
from datetime import datetime
import json
from pathlib import Path

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="pipeline_acciones",
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    tags=["practica", "analisis"],
)
def pipeline_acciones():

    @task
    def extract_data():
        print("Extrayendo datos bursatiles...")

        return [
            {"symbol": "AAPL", "price": 250.50},
            {"symbol": "MSFT", "price": 510.20},
            {"symbol": "GOOGL", "price": 285.30},
            {"symbol": "NVDA", "price": 180.75},
        ]

    @task
    def analyze_data(data):
        print("Procesando datos...")

        prices = [item["price"] for item in data]

        results = {
            "average": round(sum(prices) / len(prices), 2),
            "maximum": max(prices),
            "minimum": min(prices),
            "companies_analyzed": len(data),
        }

        print(results)
        return results

    @task
    def save_results(results):
        output = Path.home() / "airflow-practica" / "resultados.json"
        output.parent.mkdir(parents=True, exist_ok=True)

        output.write_text(
            json.dumps(results, indent=4),
            encoding="utf-8"
        )

        print(f"Resultados guardados en: {output}")

    data = extract_data()
    analysis = analyze_data(data)
    save_results(analysis)


pipeline_acciones()

