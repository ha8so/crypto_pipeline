import os
import requests
import psycopg2
from datetime import datetime, timezone


def get_connection():
    """
    Connexion PostgreSQL :
    - Si on est dans une task Airflow (AIRFLOW_CTX_DAG_ID défini) :
        -> on vise host.docker.internal:5432 (Postgres Windows depuis Docker)
    - Sinon (exécution locale) :
        -> on vise localhost:5432
    """
    running_in_airflow = os.getenv("AIRFLOW_CTX_DAG_ID") is not None

    if running_in_airflow:
        host = "host.docker.internal"
    else:
        host = "localhost"

    port = 5432
    dbname = "tajarib"
    user = "postgres"
    password = "123456"   # <--- adapte ici

    print(f"[ingest_prices] Connecting to PostgreSQL at {host}:{port}, db={dbname}, user={user}")

    return psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
    )


def ingest_prices():
    """Récupère les prix de quelques cryptos et les insère dans raw_prices."""
    conn = get_connection()
    cur = conn.cursor()

    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": "bitcoin,ethereum,binancecoin",
        "vs_currencies": "usd",
        "include_market_cap": "true",
        "include_24hr_vol": "true",
    }

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()

    now_utc = datetime.now(timezone.utc)

    for asset_id, info in data.items():
        symbol = asset_id[:3].upper()
        price = info.get("usd")
        mktcap = info.get("usd_market_cap")
        vol24 = info.get("usd_24h_vol")

        cur.execute(
            """
            INSERT INTO raw_prices (
                asset_id, asset_symbol, price_usd,
                market_cap_usd, volume_24h_usd, price_ts_utc
            )
            VALUES (%s, %s, %s, %s, %s, %s);
            """,
            (asset_id, symbol, price, mktcap, vol24, now_utc),
        )

    conn.commit()
    cur.close()
    conn.close()

    print("[ingest_prices] Ingestion terminée.")


if __name__ == "__main__":
    ingest_prices()
