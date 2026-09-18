import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Callable, Iterable

from flask import Flask, jsonify
from google import genai
from google.cloud import bigquery, storage
from google.genai import types

# Cloud Run: Reference a secret → env var of the same name.
REQUIRED_SECRETS = (
    "PROJECT_ID",  # psn-tools
    "DATASET_ID",  # tag_manager
    "SOURCE_TABLE",  # contracts
    "BRANDS_TABLE",  # brands
    "OUTPUT_TABLE",  # brand_addresses
    "PDF_BUCKET",  # contract_test_psn
    "GEMINI_API_KEY",
    "address-prompt",
    "address-schema",
)

PSN_POISON = re.compile(
    r"play sports network|\bpsn\b|chiswick park|566 chiswick",
    re.I,
)
ADDRESS_ROLES = {"brand_office", "distributor_office", "unknown"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

app = Flask(__name__)


@dataclass(frozen=True)
class Contract:
    brand_id: str
    brand_name: str
    contract_id: str
    contract_name: str
    start: date | None
    inserted_at: datetime | None
    pdf_url: str | None


@dataclass(frozen=True)
class Extract:
    counterparty_name: str
    counterparty_address: str
    brand_name_in_contract: str
    through_distributor: bool
    address_role: str
    evidence: str


@dataclass(frozen=True)
class BrandResult:
    brand_id: str
    brand_name: str
    address: str | None
    through_distributor: bool
    distributor_address: str | None
    flagged: bool
    brand_contract_id: str | None
    distributor_contract_id: str | None


def parse_extract(raw):
    if isinstance(raw, str):
        raw = raw.strip()
        if raw.startswith("```"):
            raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw)
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return None
    else:
        data = raw
    if not isinstance(data, dict):
        return None
    role = str(data.get("address_role") or "unknown").strip()
    if role not in ADDRESS_ROLES:
        role = "unknown"
    return Extract(
        counterparty_name=str(data.get("counterparty_name") or "").strip(),
        counterparty_address=str(data.get("counterparty_address") or "").strip(),
        brand_name_in_contract=str(data.get("brand_name_in_contract") or "").strip(),
        through_distributor=bool(data.get("through_distributor")),
        address_role=role,
        evidence=str(data.get("evidence") or "").strip(),
    )


def is_usable(extract):
    if extract is None or extract.address_role == "unknown":
        return False
    if not extract.counterparty_address:
        return False
    if PSN_POISON.search(extract.counterparty_address):
        return False
    return True


def is_distributor(extract):
    if extract.address_role == "distributor_office":
        return True
    if extract.address_role == "brand_office":
        return False
    return extract.through_distributor


def walk_brand(contracts: Iterable[Contract], extract_fn: Callable[[Contract], Extract | None]):
    ordered = list(contracts)
    brand_id = ordered[0].brand_id if ordered else ""
    brand_name = next((c.brand_name for c in ordered if c.brand_name), "")
    address = None
    brand_contract_id = None
    through_distributor = False
    distributor_address = None
    distributor_contract_id = None
    seen_pdfs = set()

    for contract in ordered:
        url = (contract.pdf_url or "").strip()
        if not url or url in seen_pdfs:
            continue
        seen_pdfs.add(url)
        extract = extract_fn(contract)
        if not is_usable(extract):
            continue
        if is_distributor(extract):
            if not through_distributor:
                through_distributor = True
                distributor_address = extract.counterparty_address
                distributor_contract_id = contract.contract_id
            continue
        if address is None:
            address = extract.counterparty_address
            brand_contract_id = contract.contract_id
            if extract.brand_name_in_contract and not brand_name:
                brand_name = extract.brand_name_in_contract
            break

    if not through_distributor:
        distributor_address = None
        distributor_contract_id = None
    return BrandResult(
        brand_id=brand_id,
        brand_name=brand_name,
        address=address,
        through_distributor=through_distributor,
        distributor_address=distributor_address,
        flagged=address is None,
        brand_contract_id=brand_contract_id,
        distributor_contract_id=distributor_contract_id,
    )


def sort_key(contract):
    start = contract.start or date.min
    inserted = contract.inserted_at or datetime.min.replace(tzinfo=timezone.utc)
    if inserted.tzinfo is None:
        inserted = inserted.replace(tzinfo=timezone.utc)
    return (start, inserted)


def _env_bool(name):
    return os.environ.get(name, "").lower() in {"1", "true", "yes"}


class BrandAddressExtractor:
    def __init__(self):
        missing = [name for name in REQUIRED_SECRETS if not os.environ.get(name)]
        if missing:
            raise ValueError(f"Missing required secrets: {', '.join(missing)}")

        self.project_id = os.environ["PROJECT_ID"]
        self.dataset_id = os.environ["DATASET_ID"]
        self.source_table = os.environ["SOURCE_TABLE"]
        self.brands_table = os.environ["BRANDS_TABLE"]
        self.output_table = os.environ["OUTPUT_TABLE"]
        self.pdf_bucket = os.environ["PDF_BUCKET"]
        self.gemini_api_key = os.environ["GEMINI_API_KEY"]
        prompt_raw = os.environ["address-prompt"]
        schema_raw = os.environ["address-schema"]
        self.model = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")
        self.limit = int(os.environ.get("LIMIT", "0"))
        self.force = _env_bool("FORCE")
        self.dry_run = _env_bool("DRY_RUN")

        self.bq_client = bigquery.Client(project=self.project_id)
        self.gcs_client = storage.Client(project=self.project_id)
        self.genai_client = genai.Client(api_key=self.gemini_api_key)
        self.prompt = prompt_raw.strip()
        self.response_schema = json.loads(schema_raw)
        self.pdf_cache = {}
        logger.info(
            "Address extractor initialized "
            f"(source={self.source_table}, output={self.output_table}, "
            f"bucket={self.pdf_bucket}, model={self.model})"
        )

    def _table(self, name):
        return f"{self.project_id}.{self.dataset_id}.{name}"

    def get_contracts(self):
        query = f"""
        SELECT
          c.brand_id,
          b.brand_name,
          c.contract_id,
          c.contract_name,
          c.start,
          c.inserted_at,
          c.pdf_url
        FROM `{self._table(self.source_table)}` c
        LEFT JOIN `{self._table(self.brands_table)}` b
          ON c.brand_id = b.brand_id AND NOT b.is_deleted
        WHERE NOT c.is_deleted
          AND c.brand_id IS NOT NULL
          AND c.brand_id != ''
        """
        contracts = []
        try:
            for row in self.bq_client.query(query).result():
                contracts.append(
                    Contract(
                        brand_id=row.brand_id,
                        brand_name=row.brand_name or row.contract_name or "",
                        contract_id=row.contract_id,
                        contract_name=row.contract_name or "",
                        start=row.start,
                        inserted_at=row.inserted_at,
                        pdf_url=row.pdf_url,
                    )
                )
            logger.info(f"Found {len(contracts)} contracts")
            return contracts
        except Exception as e:
            logger.error(f"Failed to fetch contracts from BigQuery: {e}")
            raise

    def existing_brand_ids(self):
        query = f"SELECT brand_id FROM `{self._table(self.output_table)}`"
        try:
            return {row.brand_id for row in self.bq_client.query(query).result()}
        except Exception as e:
            logger.error(f"Failed to read output table: {e}")
            raise

    def fetch_pdf(self, url):
        key = url[5:].split("/", 1)[-1] if url.startswith("gs://") else url.lstrip("/")
        return self.gcs_client.bucket(self.pdf_bucket).blob(key).download_as_bytes()

    def extract_pdf(self, contract):
        url = (contract.pdf_url or "").strip()
        if not url:
            return None
        if url in self.pdf_cache:
            return self.pdf_cache[url]
        try:
            pdf_bytes = self.fetch_pdf(url)
            logger.info(f"Calling Gemini for {contract.contract_id} ({len(pdf_bytes)} bytes)")
            response = self.genai_client.models.generate_content(
                model=self.model,
                contents=types.Content(
                    parts=[
                        types.Part.from_bytes(data=pdf_bytes, mime_type="application/pdf"),
                        types.Part(text=self.prompt),
                    ]
                ),
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=self.response_schema,
                ),
            )
            extract = parse_extract(response.text if response else "")
            if extract is None:
                logger.error(f"schema_mismatch for {contract.contract_id}")
            elif extract.address_role == "unknown" or not extract.counterparty_address:
                logger.info(f"Unusable extract for {contract.contract_id}, skipping contract")
            self.pdf_cache[url] = extract
            time.sleep(1)
            return extract
        except Exception as e:
            logger.exception(f"Error processing {contract.contract_id}: {e}")
            self.pdf_cache[url] = None
            return None

    def save_results(self, results):
        if not results:
            return True
        table_id = self._table(self.output_table)
        now = datetime.utcnow().isoformat()
        rows = [
            {
                "brand_id": r.brand_id,
                "brand_name": r.brand_name,
                "address": r.address,
                "through_distributor": r.through_distributor,
                "distributor_address": r.distributor_address,
                "flagged": r.flagged,
                "brand_contract_id": r.brand_contract_id,
                "distributor_contract_id": r.distributor_contract_id,
                "updated_at": now,
            }
            for r in results
        ]
        ids = [r.brand_id for r in results]
        delete = f"DELETE FROM `{table_id}` WHERE brand_id IN UNNEST(@ids)"
        try:
            self.bq_client.query(
                delete,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[bigquery.ArrayQueryParameter("ids", "STRING", ids)]
                ),
            ).result()
            table = self.bq_client.get_table(table_id)
            errors = self.bq_client.insert_rows_json(table, rows)
            if errors:
                logger.error(f"Failed to insert rows: {errors}")
                return False
            logger.info(f"Upserted {len(rows)} brands")
            return True
        except Exception as e:
            logger.error(f"Error saving to BigQuery: {e}")
            return False

    def process_brands(self):
        try:
            grouped = {}
            for contract in self.get_contracts():
                grouped.setdefault(contract.brand_id, []).append(contract)
            for brand_id, rows in grouped.items():
                grouped[brand_id] = sorted(rows, key=sort_key, reverse=True)

            done = set() if self.force else self.existing_brand_ids()
            brand_ids = [b for b in grouped if b not in done]
            if self.limit:
                brand_ids = brand_ids[: self.limit]

            if not brand_ids:
                logger.info("No brands to process")
                return {"processed": 0, "errors": 0, "message": "No brands to process"}

            results = []
            errors = 0
            for i, brand_id in enumerate(brand_ids, 1):
                logger.info(f"Processing {i}/{len(brand_ids)} {brand_id}")
                try:
                    result = walk_brand(grouped[brand_id], self.extract_pdf)
                    results.append(result)
                    logger.info(
                        f"{brand_id} flagged={result.flagged} "
                        f"dist={result.through_distributor} addr={bool(result.address)}"
                    )
                except Exception as e:
                    errors += 1
                    logger.error(f"Failed {brand_id}: {e}")

            if self.dry_run:
                return {
                    "processed": len(results),
                    "errors": errors,
                    "total": len(brand_ids),
                    "dry_run": True,
                    "results": [r.__dict__ for r in results],
                    "message": f"Dry run {len(results)}/{len(brand_ids)} brands",
                }

            if not self.save_results(results):
                return {"error": "Failed to save results to BigQuery"}

            flagged = sum(1 for r in results if r.flagged)
            return {
                "processed": len(results),
                "errors": errors,
                "flagged": flagged,
                "total": len(brand_ids),
                "message": f"Processed {len(results)}/{len(brand_ids)} brands, {flagged} flagged",
            }
        except Exception as e:
            logger.error(f"Error in process_brands: {e}")
            return {"error": str(e)}


@app.route("/", methods=["POST", "GET"])
def main_handler():
    print("=== CLOUD RUN STARTED ===")
    logger.info("Starting brand address extract")
    try:
        extractor = BrandAddressExtractor()
        result = extractor.process_brands()
        print(f"=== RESULT: {result} ===")
        if "error" in result:
            logger.error(f"Processing failed: {result}")
            return jsonify(result), 500
        logger.info(f"Processing completed: {result}")
        return jsonify(result), 200
    except Exception as e:
        error_msg = f"Unhandled error: {str(e)}"
        print(f"=== ERROR: {error_msg} ===")
        logger.error(error_msg)
        return jsonify({"error": str(e)}), 500


@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()}), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
