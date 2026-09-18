# Brand address extract

Cloud Run service that pulls contracts from BigQuery, asks Gemini for the counterparty address on each PDF, walks newest-`start` first per `brand_id`, and upserts one row per brand.

Prompt and schema are **not** baked into the image. Load them at startup from environment variables (Secret Manager) so they can be switched without a deploy.

Local source-of-truth files for those secrets:

- `1prompt.txt` — extraction rules (ignore PSN, distributor vs brand, no invented addresses)
- `2schema.json` — Gemini `response_schema`

Runtime files:

- `main.py` — Flask app and pipeline
- `Dockerfile` — Cloud Run image
- `requirements.txt` — Python dependencies

---

## Prompt (`1prompt.txt`)

Publish as the secret named `address-prompt`.

Owns procedure the JSON Schema cannot enforce: ignore PSN/Chiswick, open-ended templates, distributor = contracting party not the brand, skip if unsure.

## Schema (`2schema.json`)

Publish as the secret named `address-schema`.

Gemini is called with `response_mime_type=application/json` and this object as `response_schema`.

Required fields: `counterparty_name`, `counterparty_address`, `brand_name_in_contract`, `through_distributor`, `address_role` (`brand_office` | `distributor_office` | `unknown`), `evidence`.

---

## App (`main.py`)

Flask service for Cloud Run.

**Pipeline**

1. Read contracts from `{PROJECT_ID}.{DATASET_ID}.{SOURCE_TABLE}` joined to `{BRANDS_TABLE}` for `brand_name`
2. Group by `brand_id`, order by `start DESC` (`inserted_at` tiebreaker)
3. Skip `brand_id`s already in `{OUTPUT_TABLE}` unless `FORCE=true`
4. For each brand, fetch PDFs from `{PDF_BUCKET}` using `pdf_url` as the object path, then call Gemini until a usable distributor address (keep it) and/or a brand office (newest wins). Unusable / unknown / PSN address → next contract. None usable → flag the brand
5. Upsert `{OUTPUT_TABLE}` on `brand_id`

The output table must already exist. This service does not create tables.

**Routes**

- `GET` / `POST` `/` — run the batch
- `GET` `/health` — liveness

**Secrets** (Cloud Run: Reference a secret → env var of the **same name**)

| Secret / env var | Value |
|---|---|
| `PROJECT_ID` | `psn-tools` |
| `DATASET_ID` | `tag_manager` |
| `SOURCE_TABLE` | `contracts` |
| `BRANDS_TABLE` | `brands` |
| `OUTPUT_TABLE` | `brand_addresses` |
| `PDF_BUCKET` | `contract_test_psn` |
| `GEMINI_API_KEY` | Gemini API key |
| `address-prompt` | contents of `1prompt.txt` |
| `address-schema` | contents of `2schema.json` |

**Plain env (not secrets)**

| Variable | Default |
|---|---|
| `LIMIT` | `0` (all remaining brands) |
| `FORCE` | false |
| `DRY_RUN` | false |
| `PORT` | `8080` |

Smoke with `LIMIT=3`.

Output columns: `brand_id`, `brand_name`, `address`, `through_distributor`, `distributor_address`, `flagged`, `brand_contract_id`, `distributor_contract_id`, `updated_at`.

---

## Image (`Dockerfile`)

Python 3.11 slim. Copies only `requirements.txt` and `main.py`. Does **not** copy prompt or schema files. Listens on `8080` and runs `python main.py`.
