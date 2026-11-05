"""Validate the raw tables with Great Expectations before dbt transforms them.

Exits with status 1 if any expectation fails, which stops the Airflow run.
Usage: validate_raw.py [--target bigquery|sample]
"""
import argparse
import os
import sys
from pathlib import Path

import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.checkpoint import UpdateDataDocsAction

ROOT = Path(__file__).resolve().parents[1]
DOCS_DIR = Path(os.environ.get("GX_DOCS_DIR", ROOT / "reports" / "data_docs"))
DOCS_SITE = "olist_data_docs"
SAMPLE_DIR = Path(os.environ.get("GX_SAMPLE_DIR", ROOT / "dbt" / "sample_data"))

TIMESTAMP = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"
INTEGER = r"^\d+$"
DECIMAL = r"^\d+(\.\d+)?$"
ZIP_PREFIX = r"^\d{5}$"
STATE = r"^[A-Z]{2}$"

ORDER_STATUSES = [
    "approved", "canceled", "created", "delivered",
    "invoiced", "processing", "shipped", "unavailable",
]
PAYMENT_TYPES = ["boleto", "credit_card", "debit_card", "not_defined", "voucher"]
REVIEW_SCORES = ["1", "2", "3", "4", "5"]


def not_null(*columns):
    return [gxe.ExpectColumnValuesToNotBeNull(column=c) for c in columns]


def unique(column):
    return [gxe.ExpectColumnValuesToBeUnique(column=column)]


def matches(pattern, *columns):
    return [gxe.ExpectColumnValuesToMatchRegex(column=c, regex=pattern) for c in columns]


# Each table lists its exact columns, the fewest rows a full BigQuery load should
# have (about 90% of the source), and the rules for its values. Null values are
# skipped by the regex and set checks, so optional columns can use them too.
TABLES = {
    "customers": {
        "columns": [
            "customer_id", "customer_unique_id", "customer_zip_code_prefix",
            "customer_city", "customer_state",
        ],
        "min_rows": 90_000,
        "rules": [
            *not_null("customer_id", "customer_unique_id", "customer_state"),
            *unique("customer_id"),
            *matches(ZIP_PREFIX, "customer_zip_code_prefix"),
            *matches(STATE, "customer_state"),
        ],
    },
    "orders": {
        "columns": [
            "order_id", "customer_id", "order_status", "order_purchase_timestamp",
            "order_approved_at", "order_delivered_carrier_date",
            "order_delivered_customer_date", "order_estimated_delivery_date",
        ],
        "min_rows": 90_000,
        "rules": [
            *not_null(
                "order_id", "customer_id", "order_status",
                "order_purchase_timestamp", "order_estimated_delivery_date",
            ),
            *unique("order_id"),
            gxe.ExpectColumnValuesToBeInSet(column="order_status", value_set=ORDER_STATUSES),
            *matches(
                TIMESTAMP,
                "order_purchase_timestamp", "order_approved_at",
                "order_delivered_carrier_date", "order_delivered_customer_date",
                "order_estimated_delivery_date",
            ),
        ],
    },
    "order_items": {
        "columns": [
            "order_id", "order_item_id", "product_id", "seller_id",
            "shipping_limit_date", "price", "freight_value",
        ],
        "min_rows": 100_000,
        "rules": [
            *not_null("order_id", "order_item_id", "product_id", "seller_id", "price"),
            gxe.ExpectCompoundColumnsToBeUnique(column_list=["order_id", "order_item_id"]),
            *matches(INTEGER, "order_item_id"),
            *matches(DECIMAL, "price", "freight_value"),
            *matches(TIMESTAMP, "shipping_limit_date"),
        ],
    },
    "order_payments": {
        "columns": [
            "order_id", "payment_sequential", "payment_type",
            "payment_installments", "payment_value",
        ],
        "min_rows": 90_000,
        "rules": [
            *not_null("order_id", "payment_sequential", "payment_type", "payment_value"),
            gxe.ExpectCompoundColumnsToBeUnique(column_list=["order_id", "payment_sequential"]),
            gxe.ExpectColumnValuesToBeInSet(column="payment_type", value_set=PAYMENT_TYPES),
            *matches(INTEGER, "payment_sequential", "payment_installments"),
            *matches(DECIMAL, "payment_value"),
        ],
    },
    "order_reviews": {
        "columns": [
            "review_id", "order_id", "review_score", "review_comment_title",
            "review_comment_message", "review_creation_date", "review_answer_timestamp",
        ],
        "min_rows": 90_000,
        "rules": [
            *not_null("review_id", "order_id", "review_score"),
            gxe.ExpectCompoundColumnsToBeUnique(column_list=["review_id", "order_id"]),
            gxe.ExpectColumnValuesToBeInSet(column="review_score", value_set=REVIEW_SCORES),
            *matches(TIMESTAMP, "review_creation_date", "review_answer_timestamp"),
        ],
    },
    "products": {
        "columns": [
            "product_id", "product_category_name", "product_name_lenght",
            "product_description_lenght", "product_photos_qty", "product_weight_g",
            "product_length_cm", "product_height_cm", "product_width_cm",
        ],
        "min_rows": 29_000,
        "rules": [
            *not_null("product_id"),
            *unique("product_id"),
            *matches(
                INTEGER,
                "product_photos_qty", "product_weight_g", "product_length_cm",
                "product_height_cm", "product_width_cm",
            ),
        ],
    },
    "sellers": {
        "columns": ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"],
        "min_rows": 2_500,
        "rules": [
            *not_null("seller_id", "seller_state"),
            *unique("seller_id"),
            *matches(ZIP_PREFIX, "seller_zip_code_prefix"),
            *matches(STATE, "seller_state"),
        ],
    },
    "product_category_translation": {
        "columns": ["product_category_name", "product_category_name_english"],
        "min_rows": 60,
        "rules": [
            *not_null("product_category_name", "product_category_name_english"),
            *unique("product_category_name"),
        ],
    },
}


def bigquery_batches(context, schema: str) -> dict:
    """Run the checks inside BigQuery, so no table data is downloaded."""
    project = os.environ["GCP_PROJECT_ID"]
    location = os.environ.get("BQ_LOCATION", "US")
    source = context.data_sources.add_sql(
        name="olist_raw",
        connection_string=f"bigquery://{project}/{schema}?location={location}",
    )
    return {
        table: source.add_table_asset(
            name=table, table_name=table, schema_name=schema
        ).add_batch_definition_whole_table("all_rows")
        for table in TABLES
    }


def sample_batches(context) -> dict:
    """Read the CI sample CSVs as strings, matching the raw BigQuery tables."""
    source = context.data_sources.add_pandas_filesystem(
        name="olist_sample", base_directory=SAMPLE_DIR
    )
    return {
        table: source.add_csv_asset(
            name=table, dtype=dict.fromkeys(spec["columns"], "str")
        ).add_batch_definition_path("all_rows", path=f"{table}.csv")
        for table, spec in TABLES.items()
    }


def build_suite(table: str, spec: dict, target: str) -> gx.ExpectationSuite:
    # The CI sample has only a few rows, so the load-size check applies to BigQuery only.
    min_rows = spec["min_rows"] if target == "bigquery" else 1
    return gx.ExpectationSuite(
        name=f"raw_{table}",
        expectations=[
            gxe.ExpectTableColumnsToMatchSet(column_set=spec["columns"], exact_match=True),
            gxe.ExpectTableRowCountToBeBetween(min_value=min_rows),
            *spec["rules"],
        ],
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", choices=["bigquery", "sample"], default="bigquery")
    target = parser.parse_args().target
    schema = os.environ.get("BQ_RAW_DATASET", "olist_raw")

    context = gx.get_context(mode="ephemeral")
    context.add_data_docs_site(
        site_name=DOCS_SITE,
        site_config={
            "class_name": "SiteBuilder",
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": str(DOCS_DIR),
            },
            "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
        },
    )
    batches = bigquery_batches(context, schema) if target == "bigquery" else sample_batches(context)

    validations = []
    for table, spec in TABLES.items():
        suite = context.suites.add(build_suite(table, spec, target))
        validations.append(
            context.validation_definitions.add(
                gx.ValidationDefinition(
                    name=f"validate_{table}",
                    data=batches[table],
                    suite=suite,
                )
            )
        )

    checkpoint = context.checkpoints.add(
        gx.Checkpoint(
            name="raw_olist",
            validation_definitions=validations,
            actions=[UpdateDataDocsAction(name="update_data_docs", site_names=[DOCS_SITE])],
            result_format="SUMMARY",
        )
    )
    result = checkpoint.run()

    failed = 0
    for run in result.run_results.values():
        stats = run.statistics
        print(
            f"{run.suite_name}: {stats['successful_expectations']}"
            f"/{stats['evaluated_expectations']} passed"
        )
        for check in run.results:
            if not check.success:
                failed += 1
                config = check.expectation_config
                print(f"  FAILED {config.type} {config.kwargs}: {check.result or check.exception_info}")

    print(f"Data Docs: {DOCS_DIR / 'index.html'}")
    if not result.success:
        sys.exit(f"Validation failed: {failed} expectation(s) did not pass")


if __name__ == "__main__":
    main()
