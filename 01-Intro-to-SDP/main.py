import os

from pyspark.sql import SparkSession

from pyspark.pipelines.graph_element_registry import graph_element_registration_context
from pyspark.pipelines.spark_connect_graph_element_registry import (
    SparkConnectGraphElementRegistry
)
from pyspark.pipelines.spark_connect_pipeline import (
    create_dataflow_graph,
    start_run,
    handle_pipeline_events,
)
from pyspark import pipelines as sdp
from pyspark.sql.functions import col, regexp_extract, when

from log_generator import generate_mock_web_logs


def run_pipeline():

    # Initialize a new Spark Session
    spark = (SparkSession.builder
                .appName("Hands-on-with-SDP")
                .master("local[*]")
                .getOrCreate())

    # Step 1: Create an empty data flow graph
    dataflow_graph_id = create_dataflow_graph(
        spark,
        default_catalog=None,
        default_database=None,
        sql_conf=None
    )
    print("Dataflow graph ID: ", dataflow_graph_id)

    # Step 2: Create an element registry
    registry = SparkConnectGraphElementRegistry(spark, dataflow_graph_id)
    print("Spark connect graph registry: ", registry)

    with graph_element_registration_context(registry):

        # Define a streaming table
        sdp.create_streaming_table("raw_traffic_logs")

        # Create a flow to
        @sdp.append_flow(target="raw_traffic_logs")
        def ingest_raw_traffic_logs():
            """
            Read JSON web traffic logs as a Spark streaming DataFrame.
            :return: Spark streaming DataFrame.
            """
            input_dir = "web_traffic_logs"
            df = (
                spark.readStream
                    .format("json")
                    .option("multiLine", "false")
                    .schema(
                        "id STRING, timestamp BIGINT, "
                        "message STRUCT<ip:STRING, method:STRING, page:STRING, status:INT, "
                        "user_agent:STRING, response_time_ms:INT>"
                    )
                    .load(f"{input_dir}/*")
            )
            return df

        # Define a streaming table for our enriched data
        sdp.create_streaming_table("augmented_traffic_logs")

        # Create a flow from the raw logs table to this new enriched table
        @sdp.append_flow(target="augmented_traffic_logs")
        def augment_web_traffic_logs():
            """
            Add calculated columns to enrich the raw web traffic logs.

            Added columns:
            - `product_id`: Extracts product ID from /products/{id} URLs
            - `is_error`: Flags requests with 4xx/5xx status codes
            - `is_mobile`: Identifies mobile vs. desktop traffic by user agent
            - `page_type`: Classifies page as 'home', 'product', 'cart', 'checkout', or 'other'
            - `payload_size`: Mock size metric based on response_time (as a proxy)

            :return: Augmented DataFrame with new calculated columns.
            """
            augmented_df = (
                spark.readStream.table("raw_traffic_logs")
                # Extract product ID from product pages
                .withColumn(
                    "product_id",
                    regexp_extract(col("message.page"), r"/products/(\d+)", 1)
                )
                # Flag error responses
                .withColumn(
                    "is_error",
                    when(col("message.status") >= 400, True).otherwise(False)
                )
                # Detect mobile traffic (very simplified)
                .withColumn(
                    "is_mobile",
                    when(col("message.user_agent").rlike("iPhone|Android"), True).otherwise(False)
                )
                # Categorize page type
                .withColumn(
                    "page_type",
                    when(col("message.page") == "/", "home")
                    .when(col("message.page").rlike("^/products"), "product")
                    .when(col("message.page") == "/cart", "cart")
                    .when(col("message.page") == "/checkout", "checkout")
                    .otherwise("other")
                )
                # Fake payload size metric from response_time
                .withColumn(
                    "payload_size_kb",
                    (col("message.response_time_ms") / 10).cast("int")
                )
            )

            return augmented_df

    pipeline_results = start_run(
        spark,
        dataflow_graph_id,
        full_refresh=None,
        refresh=["raw_traffic_logs", "augmented_traffic_logs"],
        full_refresh_all=False,
        dry=False,
    )

    handle_pipeline_events(pipeline_results)

def generate_logs():
    """Generates web traffic logs if they don't exist."""
    log_path = "web_traffic_logs"
    if not os.path.isdir(log_path) or not os.listdir(log_path):
        generate_mock_web_logs(log_path, 5, 100)


if __name__ == '__main__':
    generate_logs()
    run_pipeline()
