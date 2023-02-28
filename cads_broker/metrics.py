import prometheus_client

GENERATED_BYTES_COUNTER = prometheus_client.Counter(
    "generated_bytes_counter", "Total bytes generated in successful requests"
)
