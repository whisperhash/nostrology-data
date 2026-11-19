"""A complete set of the variables PipelineConfig requires (pipeline/config.py REQUIRED), for tests.

The deployment's own AWS coordinates have no default, so a test that builds a config supplies these.
The bucket names hold underscores, which S3 refuses in a new bucket's name, so none could ever be a
real bucket."""

FAKE_ENV = {
    "NOSTROLOGY_AURORA_CLUSTER": "example-cluster",
    "NOSTROLOGY_ATHENA_WORKGROUP": "example-wg",
    "NOSTROLOGY_EXTRACTS_BUCKET": "example_extracts",
    "NOSTROLOGY_GLUE_DB": "example_db",
    "NOZZL_ARCHIVE_BUCKET": "example_archive",
}
