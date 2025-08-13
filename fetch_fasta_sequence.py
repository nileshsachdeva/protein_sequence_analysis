from Bio import Entrez
import streamlit as st
import re
import databricks.sql as dbsql
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType, StructType, StructField
import tempfile

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()


def get_dbx_connection():
    return dbsql.connect(
        server_hostname=st.secrets["DATABRICKS_SERVER"],
        http_path=st.secrets["DATABRICKS_HTTP_PATH"],
        access_token=st.secrets["DATABRICKS_TOKEN"]
    )


def fetch_from_uc_table(accessions, uc_table_name="workspace.raw.sequence"):
    if not accessions:
        # Return empty Spark DataFrame with schema
        schema = StructType([
            StructField("accession_id", StringType(), True),
            StructField("description", StringType(), True),
            StructField("sequence", StringType(), True),
        ])
        return spark.createDataFrame([], schema)

    # Build comma-separated quoted list for SQL IN clause safely
    comma_sep_accessions = ",".join([f"'{acc}'" for acc in accessions])
    query = f"SELECT accession_id, description, sequence FROM {uc_table_name} WHERE accession_id IN ({comma_sep_accessions})"
    with get_dbx_connection() as conn:
        # Fetch as pandas then convert to Spark DataFrame (Databricks SQL connector returns pandas)
        pdf = pd.read_sql(query, conn)
    if pdf.empty:
        schema = StructType([
            StructField("accession_id", StringType(), True),
            StructField("description", StringType(), True),
            StructField("sequence", StringType(), True),
        ])
        return spark.createDataFrame([], schema)
    return spark.createDataFrame(pdf)


def update_uc_table(new_data_spark_df, uc_table_name="workspace.raw.sequence"):
    if new_data_spark_df.rdd.isEmpty():
        return
    # Write new_data_spark_df to a temporary Delta table, then MERGE INTO UC_TABLE_NAME
    temp_view = "temp_updates"
    new_data_spark_df.createOrReplaceTempView(temp_view)
    merge_sql = f"""
    MERGE INTO {uc_table_name} AS target
    USING {temp_view} AS source
    ON target.accession_id = source.accession_id
    WHEN MATCHED THEN UPDATE SET target.description = source.description, target.sequence = source.sequence
    WHEN NOT MATCHED THEN INSERT (accession_id, description, sequence)
    VALUES (source.accession_id, source.description, source.sequence)
    """
    spark.sql(merge_sql)


def fetch_from_ncbi(accession):
    Entrez.email = st.secrets["NCBI_EMAIL"]
    try:
        handle = Entrez.efetch(db="protein", id=accession, rettype="fasta", retmode="text")
        record = handle.read()
        handle.close()
        return record
    except Exception as e:
        st.error(f"Failed to retrieve {accession}: {str(e)}")
        return None


def parse_fasta_header(fasta_str):
    if not fasta_str:
        return None, None, None
    lines = fasta_str.strip().splitlines()
    header = lines[0]
    m = re.match(r">(\S+)\s*(.*)", header)
    accession_id = m.group(1) if m else None
    description = m.group(2) if m else ""
    sequence = "".join(lines[1:])
    return accession_id, description, sequence


def generate_fasta_file(input_accessions):
    input_accessions = list(set(input_accessions))

    existing_df = fetch_from_uc(input_accessions)

    # Identify accessions missing or with null/empty sequence
    missing_df = existing_df.filter((col("sequence").isNull()) | (col("sequence") == ""))
    existing_accessions = [row.accession_id for row in existing_df.collect()]
    to_fetch = [acc for acc in input_accessions if acc not in existing_accessions or acc in [row.accession_id for row in missing_df.collect()]]

    new_rows = []
    if to_fetch:
        progress_bar = st.progress(0)
        for i, acc in enumerate(to_fetch):
            fasta_str = fetch_from_ncbi(acc)
            accession_id, desc, seq = parse_fasta_header(fasta_str)
            if accession_id:
                new_rows.append({"accession_id": accession_id, "description": desc, "sequence": seq})
            progress_bar.progress((i + 1) / len(to_fetch))

        if new_rows:
            new_df = spark.createDataFrame(new_rows)
            update_uc_table(new_df)
            # Refresh existing_df after update
            existing_df = fetch_from_uc(input_accessions)

    # Filter for requested accessions only
    final_df = existing_df.filter(col("accession_id").isin(input_accessions))

    return final_df

