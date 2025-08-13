# Updated app.py to handle PySpark DataFrame outputs from fetch_fasta_sequence.py

import streamlit as st
import re
import fetch_fasta_sequence as fs

def main():
    st.title("Protein Sequence Analysis")

    st.markdown("#### Step 1: Provide accession numbers")
    input_method = st.radio("Select input method:", ["Manual entry", "Upload file"])

    accession_list = []

    if input_method == "Manual entry":
        manual_input = st.text_area("Enter accession numbers (comma, space, or newline separated):")
        if manual_input.strip():
            accession_list = [x.strip() for x in re.split(r"[\s,]+", manual_input.strip()) if x.strip()]
    else:
        uploaded_file = st.file_uploader("Upload .txt file containing accession numbers", type=["txt"])
        if uploaded_file:
            accession_list = [line.strip() for line in uploaded_file.read().decode("utf-8").splitlines() if line.strip()]

    if st.button("Fetch / Generate FASTA"):
        if not accession_list:
            st.error("Please provide accession numbers.")
        else:
            spark_df = fs.generate_fasta_file(accession_list)
            if spark_df is not None and spark_df.count() > 0:
                st.success("Sequences fetched successfully.")
                # Convert Spark DataFrame to pandas for display and file download
                pandas_df = spark_df.toPandas()
                st.dataframe(pandas_df)

                csv_data = pandas_df.to_csv(index=False).encode("utf-8")
                st.download_button("Download CSV", csv_data, file_name="sequences.csv", mime="text/csv")

                fasta_data = "".join([f">{row['accession_id']} {row['description']}\n{row['sequence']}\n" for _, row in pandas_df.iterrows()])
                st.download_button("Download FASTA", fasta_data.encode("utf-8"), file_name="sequences.fasta", mime="text/plain")
            else:
                st.warning("No sequences found.")


if __name__ == "__main__":
    main()
