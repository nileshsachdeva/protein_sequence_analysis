import streamlit as st
import os
import fetch_fasta_sequence as fs


def main():
    st.markdown("<h1>Protein Sequence Analysis</h1><br>", unsafe_allow_html=True)
    # st.markdown("<p style='text-align: right; color: #FF4B4B;'>by Abhinav Rana</p>", unsafe_allow_html=True)

    # Step 1: Upload accession number file
    st.markdown("<br><p style='font-size: 24px;'>Step 1: To get started, choose a text file containing accession numbers</p><br>", unsafe_allow_html=True)
    input_file = st.file_uploader(label="Upload file", type=["txt"])

    # Step 2: Generate FASTA sequences
    st.markdown("<br><p style='font-size: 24px;'>Step 2: Fetch FASTA sequences from accession numbers</p>", unsafe_allow_html=True)
    if st.button(label="Click to generate FASTA sequences", type="primary"):
        if input_file is not None:
            fasta_file_content, total_accessions = fs.generate_fasta_file(input_file)

            # Save to session state
            st.session_state["fasta_file_content"] = fasta_file_content
            st.session_state["total_accessions"] = total_accessions

            # Save locally for BLAST
            with open("sequences.fasta", "w") as f:
                f.write(fasta_file_content.getvalue().decode("utf-8"))

            st.success("FASTA file generated successfully!")

        else:
            st.error("Please upload an input file.")
    
    # Retrieve from session state
    fasta_file_content = st.session_state.get("fasta_file_content", None)
    total_accessions = st.session_state.get("total_accessions", 0)

    if fasta_file_content:
        st.download_button(label="Download FASTA file having sequences",
                           data=fasta_file_content.getvalue(),
                           file_name='sequences.fasta',
                           mime='text/plain')


if __name__ == "__main__":
    main()