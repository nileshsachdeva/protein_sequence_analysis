import streamlit as st
import pandas as pd
import os



def main():
    st.markdown("<h1>Protein Sequence Analysis App</h1><br>", unsafe_allow_html=True)
    # st.markdown("<p style='text-align: right; color: #FF4B4B;'>by Abhinav Rana</p>", unsafe_allow_html=True)

    # Step 1: Upload accession number file
    st.markdown("<br><p style='font-size: 24px;'>Step 1: To get started, choose a text file containing accession numbers</p><br>", unsafe_allow_html=True)
    input_file = st.file_uploader(label="Upload file", type=["txt"])


if __name__ == "__main__":
    main()