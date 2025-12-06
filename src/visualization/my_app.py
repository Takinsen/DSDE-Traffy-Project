import streamlit as st
import pandas as pd

st.title("My Traffy Dashboard")
st.write("Hello Data Science!")

df = pd.read_csv("../../data/cleansed/bangkok_traffy_clean.csv")
st.dataframe(df.head())
