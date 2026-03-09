import streamlit as st
from databricks import sql
import pandas as pd
import os


st.title("🎬 Movie Data Manager")

# Sidebar for Credentials
with st.sidebar:
    st.header("Connection Settings")
    host = st.text_input("Databricks Host", value="adb-xxxx.azuredatabricks.net")
    http_path = st.text_input("HTTP Path", type="password")
    token = st.text_input("Access Token", type="password")

# --- SECTION 1: INSERT DATA ---
st.header("Add New Rating")
with st.form("movie_form"):
    movie_id = st.number_input("Enter Movie ID", min_value=1, step=1)
    rating = st.slider("Select Rating", min_value=0.0, max_value=10.0, step=0.1)
    submit_button = st.form_submit_button(label="Submit to Databricks")

if submit_button:
    try:
        with sql.connect(server_hostname=host, http_path=http_path, access_token=token) as conn:
            with conn.cursor() as cursor:
                # Use '?' for parameters to keep it secure and clean
                insert_query = "INSERT INTO test.bronze_test_rating.movies (id, rating) VALUES (?, ?)"
                cursor.execute(insert_query, (movie_id, rating))
                st.success(f"✅ Inserted ID {movie_id} with rating {rating}!")
    except Exception as e:
        st.error(f"Upload Error: {e}")

st.divider()


# --- SECTION 2: FETCH DATA ---

st.header("View Recorded Ratings")
if st.button("Fetch Latest Records"):
    try:
        with sql.connect(server_hostname=host, http_path=http_path, access_token=token) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM test.bronze_test_rating.movies ORDER BY id DESC LIMIT 50")
                result = cursor.fetchall()
                
                if result:
                # Convert the result into a Pandas DataFrame
                    df = pd.DataFrame(result, columns=[desc[0] for desc in cursor.description])
                    
                    st.write("Displaying the latest 50 records:")
                    st.dataframe(df, use_container_width=True)
                else:
                    st.warning("The table is currently empty.")
    except Exception as e:
            st.error(f"Fetch Error: {e}")