import streamlit as st
from databricks import sql
import pandas as pd
import os
import google.generativeai as genai


st.title("🎬 Movie Data Manager")

# Sidebar for Credentials
with st.sidebar:
    st.header("Connection Settings")
    host = st.text_input("Databricks Host", value="adb-xxxx.azuredatabricks.net")
    http_path = st.text_input("HTTP Path", type="password")
    token = st.text_input("Access Token", type="password")
    GEMINI_API_KEY = st.text_input("Gemini API Key", type="password")


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

st.divider()

# --- SECTION 3: CHATBOT ---



genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-3-flash-preview')

TABLE_SCHEMA = """
Table Name: movie.movie_gold.movie_review_rating_netflix
Columns:
 - platform_id : string
 - platform_name : string
 - IMDb_rating : string
 - Rotten_Tomatoes_rating : string
"""

# --- Helper Functions ---
@st.cache_resource
def get_db_connection():
    """Establish connection to Databricks SQL Warehouse."""
    return sql.connect(
        server_hostname=host,
        http_path=http_path,
        access_token=token
    )

def execute_sql(query):
    """Execute SQL query on Databricks and return a pandas DataFrame."""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            return pd.DataFrame(result, columns=columns)
    except Exception as e:
        return f"Error executing query: {str(e)}"

def generate_sql(user_question):
    """Ask Gemini to translate the user question into a SQL query."""
    prompt = f"""
    You are an expert Databricks SQL developer. Convert the user's question into a valid SQL query.
    Return ONLY the raw SQL query without any markdown formatting, code blocks, or explanations.
    
    Schema:
    {TABLE_SCHEMA}
    
    Question: {user_question}
    """
    response = model.generate_content(prompt)
    # Strip any markdown backticks if Gemini accidentally includes them
    return response.text.replace('```sql', '').replace('```', '').strip()

def synthesize_response(user_question, data):
    """Ask Gemini to format the raw data into a human-readable answer."""
    prompt = f"""
    You are a helpful data assistant. Answer the user's question based on the provided data results.
    Keep the answer concise, accurate, and conversational.
    
    User Question: {user_question}
    Data Results: \n{data.to_string() if isinstance(data, pd.DataFrame) else data}
    """
    response = model.generate_content(prompt)
    return response.text

# --- Streamlit UI ---
st.header("📊 Databricks AI Assistant")
st.markdown("Ask questions about your data in plain English.")

# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display chat messages from history on app rerun
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# React to user input
if prompt := st.chat_input("E.g., What were the total sales last month?"):
    # Display user message in chat message container
    st.chat_message("user").markdown(prompt)
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})

    with st.spinner("Analyzing your request..."):
        # 1. Generate SQL
        sql_query = generate_sql(prompt)
        
        # 2. Execute SQL
        data_result = execute_sql(sql_query)
        
        # 3. Generate Final Answer
        if isinstance(data_result, str) and data_result.startswith("Error"):
            final_answer = f"I encountered an issue retrieving the data: {data_result}"
        else:
            final_answer = synthesize_response(prompt, data_result)
            
        # Display assistant response
        with st.chat_message("assistant"):
            st.markdown(final_answer)
            with st.expander("View Generated SQL & Raw Data"):
                st.code(sql_query, language="sql")
                if isinstance(data_result, pd.DataFrame):
                    st.dataframe(data_result)
        
        # Add assistant response to chat history
        st.session_state.messages.append({"role": "assistant", "content": final_answer})