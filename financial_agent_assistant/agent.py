from openai import OpenAI
from dotenv import load_dotenv
from openai.types.chat.chat_completion import ChatCompletion
import streamlit as st
import json
import os
from agent_clickhouse_query import extract_from_db


### Initial configurations
load_dotenv()
client = OpenAI()

LLM = os.environ.get("MODEL")

with open("function_calling.json", encoding="utf-8") as file:
    tools = json.load(file)

with open("schema.json", "r", encoding="utf-8") as file:
    schema = json.load(file)

with open("system_message.json", "r", encoding="utf-8") as file:
    system_messages = json.load(file)

if "chat_history" not in st.session_state:
    st.session_state['chat_history'] = system_messages

### Layout
st.set_page_config(layout="wide", page_title="Chat Application")
col1, col2 = st.columns(2)
with col1:
    st.image(os.path.join(os.getcwd(), "images/assistant.png"), width=300)

with col2:
    st.title("""
        :blue[Virtue Cooperative]
                Hi I'm Financial Assistant
                How can I help you?
    """)

st.markdown(
    """
    <style>
        /* Change sidebar background color */
        [data-testid="stSidebar"] {
            background-color: #696969 !important; /* Blue */
        }

        /* Change all text color inside the sidebar */
        [data-testid="stSidebar"] * {
            color: white !important;
        }

        /* Specifically change sidebar title and subheader color */
        [data-testid="stSidebar"] h1, 
        [data-testid="stSidebar"] h2 {
            color: white !important;
        }

        /* Change all buttons background color */
        [data-testid="stButton"] button {
            background-color: 	#181818 !important; /* White background */
            color: white !important; /* Blue text */
            border-radius: 10px !important; /* Rounded corners */
            padding: 10px 20px !important; /* Adjust padding */
            font-size: 16px !important; /* Adjust font size */
        }

        [data-testid="stButton"] button p {
            color: white !important;  /* Ensure text inside button is blue */
        }
    </style>
    """,
    unsafe_allow_html=True
)

with st.sidebar:
    st.title("Financial Consultant")  # This will be white
    st.subheader("Features: ")  # This will also be white
    st.write("- Financial Analysis")
    st.write("- Budgeting and Forecasting")
    st.write("- Investment Strategies")
    st.write("- Risk Management")
    st.write("- Provision financial information")
    st.write("- Cost Reduction")
    st.write("- Tax Optimization")
    st.write("- Strategic Planning")

def call_task(name, args):
    if name == "extract_from_db":
        return extract_from_db(**args)

def ask_openai(chat_history):
    try:
        response = client.chat.completions.create(
            model=LLM,
            messages= chat_history,
            tools=tools,
            tool_choice='auto',
            # stream=True
        )
        return response

    except Exception as e:
        print(e)

def process_chat_history():
    response = ask_openai(st.session_state['chat_history'])
    message = response.choices[0].message
    if message.tool_calls and len(message.tool_calls) != 0:
        function = message.tool_calls[0].function
        function_name = function.name
        arguments = json.loads(function.arguments)
        tool_call_id = message.tool_calls[0].id
        result = call_task(function_name, arguments)
        supply_result_to_model = {
            "role": "tool",
            "content": str(result),
            "tool_call_id": tool_call_id
        }
        try:
            st.session_state['chat_history'].append(message)
            st.session_state['chat_history'].append(supply_result_to_model)
        except Exception as e:
            st.write(f"An error occurrred: {e}")

def ask_openai_stream(chat_history, stream=True):
    response = client.chat.completions.create(
        model=LLM,
        messages=chat_history,
        stream=stream
    )

    return response

def response_generator(chat_history):
    for chunk in ask_openai_stream(chat_history, stream=True):
        if chunk.choices and chunk.choices[0].delta.content:
            yield chunk.choices[0].delta.content


#display chat history
for history in st.session_state['chat_history']:
    if isinstance(history, dict):
        if history['role'] == 'assistant':
            st.chat_message("ai").write(history['content'])
        if history['role'] == 'user':
            st.chat_message("human").write(history['content'])

def run_application():
    prompt = st.chat_input("Message Virtual financial consultant", key="chat_input")
    if prompt:
        st.chat_message("human").write(prompt)
        print(st.session_state['chat_history'])
        st.session_state['chat_history'].append({'role': "user", "content": prompt})
        process_chat_history()
        output = response_generator(st.session_state['chat_history'])
        with st.chat_message("ai"):
            ai_message = st.write_stream(output)
        st.session_state['chat_history'].append({'role': "assistant", "content": ai_message})

if __name__ == '__main__':
  run_application()





