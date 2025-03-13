import streamlit as st
import pandas as pd
import random
import re
import json

from pyairtable import Api

ANNOTATION_BASE_ID = "appkIcAOOsCPyyrmU"
ANNOTATIONS_TABLE_NAME = "tblx7STGTFWCMhrvg"
airtable_key_path = "airtable_key"
API_KEY = open(airtable_key_path, "r").read().strip()

api = Api(API_KEY)
annotations_table = api.table(ANNOTATION_BASE_ID, ANNOTATIONS_TABLE_NAME)
batches_table = api.table('appfwulPjVHbYVUDt', 'tblsK9MqSSVgjzy97')
baselines = ['random', 'ours', 'gpt-4o', 'sciIE', 'mpnet_zero']


def build_query(anchor_text, relation):
    if relation == 'combination':
        query = f"What could we blend with **{anchor_text}** to address the context?"
    else:
        anchor_text = anchor_text.capitalize()
        query = f"What would be a good source of inspiration for **{anchor_text}**?"

    return query


def send_to_airtable(df):
    batch_id = st.session_state.batch_id
    chunk_size = 10
    for idx in range(0, len(df), chunk_size):
        chunk = df.iloc[idx:idx + chunk_size]
        records = []
        for _, row in chunk.iterrows():
            record = {
                "example_id": row['id'],
                "batch_id": batch_id,
                "context": row['context'],
                "query": row['query'],
                "gold": row['gold'],
                "annotator": row['annotator'],
            }
            baselines_results = {}
            for curr_baseline in baselines:
                baselines_results[curr_baseline] = {
                    "suggestion": row[f'{curr_baseline}_suggestion'],
                    'k': row[f'{curr_baseline}_k'],
                    "sci_sense": row[f'{curr_baseline}_sci_sense'],
                    "og": row[f'{curr_baseline}_og'],
                    "specific": row[f'{curr_baseline}_specific'],
                    "interest": row[f'{curr_baseline}_interest'],
                }
            record['baselines_results'] = json.dumps(baselines_results)
            records.append(record)
        annotations_table.batch_create(records)

    records = batches_table.all(formula=f"{{batch_id}} = '{batch_id}'")
    if records:
        record_id = records[0]['id']
        batches_table.update(record_id, {"status": "done"})
        print(f"Batch {batch_id} marked as Completed.")
    else:
        print(f"Batch {batch_id} not found.")


def get_user_data_chunk(user_email):
    if 'tom' in user_email:
        records = batches_table.all(formula="{batch_id} = '162b21af-9ce9-45be-aef0-450c9cd9d6e0'")
    else:
        records = batches_table.all(formula="{status} = 'not_started'")
    if records:
        first_batch = records[0]
        record_id = first_batch['id']
        batch_path = first_batch['fields'].get("file_path")
        batch_id = first_batch['fields'].get("batch_id")

        batches_table.update(record_id, {
            "annotator": user_email,
            "status": "in_progress"
        })

        print(f"Assigned batch {batch_id} to {user_email}.")
        batch = pd.read_csv(batch_path)
        st.session_state.batch_id = batch_id
        return batch

    else:
        print("No available batches to assign.")
        return None


# Initialize session state variables
if "email_entered" not in st.session_state:
    st.session_state.email_entered = False
if "user_email" not in st.session_state:
    st.session_state.user_email = ""
if "user_name" not in st.session_state:
    st.session_state.user_name = ""
if "data_chunk" not in st.session_state:
    st.session_state.data_chunk = None
if "batch_id" not in st.session_state:
    st.session_state.batch_id = ""
if 'save_path' not in st.session_state:
    st.session_state.save_path = ""
if "current_example" not in st.session_state:
    st.session_state.current_example = 0
if "annotations" not in st.session_state:
    st.session_state.annotations = []
if "shuffled_baselines" not in st.session_state:
    st.session_state.shuffled_baselines = {}
if "finished" not in st.session_state:
    st.session_state.finished = False

# Email entry screen
if not st.session_state.email_entered:
    # Page title
    st.markdown("<h1 style='text-align: center;'>Welcome 🤗</h1>", unsafe_allow_html=True)
    st.write("<h3 style='text-align: center;'>Thank you for contributing to our research!</h3>", unsafe_allow_html=True)
    st.markdown(
        "This platform is designed to collect **annotations on the helpfulness of AI-generated ideas** for our study.")

    # Add some spacing
    st.markdown("<br>", unsafe_allow_html=True)

    # Center the form
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.write("<h6 style='text-align: left;'>Enter your email to begin:</h6>", unsafe_allow_html=True)
        with st.form(key="email_form"):
            email = st.text_input("**Email Address**", key="email_input", placeholder="Enter your email...")


            def is_valid_email(email):
                return bool(re.match(r"^[\w\.-]+@[\w\.-]+\.\w{2,}$", email))  # Regex validation


            submit = st.form_submit_button("🚀 Submit")

            if submit:
                if is_valid_email(email):
                    username = email.split("@")[0]
                    st.session_state.email_entered = True
                    st.session_state.user_email = email
                    st.session_state.user_name = username

                    # Fetch user-specific data
                    st.session_state.data_chunk = get_user_data_chunk(email)
                    if st.session_state.data_chunk.empty:
                        st.warning("⚠️ No tasks available. Please contact the study organizer.")

                    st.success(f"✅ Welcome, {username}! Redirecting you now...")
                    st.rerun()
                else:
                    st.warning("⚠️ Please enter a valid email (e.g., user@example.com).")

elif not st.session_state.finished:
    st.markdown(
        """
        <style>
            .block-container {
                max-width: 45%;  /* Adjust width as needed */
                margin: auto;  /* Centers the content */
            }
        </style>
        """,
        unsafe_allow_html=True
    )
    current_example = st.session_state.current_example
    example = st.session_state.data_chunk.iloc[current_example]

    if current_example not in st.session_state.shuffled_baselines:
        random.shuffle(baselines)
        st.session_state.shuffled_baselines[current_example] = baselines
    else:
        baselines = st.session_state.shuffled_baselines[current_example]
    st.subheader("Task: Evaluating AI-Generated Ideas", divider="blue")
    st.info(
        "##### ℹ️ Instructions\n\n"
        "Your goal is to assess the helpfulness of AI-generated suggestions in assisting users with writing paper abstracts.\n\n"
        "You will be provided with:\n"
        "1. **A context** describing a problem, specific settings, goal, etc.\n"
        "2. **A query** asking for a suggestion to help with the context.\n"
        "3. **A list of AI-generated suggestions**.\n\n"
        "For each suggestion, assign a score of **Low** | **Medium** | **High** in the following criteria: \n"
        "- 🧩 **Scientific Soundness** – Is it relevant and scientifically valid?\n"
        "- 💡 **Novelty** – Is it innovative in relation to existing works?\n"
        "- 🎯 **Specificity** – Is it well-defined and not overly broad?\n"
        "- 🤔 **Interest** – Is it engaging or thought-provoking?\n\n"
    )

    anchor = example['anchor']
    relation = example['relation']
    context = example['context']
    query_text = build_query(anchor, relation)

    with st.form(key=f'form_{current_example}'):
        st.markdown('##### Context')
        st.markdown(f"{context[0].capitalize() + context[1:]}")
        st.markdown("##### Query")
        st.markdown(f"{query_text[0].capitalize() + query_text[1:]}")
        st.markdown('##### Suggestions')
        annotations = {'context': context, 'query': query_text,
                       'gold': example['positive'],
                       'annotator': st.session_state.user_email,
                       'id': example['id']}
        for i, baseline in enumerate(baselines, start=1):
            st.markdown(f"**{i}.  {example[baseline].capitalize()}**")
            # st.markdown(f"**{i}.  {example[baseline].capitalize()}** [🐞-{baseline}]")

            cols = st.columns(6)  # Compact layout
            annotations[f'{baseline}_sci_sense'] = cols[1].radio(
                "🧩 **Sound?**", ["Low", "Med", "High"],
                horizontal=False, key=f'sci_{current_example}_{baseline}'
            )
            annotations[f'{baseline}_og'] = cols[2].radio(
                "💡 **Novel?**", ["Low", "Med", "High"],
                horizontal=False, key=f'og_{current_example}_{baseline}'
            )
            annotations[f'{baseline}_specific'] = cols[3].radio(
                "🎯 **Specific?**", ["Low", "Med", "High"],
                horizontal=False, key=f'specific_{current_example}_{baseline}'
            )

            annotations[f'{baseline}_interest'] = cols[4].radio(
                "🤔 **Interesting?**", ["Low", "Med", "High"],
                horizontal=False, key=f'interest_{current_example}_{baseline}'
            )

            st.markdown("<hr>", unsafe_allow_html=True)

            annotations[f'{baseline}_suggestion'] = example[baseline]
            annotations[f'{baseline}_k'] = str(example['k'])

        submitted = st.form_submit_button("Submit & Proceed ➡️")
        if submitted:
            st.session_state.annotations.append(annotations)
            st.session_state.current_example += 1 if current_example < len(st.session_state.data_chunk) - 1 else 0
            st.session_state.finished = current_example >= len(st.session_state.data_chunk) - 1
            st.rerun()

    st.progress((current_example + 1) / len(st.session_state.data_chunk))
    st.markdown(f"Task {current_example + 1} of {len(st.session_state.data_chunk)}")

else:
    st.markdown("<h2 style='text-align: center;'>🎉 You have completed all your tasks! 🎉</h2>", unsafe_allow_html=True)

    # Center the thank-you message
    col1, col2, col3 = st.columns([1, 3, 1])
    with col2:
        st.markdown("<h3 style='text-align: center;'>Thank you for your contribution! 🥹</h3>", unsafe_allow_html=True)

    # Fetch user annotations
    annotations_df = pd.DataFrame(st.session_state.get("annotations", []))

    st.divider()
    st.balloons()  # 🎈 Confetti effect!
    send_to_airtable(annotations_df)
