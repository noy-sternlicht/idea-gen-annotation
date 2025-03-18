import ast
import json
import os
import shutil
import uuid
import pandas as pd
import torch
from pyairtable import Api
from sentence_transformers import SentenceTransformer
from sentence_transformers import util
from tqdm import tqdm

BASE_ID = "appfwulPjVHbYVUDt"
TABLE_NAME = "tblsK9MqSSVgjzy97"
airtable_key_path = "airtable_key"
AIRTABLE_API_KEY = open(airtable_key_path, "r").read().strip()

api = Api(AIRTABLE_API_KEY)
table = api.table(BASE_ID, TABLE_NAME)

DATA_PATH = 'human_study_examples.csv'
USERS_EXPERTISE = 'users_expertise.json'
BATCH_SIZE = 5

baselines = ['random', 'ours', 'gpt-4o', 'sciIE', 'mpnet_zero', 'positive']

model = SentenceTransformer("ibm-granite/granite-embedding-125m-english")


def divide_data_to_batches(data_path, batch_size):
    data = pd.read_csv(data_path)
    print(f"Loaded {len(data)} examples")

    data = data[data.apply(lambda x: len(set(x[baseline].lower() for baseline in baselines)) == len(baselines), axis=1)]
    data['arxiv_categories'] = data['arxiv_categories'].apply(ast.literal_eval)
    print(f'Loaded {len(data)} examples with all baselines')

    corpus_embeddings = model.encode(data['context'].tolist(), show_progress_bar=True, batch_size=1024).tolist()
    id_to_embedding = {}
    for _, row in data.iterrows():
        id_to_embedding[row['id']] = corpus_embeddings[len(id_to_embedding)]

    corpus_embeddings = id_to_embedding

    users_expertise = json.load(open(USERS_EXPERTISE))["users_expertise"]
    user_embeddings = {user: model.encode(user_info['research_areas']) for user, user_info in
                       users_expertise.items()}

    batches_by_user = {}

    used_data = set()

    pbar = tqdm(total=len(data))

    while True:
        old_used_data = len(used_data)
        for user, user_info in users_expertise.items():
            user_data = data[data['arxiv_categories'].apply(
                lambda x: all(category in user_info['arxiv_categories'] for category in x))]

            user_data = user_data[~user_data['id'].isin(used_data)]

            if len(user_data) == 0:
                continue

            user_data = user_data.sample(frac=1)

            embedded_user_corpus = torch.stack(
                [torch.tensor(corpus_embeddings[example_id]) for example_id in user_data['id']])
            embedded_user_query = torch.tensor(user_embeddings[user])

            batch_len = min(batch_size, len(user_data))

            batch = util.semantic_search(embedded_user_query, embedded_user_corpus, score_function=util.cos_sim,
                                         top_k=batch_len)[0]

            batch_example_ids = [batch[i]['corpus_id'] for i in range(len(batch))]
            batch = user_data.iloc[batch_example_ids]


            # print('==============================')
            # print(f"UserQuery: {user_info['research_areas']}")
            # print('---')
            # for i in range(len(batch)):
            #     print(f"----\n{batch.iloc[i]['context']}\n----")

            if user not in batches_by_user:
                batches_by_user[user] = []

            batches_by_user[user].append(batch)
            used_data.update(batch['id'].tolist())

            pbar.update(len(batch))

        if len(used_data) == old_used_data:
            print("Used all data")
            break

    return batches_by_user


def main():
    batch_dir = 'batches'
    if os.path.exists(batch_dir):
        shutil.rmtree(batch_dir)

    os.makedirs(batch_dir)

    data_batches = divide_data_to_batches(DATA_PATH, BATCH_SIZE)

    for user, batches in data_batches.items():
        for i, batch in enumerate(batches):
            batch_id = str(uuid.uuid4())
            batch_path = os.path.join(batch_dir, f"{batch_id}.csv")
            batch.to_csv(batch_path, index=False)
            print(f"Saved batch {i} to {batch_path}")

            table.create({
                "priority": str(i),
                "batch_id": batch_id,
                "file_path": batch_path,
                "annotator": user,
                "status": "not_started"
            })


if __name__ == '__main__':
    main()
