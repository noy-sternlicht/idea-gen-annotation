import json
import os
import uuid
import pandas as pd
from pyairtable import Api

# users_path = 'users.json'
# users = json.load(open(users_path))['users']
#
# data = 'human_study_examples.csv'
# nr_examples_per_annotator = 10
#
# data_df = pd.read_csv(data)
# users_dir = 'users'
# if not os.path.exists(users_dir):
#     os.makedirs(users_dir)
#
# sampled_data_ids = set()
#
# for user in users:
#     unused_data = data_df[~data_df['id'].isin(sampled_data_ids)]
#     sampled_data = unused_data.sample(nr_examples_per_annotator)
#     sampled_data_ids.update(sampled_data['id'])
#     user_data = os.path.join(users_dir, f"{user['id']}.csv")
#     sampled_data.to_csv(user_data, index=False)
#     print(f"Saved {nr_examples_per_annotator} examples for user {user['id']} to {user_data}")


BASE_ID = "appfwulPjVHbYVUDt"
TABLE_NAME = "tblsK9MqSSVgjzy97"
airtable_key_path = "airtable_key"
AIRTABLE_API_KEY = open(airtable_key_path, "r").read().strip()

api = Api(AIRTABLE_API_KEY)
table = api.table(BASE_ID, TABLE_NAME)


def divide_data_to_batches(data_path, batch_size):
    data = pd.read_csv(data_path)
    data = data.sample(frac=1)
    nr_batches = len(data) // batch_size
    data_batches = []
    for i in range(nr_batches):
        batch = data.iloc[i * batch_size: (i + 1) * batch_size]
        data_batches.append(batch)
    return data_batches


def main():
    data_path = 'human_study_examples.csv'
    batch_dir = 'batches'
    os.makedirs(batch_dir)

    batch_size = 3
    data_batches = divide_data_to_batches(data_path, batch_size)
    for i, batch in enumerate(data_batches):
        batch_id = str(uuid.uuid4())
        batch_path = os.path.join(batch_dir, f"{batch_id}.csv")
        batch.to_csv(batch_path, index=False)
        print(f"Saved batch {i} to {batch_path}")

        table.create({
            "batch_id": batch_id,
            "file_path": batch_path,
            "annotator": "",
            "status": "not_started"
        })


if __name__ == '__main__':
    main()
