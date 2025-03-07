from pyairtable import Api
import json
import pandas as pd
import os

airtable_key_path = "airtable_key"
API_KEY = open(airtable_key_path, "r").read().strip()

api = Api(API_KEY)
annotations_table = api.table("appkIcAOOsCPyyrmU", "tblx7STGTFWCMhrvg")


def main():
    records = annotations_table.all()

    baselines_results = []

    for record in records:
        record_baselines_results = json.loads(record['fields']['baselines_results'])
        for baseline, results in record_baselines_results.items():
            baselines_results.append({
                'example_id': record['fields']['example_id'],
                'annotator': record['fields']['annotator'],
                'context': record['fields']['context'],
                'gold': record['fields']['gold'],
                'baseline': baseline,
                'suggestion': results['suggestion'],
                'k': results['k'],
                'sci_sense': results['sci_sense'],
                'novelty': results['og'],
                'specificity': results['specific']
            })

    baselines_results_df = pd.DataFrame(baselines_results)
    baselines_results_df.to_csv('baselines_results.csv', index=False)
    print('Saved baselines results to baselines_results.csv')

    for criteria in ['sci_sense', 'novelty', 'specificity']:
        criteria_results = baselines_results_df[['baseline', criteria]]
        histogram = criteria_results.pivot_table(index="baseline", columns=criteria, aggfunc="size", fill_value=0)
        print(histogram)


if __name__ == '__main__':
    main()
