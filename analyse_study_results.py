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
                'specificity': results['specific'],
                'interest': results['interest']
            })

    baselines_results_df = pd.DataFrame(baselines_results)
    baselines_results_df.to_csv('baselines_results.csv', index=False)
    print('Saved baselines results to baselines_results.csv')

    results = []
    labels = ['Low', 'Med', 'High']
    mean_scores = {}
    for criteria in ['sci_sense', 'novelty', 'specificity', 'interest']:
        criteria_results = baselines_results_df[['baseline', criteria]]
        for baseline in criteria_results['baseline'].unique():
            baseline_results = criteria_results[criteria_results['baseline'] == baseline]
            histogram = baseline_results[criteria].value_counts().to_dict()
            for label in labels:
                if label not in histogram:
                    histogram[label] = 0
            results.append({
                'criteria': criteria,
                'baseline': baseline,
                'Low': histogram['Low'],
                'Med': histogram['Med'],
                'High': histogram['High']
            })

            if baseline not in mean_scores:
                mean_scores[baseline] = {}
            mean_scores[baseline][criteria] = (histogram['Low'] + 2 * histogram['Med'] + 3 * histogram['High']) / len(
                baseline_results)


    results_df = pd.DataFrame(results)
    print(results_df)
    results_df.to_csv('baselines_results_aggregated.csv', index=False)

    mean_scores_df = pd.DataFrame(mean_scores).T.round(2)
    print(mean_scores_df)
    mean_scores_df.to_csv('baselines_mean_scores.csv')


if __name__ == '__main__':
    main()
