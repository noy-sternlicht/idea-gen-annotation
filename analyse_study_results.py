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


    support = 0

    for record in records:
        record_baselines_results = json.loads(record['fields']['baselines_results'])
        is_ill_defined = 'is_ill_defined' in record['fields']
        if not is_ill_defined:
            support += 1

        knowledge_level = record['fields']['knowledge_level']

        for baseline, results in record_baselines_results.items():
            baselines_results.append({
                'example_id': record['fields']['example_id'],
                'annotator': record['fields']['annotator'],
                'context': record['fields']['context'],
                'gold': record['fields']['gold'],
                'baseline': baseline,
                'suggestion': results['suggestion'],
                'is_ill_defined': is_ill_defined,
                'knowledge_level': knowledge_level,
                'k': results['k'],
                'rank': results['rank'],
                'normalized_rank': 0.2 * float(knowledge_level) * float(results['rank'])
            })

    baselines_results_df = pd.DataFrame(baselines_results)
    baselines_results_df = baselines_results_df[~baselines_results_df['is_ill_defined']]
    baselines_results_df.to_csv('baselines_results.csv', index=False)
    print('Saved baselines results to baselines_results.csv')


    results = pd.DataFrame()
    results['meanRank'] = baselines_results_df.groupby('baseline')['rank'].mean().round(2)
    results['meanRankNorm'] = baselines_results_df.groupby('baseline')['normalized_rank'].mean().round(2)
    results['medianRank'] = baselines_results_df.groupby('baseline')['rank'].median().round(2)
    results['medianRankNorm'] = baselines_results_df.groupby('baseline')['normalized_rank'].median().round(2)


    print(results)
    print(f'Support: {support}')


    results_summary_path = 'baselines_results_summary.csv'
    results.to_csv(results_summary_path)





    # labels = ['Low', 'Med', 'High']
    # mean_scores = {}
    # for criteria in ['sci_sense', 'novelty', 'specificity', 'interest']:
    #     criteria_results = baselines_results_df[['baseline', criteria]]
    #     for baseline in criteria_results['baseline'].unique():
    #         baseline_results = criteria_results[criteria_results['baseline'] == baseline]
    #         histogram = baseline_results[criteria].value_counts().to_dict()
    #         for label in labels:
    #             if label not in histogram:
    #                 histogram[label] = 0
    #         results.append({
    #             'criteria': criteria,
    #             'baseline': baseline,
    #             'Low': histogram['Low'],
    #             'Med': histogram['Med'],
    #             'High': histogram['High']
    #         })
    #
    #         if baseline not in mean_scores:
    #             mean_scores[baseline] = {}
    #         mean_scores[baseline][criteria] = (histogram['Low'] + 2 * histogram['Med'] + 3 * histogram['High']) / len(
    #             baseline_results)
    #
    #
    # results_df = pd.DataFrame(results)
    # print(results_df)
    # results_df.to_csv('baselines_results_aggregated.csv', index=False)
    #
    # mean_scores_df = pd.DataFrame(mean_scores).T.round(2)
    # print(mean_scores_df)
    # mean_scores_df.to_csv('baselines_mean_scores.csv')


if __name__ == '__main__':
    main()
