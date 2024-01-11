import json
import requests


def extract_segment_bert(paper_string: str):
    reference_strings = requests.post('http://127.0.0.1:8000/extract', data=paper_string)

    # todo: save extracted strings?

    segmented_references = requests.post('http://127.0.0.1:8000/segment', data=reference_strings)

    return [r.__dict__ for r in segmented_references]


# for testing
if __name__ == '__main__':
    with open("data/geocite_example.json", 'r') as f:
        data = json.load(f)
    r = extract_segment_bert(data['hits']['hits'][0]['_source']['cermine_fulltext'])
    print(json.dumps(r))
