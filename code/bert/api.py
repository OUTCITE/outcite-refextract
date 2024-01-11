from fastapi import FastAPI
from pydantic import BaseModel
from typing import List
import json

from langdetect import detect
import spacy

import torch
from transformers import BertTokenizer

from preprocess_data import sentencize_and_tokenize_text, sequence_filler, pad_sentences, tag_tokens_correctly, \
    print_labels_of_sentences, get_extracted_references, get_segmented
from utils import load_model
from reference import Reference


nlp_english = spacy.load("en_core_web_sm")
nlp_german = spacy.load("de_core_news_sm")

app = FastAPI()

tokenizer = BertTokenizer.from_pretrained('bert-base-multilingual-cased', do_lower_case=False)
extraction_model, extraction_tag_values = load_model('models/Checkpoint_Extraction_multilingual_maximumContext.pt')
segmentation_model, segmentation_tag_values = load_model('models/Checkpoint_Segmentation_multilingual_maximumContext.pt')


class ExtractResponse(BaseModel):
    references: List[str] = list()


class SegmentResponse(BaseModel):
    references: List[Reference] = list()


@app.post("/extract", response_model=ExtractResponse)
def extract(request: str):
    lng = detect(request[0:100])  # todo: how useful is it?

    nlp = nlp_english
    if lng == 'de':
        nlp = nlp_german

    tokenized_sentences = sentencize_and_tokenize_text(request, nlp)

    tokenized_sentences = sequence_filler(tokenized_sentences, tokenizer)

    bert_tokenized_sentences = [tokenizer.encode(s) for s in tokenized_sentences]

    padded_tokenized_sentences = pad_sentences(bert_tokenized_sentences)

    att_masks = [[float(i != 0.0) for i in ii] for ii in padded_tokenized_sentences]

    input_ids = torch.LongTensor(padded_tokenized_sentences)
    att_masks = torch.LongTensor(att_masks)

    output = extraction_model(input_ids, token_type_ids=None, attention_mask=att_masks)

    all_new_tokens, all_new_labels = tag_tokens_correctly(input_ids, output, extraction_tag_values, tokenizer)

    print_labels_of_sentences(all_new_tokens, all_new_labels)

    references = get_extracted_references(all_new_tokens, all_new_labels)

    return ExtractResponse(
        references=references
    )


@app.post("/segment", response_model=SegmentResponse)
def segment(request: ExtractResponse):
    lng = detect(request.references[0])  # todo: how useful is it?

    nlp = nlp_english
    if lng == 'de':
        nlp = nlp_german

    sentences_tokenized = []
    for sentence in request.references:
        s = str(sentence)
        tokens = nlp(s)
        sentences_tokenized.append([str(t) for t in tokens])

    # tokenized_sentences = sequence_filler(sentences_tokenized, tokenizer)

    bert_tokenized_sentences = [tokenizer.encode(s) for s in sentences_tokenized]

    padded_tokenized_sentences = pad_sentences(bert_tokenized_sentences)

    att_masks = [[float(i != 0.0) for i in ii] for ii in padded_tokenized_sentences]

    input_ids = torch.LongTensor(padded_tokenized_sentences)
    att_masks = torch.LongTensor(att_masks)

    output = segmentation_model(input_ids, token_type_ids=None, attention_mask=att_masks)

    # todo: do not combine references in one
    all_new_tokens, all_new_labels = tag_tokens_correctly(input_ids, output, segmentation_tag_values, tokenizer)

    print_labels_of_sentences(all_new_tokens, all_new_labels)

    refs = get_segmented(all_new_tokens, all_new_labels, segmentation_tag_values)

    return SegmentResponse(
        references=refs
    )


# for testing
if __name__ == '__main__':
    # with open("data/geocite_example.json", 'r') as f:
    #     data = json.load(f)
    #
    with open("data/test.txt") as f:
        inputs = ""
        for l in f:
            inputs += l

    r = extract(inputs)

    with open("output/extracted_references.csv", "w") as wf:
        for ref in r.references:
            wf.write(ref + '\n')

    # r = extract(data['hits']['hits'][0]['_source']['cermine_fulltext'])
    # r = extract(data['hits']['hits'][0]['_source']['fulltext'])

    r = segment(r)

    print()

    # response = ExtractResponse()
    # response.references = ['Bruckbauer, St. (2004), „12+8=20 – The Euro in Eastern Europe“, BA-CA Xplicit, Vienna',
    #              'Campos, N. F. and F. Coricelli (2002), „Growth in Transition: What we Know, What we Dont´t, and What we Should']
    # r = segment(response)
