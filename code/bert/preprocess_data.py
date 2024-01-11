# author of most of the code in this file: Hasan Evci


from keras.preprocessing.sequence import pad_sequences
from reference import Reference, Publisher, Editor


def get_length_of_bert_sentence(sentence, tokenizer):
    length = 0
    for word in sentence:
        tokenized_word = tokenizer.tokenize(word)
        if not tokenized_word:
            tokenized_word = ['[UNK]']
        length += len(tokenized_word)
    return length


# todo: doesn't seem to work "perfect", have another look
def sentencize_and_tokenize_text(text, nlp_model):
    """given a text as input, this method sentencizes the text.
    Afterwards each sentence is tokenized. Each tokenized sentence
    is stored in a list and consequently a list of lists is returned


    Args:
        text (TYPE): Description
        nlp_model (TYPE): Description

    Returns:
        TYPE: Description
    """

    sentences = []
    sentences_tokenized = []
    predicted_sentences = nlp_model(text.replace("\n", " "))
    [sentences.append(str(s)) for s in predicted_sentences.sents]  # inefficient
    for sentence in sentences:
        s = str(sentence)
        tokens = nlp_model(s)
        sentences_tokenized.append([str(t) for t in tokens])  # inefficient

    return sentences_tokenized


def sequence_filler(sentences, tokenizer):
    new_sentences = []

    filled_sentences = []
    current_length = 0
    sentence_counter = 0
    for s in sentences:
        bert_length_of_current_sentence = get_length_of_bert_sentence(s, tokenizer)
        current_length += bert_length_of_current_sentence
        if current_length > 512 - 2:  # for CLS and SEP tokens
            if sentence_counter != 0:
                new_sentences.append(filled_sentences)

                filled_sentences = s
                current_length = bert_length_of_current_sentence
                sentence_counter = 1
            else:
                filled_sentences.extend(s)

                new_sentences.append(filled_sentences)

                filled_sentences = []

                current_length = 0
                sentence_counter = 0
        else:
            filled_sentences.extend(s)

            sentence_counter += 1

    if filled_sentences:
        new_sentences.append(filled_sentences)

    return new_sentences


def pad_sentences(sentences):
    padded_sentences = pad_sequences([s for s in sentences],
                                     # List of sequences: each splittet token by wordpiece bert tokenizer
                                     maxlen=512,  # this is the length of how long the arrays need to be
                                     dtype="long",  # Type of the output sequences
                                     value=0.0,  # this is the value of the padding
                                     truncating="post",
                                     # remove values from sequences larger than maxlen, either at the beginning or at the end of the sequences
                                     padding="post")  # padding will be added at the end of arrays, pad either before or after each sequence.
    return padded_sentences


# tag splittet tokens too (BERT splits tokens in bla ##yadi ##yada = blayadiyada)
def tag_tokens_correctly(sentence_tensors, label_indices_all, tag_values, tokenizer):
    all_new_tokens, all_new_labels = [], []
    for sentence_tensor, label_indices in zip(sentence_tensors, label_indices_all[0]):
        tokens = tokenizer.convert_ids_to_tokens(sentence_tensor.to('cpu').numpy())
        new_tokens, new_labels = [], []
        for token, label_idx in zip(tokens, label_indices):
            if token.startswith("##"):
                new_tokens[-1] = new_tokens[-1] + token[2:]
            else:
                new_labels.append(tag_values[label_idx])  # i need to save tag_values from previous training, too
                new_tokens.append(token)
        all_new_tokens.append(new_tokens)
        all_new_labels.append(new_labels)
    return all_new_tokens, all_new_labels


def print_labels_of_sentences(all_new_tokens, all_new_labels):
    sentenceCounter = 1
    for new_tokens, new_labels in zip(all_new_tokens, all_new_labels):
        print(sentenceCounter, ". Sentence - all Tags \n")
        sentenceCounter += 1
        for token, label in zip(new_tokens, new_labels):
            print("{}\t{}".format(label, token))
        print()


def get_extracted_references(all_new_tokens, all_new_labels):
    references = []
    for new_tokens, new_labels in zip(all_new_tokens, all_new_labels):
        reference = []
        inside = False
        for token, label in zip(new_tokens, new_labels):
            if token == '[PAD]' or token == '[CLS]':
                if inside:
                    references.append(reference)
                    reference = []
                    inside = False
                continue
            if label == 'B-ref' and not inside:
                inside = True
                reference += [token]
                continue
            if inside and (label == "O" or label == "B-ref"):
                references.append(reference)
                reference = []
                inside = False
            if inside and label == "I-ref":
                reference += [token]
        if len(reference) > 0:
            references.append(reference)

    return [" ".join(r).replace("[UNK]", "").strip() for r in references]


def match_labels_to_object(label: str, value: str, r: Reference):
    if label == 'title':
        r.title = value

    if label == 'firstpage':
        r.start = value

    if label == "lastpage":
        r.end = value

    if label == "year":
        r.year = value

    if label == "issue":
        r.issue = value

    if label == "volume":
        r.volume = value

    if label == "source":
        r.source = value

    if label == "given-names":
        pass
        # 'given-names'
        # 'surname'

    if label == "publisher":
        p = Publisher()
        p.publisher_string = value
        r.publishers.append(p)

    if label == "editor":
        e = Editor()
        e.editor_string = value
        r.editors.append(e)

    # todo: these tags?
    # 'identifier'
    # 'url'
    # 'other'


def get_segmented(all_new_tokens, all_new_labels, segmentation_tag_values):
    refs = []
    for new_tokens, new_labels in zip(all_new_tokens, all_new_labels):
        r = Reference()
        current_tag = None
        value = None
        # todo: author names - when one ends and the next starts?
        for token, label in zip(new_tokens, new_labels):
            if token == '[PAD]' or token == '[UNK]' or token == '[CLS]':
                continue

            # todo: is it good?
            if label == 'ref' or label == 'author':
                continue

            if current_tag != label and value is not None:
                match_labels_to_object(current_tag, value, r)

                value = None
                current_tag = None

            if current_tag is None and label in segmentation_tag_values and label != "PAD":
                current_tag = label
                value = token
                continue

            if current_tag == label:
                value += " " + token

        if value is not None:
            match_labels_to_object(current_tag, value, r)

        refs.append(r)

    return refs
