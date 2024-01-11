from BERT_CRF import Bert_CRF
import torch


def load_model(checkpoint: str):
    checkpoint = torch.load(checkpoint,
                            map_location=torch.device('cpu'))

    tag_values = checkpoint['tag_values']

    tag2idx = {t: i for i, t in enumerate(tag_values)}

    model = Bert_CRF.from_pretrained(
        'bert-base-multilingual-cased',
        num_labels=len(tag2idx),
        output_attentions=False,
        output_hidden_states=False
    )

    model.load_state_dict(checkpoint['state_dict'])

    return model, tag_values