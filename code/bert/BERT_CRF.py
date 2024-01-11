from transformers import BertPreTrainedModel, BertModel
from pytorchcrf import CRF
import torch.nn as nn
import torch
import torch.nn.functional as F


log_soft = F.log_softmax


class Bert_CRF(BertPreTrainedModel):
    """a class summarizing the model architecture when crf is used on top of bert.

    Attributes:
        bert (TYPE): the pretrained bert model to be used
        classifier (TYPE): the classifier to be used on top of the pretrained bert model
        crf (TYPE): the conditional random field to be used for the classifiers outputs
        dropout (TYPE): dropout with its probability
        num_labels (TYPE): number of labels to be used
    """

    def __init__(self, config):
        super(Bert_CRF, self).__init__(config)
        self.num_labels = config.num_labels
        self.bert = BertModel(config)
        self.dropout = nn.Dropout(config.hidden_dropout_prob)  # 0.1 is default by BERT
        self.classifier = nn.Linear(config.hidden_size, self.num_labels)
        self.init_weights()
        self.crf = CRF(self.num_labels, batch_first=True)

    def forward(self, input_ids, token_type_ids=None, attention_mask=None, labels=None):
        outputs = self.bert(input_ids,
                            attention_mask=attention_mask,
                            token_type_ids=token_type_ids)
        sequence_output = outputs[0]
        sequence_output = self.dropout(sequence_output)
        emission = self.classifier(sequence_output)
        attention_mask = attention_mask.type(torch.uint8)
        if labels is not None:
            labels = labels.type(torch.long)
            loss = -self.crf(log_soft(emission, 2), labels, mask=attention_mask, reduction='mean')
            return loss
        else:
            prediction = self.crf.decode(emission, mask=attention_mask)
            return prediction

