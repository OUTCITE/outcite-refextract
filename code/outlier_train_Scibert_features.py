# same approach as in outlier_train.py but manual feature extraction is replaced with SciBert

from sklearn.feature_extraction import DictVectorizer

from sklearn import svm, metrics
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from joblib import dump


def conf_matrix(lables_test, prediced):
    disp = metrics.ConfusionMatrixDisplay.from_predictions(lables_test, prediced)
    disp.figure_.suptitle("Confusion Matrix")
    print(f"Confusion matrix:\n{disp.confusion_matrix}")

    y_true = []
    y_pred = []
    cm = disp.confusion_matrix

    # For each cell in the confusion matrix, add the corresponding ground truths
    # and predictions to the lists
    for gt in range(len(cm)):
        for pred in range(len(cm)):
            y_true += [gt] * cm[gt][pred]
            y_pred += [pred] * cm[gt][pred]
    print(
        "Classification report rebuilt from confusion matrix:\n"
        f"{metrics.classification_report(y_true, y_pred)}\n"
    )


if __name__ == '__main__':
    vec = DictVectorizer()

    features_list = []
    labels = []

    from transformers import *

    tokenizer = AutoTokenizer.from_pretrained('allenai/scibert_scivocab_cased')
    model = AutoModel.from_pretrained('allenai/scibert_scivocab_cased')

    with open("data/dataset/transformed/data.csv") as f:
        for l in f:
            line, label = l.split("\t")

            inputs = tokenizer(line, padding=True, truncation=True,
                               return_tensors="pt", return_token_type_ids=False, max_length=512)
            output = model(**inputs)

            features_list.append(list(output.last_hidden_state.detach().numpy()[0][0]))
            labels.append(int(label))

    X_train, X_test, y_train, y_test = train_test_split(
        list(features_list), labels, test_size=0.2, shuffle=True
    )

    clf = svm.SVC(gamma=0.001)
    clf.fit(X_train, y_train)
    dump(clf, 'outlier_detection_svm_bert.joblib')

    svc_predicted = clf.predict(X_test)

    index = 0
    with open("data/dataset/transformed/data.csv") as f:
        for l in f:
            line, label = l.replace("\n", "").split("\t")

            if svc_predicted[index] != int(label):
                print(str(svc_predicted[index]) + ", " + label + ", " + line)

    clf_rf = RandomForestClassifier(max_depth=2, random_state=0)
    clf_rf.fit(X_train, y_train)
    dump(clf_rf, 'outlier_detection_clf_rf_bert.joblib')

    rf_predicted = clf_rf.predict(X_test)

    print("SVC conf matrix")
    conf_matrix(y_test, svc_predicted)
    print("RF conf matrix")
    conf_matrix(y_test, rf_predicted)
