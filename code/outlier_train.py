from sklearn.feature_extraction import DictVectorizer
from sklearn import svm, metrics
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from joblib import dump

from collections import Counter
import re


def extract_features(line):
    features = {}

    counter = Counter(line)

    features["quotation marks"] = counter['?']
    features["separators"] = counter['-']
    features["journal"] = 1 if "journal" in line.lower() else 0
    features["unique_chars"] = len(counter.keys())
    features["chars"] = len(line)
    features["words"] = len(line.split(" "))
    features["not_alpha_num"] = 1 if line.isalnum() else 0

    features["year"] = len(re.findall(r'.*([1-3][0-9]{3})', line))  # how many years

    return features


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
    # Read data, extract features, do the train/test split
    vec = DictVectorizer()

    features_list = []
    labels = []

    with open("data/dataset/transformed/data.csv") as f:
        for l in f:
            line, label = l.split("\t")
            features_list.append(extract_features(line))
            labels.append(int(label))

    features_vectorized = vec.fit_transform(features_list).toarray()

    X_train, X_test, y_train, y_test = train_test_split(
        features_vectorized, labels, test_size=0.2, shuffle=True
    )

    # Train and test standard SVM
    clf = svm.SVC(gamma=0.001)
    clf.fit(X_train, y_train)
    dump(clf, 'outlier_detection_svc.joblib')

    svc_predicted = clf.predict(X_test)

    # Train and test a random forest classifier
    clf_rf = RandomForestClassifier(max_depth=2, random_state=0)
    clf_rf.fit(X_train, y_train)
    dump(clf_rf, 'outlier_detection_clf_rf.joblib')

    rf_predicted = clf_rf.predict(X_test)

    print("SVC conf matrix")
    conf_matrix(y_test, svc_predicted)
    print("RF conf matrix")
    conf_matrix(y_test, rf_predicted)
