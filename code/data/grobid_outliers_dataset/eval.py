from dataclasses import dataclass, asdict
import os
import re
import json
from typing import List
from sklearn import metrics

from flair.data import Sentence
from flair.models import SequenceTagger
from joblib import load
# from .outlier_train import extract_features

tagger = SequenceTagger.load('ner')
# clf = load('outlier_detection.joblib')


def is_outlier(refobj, use_model=False):
    if refobj is None or refobj['reference'] is None or len(refobj['reference']) < 10:  # TODO: min length?
        return True

    if 'title' not in refobj:
        return True
    elif refobj['title'] is None or len(refobj['title']) < 10:
        return True
    # elif is_space_separated_characters(refobj['title'], 5):
    #     return True

    if ('authors' not in refobj or refobj['authors'] is None) and ('editor' not in refobj or refobj['editor'] is None):
        return True
    else:
        for a in refobj['authors']:
            # print(a['author_string'])
            if not isinstance(a['author_string'], str):
                return True
            if 'author_string' in a and len(a['author_string']) > 0 and bool(re.search(r'\d', a['author_string'])):  # check if an author name contains nu>
                return True
            sentence = Sentence(a['author_string'])
            tagger.predict(sentence)
            has_human_name = False
            for entity in sentence.get_spans('ner'):
                if entity.tag == "PER":
                    has_human_name = True
                    break
            if not has_human_name:
                return True

    # if use_model:
    #     input = extract_features(refobj['reference'])
    #     return bool(clf.predict([input])[0])

    return False


def is_space_separated_characters(title: str, n: int):
    """Checks if a string contains n single characters separated by spaces:
    e.g. if n=5 "A b d 5 g" will get True """
    regexp = re.compile(r'[\S\s]{' + str(n) + '}')
    return regexp.search(title)


@dataclass
class Author:
    author_string: str


@dataclass
class RefObj:
    reference: str
    title: str
    authors: List[Author]
    editor: List[Author]

    label: int


ref_objs = []
for label in ["correct", "incorrect"]:
    for file in os.listdir(label):
        if not os.path.isfile(label + "_parsed/" + file.replace(".txt", ".json")):
            continue
        with open(label + "/" + file) as txt_f, open(label + "_parsed/" + file.replace(".txt", ".json")) as json_f:
            j_refs = json.load(json_f)
            for i, line in enumerate(txt_f):
                json_obj = j_refs[i]
                ro = RefObj(reference=line, title=None if "title" not in json_obj else json_obj["title"],
                            authors=[] if "author" not in json_obj
                            else [Author(str("" if "given" not in a else a["given"]) + " " + str("" if "family" not in a else a["family"])) for a in json_obj["author"]],
                            editor=[] if "editor" not in json_obj
                            else [Author(str("" if "given" not in a else a["given"]) + " " + str("" if "family" not in a else a["family"])) for a in json_obj["editor"]],
                            label=0 if label == "correct" else 1)
                ref_objs.append(ro)

predictions = []
true_labels = []
num_outliers = 0
for ro in ref_objs:
    pred = int(is_outlier(asdict(ro)))
    num_outliers += pred

    if pred != ro.label:
        print("pred: " + str(pred) + ", label: " + str(ro.label) + ", ref: " + ro.reference)

    predictions.append(pred)
    true_labels.append(ro.label)

print("Number of outliers: " + str(num_outliers))

disp = metrics.ConfusionMatrixDisplay.from_predictions(true_labels, predictions)
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

# with NER
# Number of outliers: 81
# Confusion matrix:
# [[178  27]
#  [ 51  54]]
# Classification report rebuilt from confusion matrix:
#               precision    recall  f1-score   support
#
#            0       0.78      0.87      0.82       205
#            1       0.67      0.51      0.58       105
#
#     accuracy                           0.75       310
#    macro avg       0.72      0.69      0.70       310
# weighted avg       0.74      0.75      0.74       310

# False positives
# pred: 1, label: 0, ref: [6] M. Eggert, R. Hau ling, M. Henze, L. Hermerschmidt, R. Hummen, D. Kerpen, A. Navarro Perez, B. Rumpe, D. Thi en, K. Wehrle, SensorCloud: Towards the Interdisciplinary Development of a Trustworthy Platform for Globally Interconnected Sensors and Actuators, in: H. Krcmar, R. Reussner, B. Rumpe (Eds.), Trusted Cloud Computing, Springer, 2014, pp. 203{ 218. doi:10.1007/978-3-319-12718-7_13
# pred: 1, label: 0, ref: J. H. Ziegeldorf, N. Viol, M. Henze, K. Wehrle, POSTER: Privacy- preserving Indoor Localization, in: 7th ACM Conference on Security and Privacy in Wireless and Mobile Networks, 2014, pp. 1-2. doi:10.13140/ 2.1.2847.4886.
# pred: 1, label: 0, ref: I. G. Smith (Ed.), The Internet of Things 2012 -New Horizons, IERC, 2012.
# pred: 1, label: 0, ref: S. Pearson, A. Benameur, Privacy, Security and Trust Issues Arising from Cloud Computing, in: 2010 IEEE Second International Conference on Cloud Computing Technology and Science (CloudCom), IEEE, 2010, pp. 693-702. doi:10.1109/CloudCom.2010.66.
# pred: 1, label: 0, ref: De Brauw Blackstone Westbroek N.V., EU Country Guide Data Location & Access Restriction (2013).
# pred: 1, label: 0, ref: D. Thilakanathan, S. Chen, S. Nepal, R. Calvo, L. Alem, A platform for secure monitoring and sharing of generic health data in the Cloud, Fu- ture Generation Computer Systems 35 (2014) 102-113. doi:10.1016/j. future.2013.09.011.
# pred: 1, label: 0, ref: eXtensible Access Control Markup Language (XACML) Version 3.0, OASIS Standard (2013).
# pred: 1, label: 0, ref: Freemarker project, Freemarker (2015). URL http://freemarker.org/
# pred: 1, label: 0, ref: [75] M. Henze, M. Gro fengels, M. Koprowski, K. Wehrle, Towards Data Handling Requirements-aware Cloud Computing, in: 2013 IEEE 5th International Conference on Cloud Computing Technology and Science (CloudCom), IEEE, 2013, pp. 266{269. doi:10.1109/CloudCom.2013.145.
# pred: 1, label: 0, ref: Ayubi, p. 231. Oliver Roy, The Failure of Political Islam, Cambridge, Massachussetts, Harvard University Press, 1996, pp. 61-63.
# pred: 1, label: 0, ref: Wiktorowicz, The Limits of Democracy in the Middle East, pp. 609-611.
# pred: 1, label: 0, ref: ! Literatur Cahill, Spencer E. & Eggleston, Robin (1994). Managing Emotions in Public: The Case of Wheelchair Users. Social Psychology Quarterly, 57 (4), 300-312.
# pred: 1, label: 0, ref: Giragosian, Richard 2009: Changing Armenia-Turkish Relations, Fokus Südkaukasus no. 1/09, Friedrich- Ebert-Stiftung, Berlin.
# pred: 1, label: 0, ref: Göle, Nilüfer 1996: The Forbidden Modern. Civilization and Veiling, Ann Arbor MI.
# pred: 1, label: 0, ref: GTAI 2011: Germany Trade and Invest (GTAI), Wirtschaftsdaten kompakt: Türkei, www.gtai.de/ext/ anlagen/PubAnlage_7707.pdf?show=true (15.10.2011).
# pred: 1, label: 0, ref: Isyar, Ömer Göksal 2005: An Analysis of Turkish-American Relations from 1945 to 2004, in: Alternatives. Turkish Journal of International Relations, vol. 4, no. 3, pp. 21-52.
# pred: 1, label: 0, ref: Kramer, Heinz 2007: Türkei, in: Siegmar Schmidt et al. (eds.), Handbuch zur deutschen Außenpolitik, Wiesbaden, pp. 482-493.
# pred: 1, label: 0, ref: Makovsky, Alan 1999: Turkey, in: Robert S. Chase et al. (eds.), The Pivotal States: A New Framework for and U.S. Policy in the Developing World, New York, pp. 88-119.
# pred: 1, label: 0, ref: US Congress 1997a: Questions for the record submitted by Mr. Frelinghuysen, answer submitted by State Department, in: House Committee on Appropriations, Foreign Operations, Export financing, and Related Programs Appropriations for 1998, Part 2.
# pred: 1, label: 0, ref: US Congress 1997b: Questions for the record submitted by Ms. Pelosi, answer submitted by Department of Defense, in: House Committee on Appropriations, Foreign Operations, Export financing, and Related Programs Appropriations for 1998, Part 2.
# pred: 1, label: 0, ref: US Overseas 2011: US Overseas Loans and Grants, Obligations and Loan Authorizations, http://gbk.eads. usaidallnet.gov (15.10.2011).
# pred: 1, label: 0, ref: Weick, Curd-Torsten 2000: Die schwierige Balance. Kontinuitäten und Brüche deutscher Türkeipolitik, Münster.
# pred: 1, label: 0, ref: White House 2010: Readout of the President's Call with Prime Minister Erdogan of Turkey, 12.10.2010.
# pred: 1, label: 0, ref: White House 2002: Press Briefing by Ari Fleischer, 10.12.2002.
# pred: 1, label: 0, ref: Case, K. E. y Fair, R. C. (1992). Fundamentos de economía. Estados Unidos: Prentice Hall Hispanoamericana.
# pred: 1, label: 0, ref: Cooper, D. J., K. Saral, and M. C. Villeval (2019). Why join a team? IZA Discussion Paper (12587).
# pred: 1, label: 0, ref: Hamilton, B. H., J. A. Nickerson, and H. Owan (2003). Team incentives and worker hetero- geneity: An empirical analysis of the impact of teams on productivity and participation. Journal of Political Economy 111 (3), 465-497.


# without NER
# Number of outliers: 45
# Confusion matrix:
# [[199   6]
#  [ 66  39]]
# Classification report rebuilt from confusion matrix:
#               precision    recall  f1-score   support
#
#            0       0.75      0.97      0.85       205
#            1       0.87      0.37      0.52       105
#
#     accuracy                           0.77       310
#    macro avg       0.81      0.67      0.68       310
# weighted avg       0.79      0.77      0.74       310

# pred: 1, label: 0, ref: I. G. Smith (Ed.), The Internet of Things 2012 -New Horizons, IERC, 2012.
# pred: 1, label: 0, ref: Freemarker project, Freemarker (2015). URL http://freemarker.org/
# pred: 1, label: 0, ref: Ayubi, p. 231. Oliver Roy, The Failure of Political Islam, Cambridge, Massachussetts, Harvard University Press, 1996, pp. 61-63.
# pred: 1, label: 0, ref: Kramer, Heinz 2007: Türkei, in: Siegmar Schmidt et al. (eds.), Handbuch zur deutschen Außenpolitik, Wiesbaden, pp. 482-493.
# pred: 1, label: 0, ref: Makovsky, Alan 1999: Turkey, in: Robert S. Chase et al. (eds.), The Pivotal States: A New Framework for and U.S. Policy in the Developing World, New York, pp. 88-119.
# pred: 1, label: 0, ref: White House 2002: Press Briefing by Ari Fleischer, 10.12.2002.