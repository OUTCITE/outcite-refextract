from EXparser.Segment_F1 import *
from langdetect import detect
from JsonParser import *
from configs import *
from logger import *
import re
import dataclasses
import json

from reference import *


def del_none(d):
    for key, value in list(d.items()):
        if not value:
            del d[key]
        elif isinstance(value, dict):
            del_none(value)
    return d


def map_Exparser_to_output(item: str, ref: str):
    r = Reference()
    r.reference = ref

    # extract authors
    for it in item.split("</author>")[:-1]:
        a = Author()
        value = it[it.find("<author>") + len("<author>"):]
        a.author_string = re.sub('<[^>]*>', '', value)
        for s in value.split("</surname>")[:-1]:
            if "<surname" in s:
                v = s[s.find("<surname ") + len("<surname ") + 14:]  # todo: 14 is to remove the prob=..; replace with better
                if a.surnames is not None:
                    a.surnames = a.surnames + " " + v
                else:
                    a.surnames = v
        for s in value.split("</given-names>")[:-1]:
            if "<given-names" in s:
                v = s[s.find("<given-names ") + len("<given-names ") + 14:]
                a.firstnames.append(v)

        r.authors.append(a)

    tags_to_merge = ["title", "source", "year", "volume", "issue", "other"]
    # extract tags to merge
    for tag in tags_to_merge:
        value = ""
        for it in item.split("</" + tag + ">")[:-1]:
            br_tag = "<" + tag + " "
            if "<" + tag + " " in it:
                value += it[it.find(br_tag) + len(br_tag) + 14:] + " "
        if tag == "title":
            r.title = value.strip()
        elif tag == "source":
            r.source = value.strip()
        elif tag == "year":
            r.year = value.strip()
        elif tag == "issue":
            r.issue = value.strip()

    # extract tags to list
    tags_to_merge = ["editor", "publisher"]
    for tag in tags_to_merge:
        for it in item.split("</" + tag + ">")[:-1]:
            br_tag = "<" + tag + " "
            if "<" + tag + " " in it:
                if tag == "editor":
                    e = Editor()
                    e.editor_string = it[it.find(br_tag) + len(br_tag) + 14:]
                    r.editors.append(e)
                elif tag == "publisher":
                    p = Publisher()
                    p.publisher_string = it[it.find(br_tag) + len(br_tag) + 14:]
                    r.publishers.append(p)

    r = del_none(dataclasses.asdict(r))
    r["authors"] = [del_none(a) for a in r["authors"]]
    if "publishers" in r:
        r["publishers"] = [del_none(a) for a in r["publishers"]]
    if "editors" in r:
        r["editors"] = [del_none(a) for a in r["editors"]]

    return json.dumps(r)


def extract_segment_exparser(layout_file_string: str):
    segmented_references = []

    global lng
    try:
        lng = detect(layout_file_string)
    except:
        log("Cannot extract language from " + layout_files_path)
        lng = ""

    # todo: pass one model: rf as we have one model now for extraction for de and en
    txt, valid, _, ref_prob0 = ref_ext(layout_file_string, lng, idxx, rf, rf)
    refs = segment(txt, ref_prob0, valid)
    reslt, refstr, retex = sg_ref(txt, refs, 1)

    # result: segmented references # refstr: refstr references # retex: bibtex
    log('Number of references: ' + str(len(refstr)))

    #for item, ref in zip(reslt, txt):
    #    segmented_references.append(map_Exparser_to_output(item, ref))

    #return segmented_references
    return [(item,ref) for item,ref in zip(reslt, txt)]


if __name__ == "__main__":
    res = extract_segment_exparser("Andersen, T. Janus: Forbrugerpolitik og forbrugerOrganisation (Consumer Policy and Consumer­	128.0500030517578	526.116455078125	7.559051513671875	340.69749450683594	92	YPWFID+TimesNewRomanPS-BoldMT\norganization in Scandinavia) Samfundslitteratur 1980.	139.59999084472656	534.8233642578125	7.501556396484375	185.08387756347656	92	YPWFID+TimesNewRomanPS-BoldMT\nAubert, Vilhelm: “Some Social Functions of Legislation“ in Blegvad, Britt-Mari (ed.) Contribu-	127.8499984741211	543.61376953125	7.5618896484375	339.89270782470703	92	YPWFID+TimesNewRomanPS-BoldMT")
    print(res)
