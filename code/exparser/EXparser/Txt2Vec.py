# -*- coding: UTF-8 -*- 
import os
import csv
import re
import numpy as np


def check_ref(ln):
    tmp = re.findall(r'<ref>', ln)
    if tmp:
        ref = 1
    else:
        ref = 0
    return ref


def check_eref(ln):
    tmp = re.findall(r'</ref>', ln)
    if tmp:
        eref = 1
    else:
        eref = 0
    return eref


def text_to_vec(data_dir: str):
    fold = data_dir + "/LRT"
    fdir = os.listdir(fold)
    total = str(len(fdir))
    for u in range(0, len(fdir)):
        if fdir[u].startswith("."):
            continue
        print('>Text to Vec:' + str(u + 1) + '/' + total)
        if not os.path.isfile(data_dir + "/RefLD/" + fdir[u]):
            if not os.path.isdir(data_dir + '/RefLD'):
                os.makedirs(data_dir + '/RefLD')

            fname = fold + "/" + fdir[u]
            file = open(fname)
            reader = csv.reader(file, delimiter='\t', quoting=csv.QUOTE_NONE)

            b = 0
            e = 0
            i = 0
            R = np.empty((0, 1), int)
            for row in reader:
                if row != []:
                    row[0] = row[0]
                    b = check_ref(row[0])
                    e = check_eref(row[0])

                    if b == 0 and e == 0 and i == 0:
                        ref = 0
                    elif b == 1 and e == 0 and i == 0:
                        ref = 1
                        i = 1
                    elif b == 0 and e == 0 and i == 1:
                        ref = 2
                    elif b == 0 and e == 1:
                        ref = 3
                        i = 0
                    elif b == 1 and e == 1:
                        ref = 1
                        i = 0
                    R = np.append(R, [[ref]], 0)
            np.savetxt(data_dir + '/RefLD/' + fdir[u], R)


# text_to_vec()
