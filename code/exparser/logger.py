import os
dirname = os.path.dirname(__file__)

logf = open(os.path.join(dirname, "EXparser/logs/logfile.log"), "a")


def log(msg):
    logf.write(msg)
