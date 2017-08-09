import os
import py7zlib
from ftplib import FTP
from datetime import datetime, date, timedelta

def decompress():
    "Decompresses input/ztm_pack.7z and returns list of files"
    archive = py7zlib.Archive7z(open("input/ztm_pack.7z", "rb"))
    for name in archive.getnames():
        outfile = open(os.path.join("input", name), "wb")
        outfile.write(archive.getmember(name).read())
        outfile.close()
    return(archive.getnames())

def download(fileDate="", previousDate=""):
    """Downloads schedules effective at fileDate (string with %y%m%d form), or today.
    Then cheks if this file was already parsed, by comapring it with previousDate (RA%y%m%d fomrat).
    Returns filename if a new file has been downloaded oterwise returns None.
    """
    server = FTP("rozklady.ztm.waw.pl")
    server.login()
    files = server.nlst()
    fdate = datetime.strptime(fileDate, "%y%m%d").date() if fileDate else date.today()
    while True:
        fname = fdate.strftime("RA%y%m%d.7z")
        if fname in files:
            break
        else:
            fdate -= timedelta(days=1)

    if fname == "%s.7z" % previousDate:
        return(None)
    else:
        server.retrbinary("RETR " + fname, open("input/ztm_pack.7z", "wb").write)
        files = decompress()
        return(os.path.join("input", files[0]))

def findfile():
    "Finds ZTM's file in input dir and returns path to it"
    files = os.listdir("input")
    for file in files:
        if file.startswith("RA") and file.endswith(".TXT"):
            return(os.path.join("input", file))

def cleanup(local):
    "Cleans input and output directories before any actions."
    if local:
        dirs = ["output"]
    else:
        dirs = ["input", "output"]
    for dir in dirs:
        if not os.path.exists(dir):
            os.mkdir(dir)
        for file in [os.path.join(dir, x) for x in os.listdir(dir)]:
            os.remove(file)
