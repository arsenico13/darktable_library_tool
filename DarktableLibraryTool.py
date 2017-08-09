import sqlite3
import pathlib
import itertools
from os import path
import os
import shutil

VERSION = "0.1"
print("Darktable Preset Export Tool, Version {}\n".format(VERSION))

#Startup
dbPath = pathlib.Path(path.expanduser(input("Enter database path: ")))

def CreateMainDbBackup():
    extCnt = 0
    while True:
        backupPath = dbPath.parent / (dbPath.stem + "_back" + str(extCnt) + dbPath.suffix)
        if path.exists(str(backupPath)):
            extCnt += 1
        else:
            break

    print("Create DB backup: {}\n".format(backupPath))
    shutil.copy(str(dbPath), str(backupPath))

#Opening DB
print(path.exists(str(dbPath)))
database = sqlite3.connect(str(dbPath))

#Fetching DB Information
ti = database.cursor().execute("PRAGMA table_info(presets)")
colNames = [x[1] for x in ti.fetchall()]
ti = database.cursor().execute("PRAGMA table_info(presets)")
colTypes = [x[2] for x in ti.fetchall()]

FILTERS_INIT = "writeprotect='0'"
filters = FILTERS_INIT

def GetSelectedPresets(columns : str):
    c = database.cursor()
    c.execute("SELECT {} FROM presets WHERE ({})".format(columns, filters))
    return c.fetchall()

def PrintSelectedPresets():
    print("{:30}{}".format("NAME", "OPERATION"))
    print("{:->30}{:->30}".format("", ""))
    for name, operation in GetSelectedPresets("name, operation"):
        print("{:30}{}".format(name, operation))



#Operations
while True:
    #Display Menu
    print("Currently selected:")
    PrintSelectedPresets()
    print()
    print("Select Operation")
    print("  filter           - Filter selected presets")
    print("  export           - Export selected presets")
    print("  import           - Import resets to DB")
    print("  quit             - exit program")
    cmd = input()
    print()

    if cmd == "quit":
        print("quitting...")
        exit()

    elif cmd == "filter":
        print("Select Filtering")
        print("  namelike <x>       - Only include presets whos name is similar to <x> (SQL Syntax, use % for Wildcard)")
        print("  operationis <x>    - Only include presets for operation <x>")
        print("  reset              - Reset filters")
        filtIn = input()

        filt = filtIn.split(" ")[0]
        try:
            param = filtIn.split(" ")[1]
        except IndexError:
            param = ""
        if filt == "namelike":
            filters += " AND name LIKE '{}'".format(param)
        elif filt == "operationis":
            filters += " AND operation='{}'".format(param)
        elif filt == "reset":
            filters = FILTERS_INIT
        else:
            print("Illegal Input: {}".format(filtIn))
            continue

        print()
        continue

    elif cmd == "export":

        exportPath = pathlib.Path(input("Enter export file path (extension .db): "))
        print()
        if not path.exists(str(exportPath.parent)):
            os.mkdir(str(exportPath.parent))
        exportDb = sqlite3.connect(str(exportPath))
        c = exportDb.cursor()
        sqlCmd = "CREATE TABLE presets ({});".format(", ".join(["{} {}".format(colNames[i], colTypes[i]) for i in range(len(colNames))]))
        c.execute(sqlCmd)
        #for row in GetSelectedPresets("*"):
        sqlCmd = "INSERT INTO presets VALUES ({})".format(",".join(itertools.repeat("?", len(colNames))))
        c.executemany(sqlCmd, GetSelectedPresets("*"))
        exportDb.commit()
        continue

    elif cmd == "import":

        CreateMainDbBackup()

        importPath = pathlib.Path(input("Enter import file path (extension .db): "))
        print()
        importDb = sqlite3.connect(str(importPath))
        c = importDb.cursor()
        allPresets = c.execute("SELECT * FROM presets")

        sqlCmd = "INSERT INTO presets VALUES ({})".format(",".join(itertools.repeat("?", len(colNames))))
        cdb = database.cursor()
        for preset in allPresets:
            try:
                cdb.execute(sqlCmd, preset)
            except sqlite3.IntegrityError:
                print("Skipped: {} for operator {} (already exists)".format(preset[0], preset[2]))
        print()
        database.commit()
        continue

    else:
        print("Illegal command, try again\n")
        continue


database.close()
exit()


