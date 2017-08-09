import sqlite3
import pathlib
from os import path
import shutil
from DarktableDb import DarktableDb, FilterType

#Function Definitions
def CreateDbBackup(dbPath : pathlib.Path):
    extCnt = 0
    while True:
        backupPath = dbPath.parent / (dbPath.stem + "_back" + str(extCnt) + dbPath.suffix)
        if path.exists(str(backupPath)):
            extCnt += 1
        else:
            break

    print("Create DB backup: {}\n".format(backupPath))
    shutil.copy(str(dbPath), str(backupPath))

#Startup
VERSION = "0.1"
print("Darktable Preset Export Tool, Version {}\n".format(VERSION))
print("You can either open the Darktable database (default: ~/.config/darktable/library.db) or a database created by " +
      "the export command of this tool.\n")

#Opening DB

def SetActiveDb() -> DarktableDb:
    while True:
        dbPath = pathlib.Path(input("Enter database path: "))
        activeDb = DarktableDb(dbPath)
        if activeDb.dbExists:
            break
        else:
            print("File does not exist, try again\n")
    activeDb.OpenExisting()
    return activeDb

activeDb = SetActiveDb()


def PrintSelectedPresets():
    print("{:30}{}".format("NAME", "OPERATION"))
    print("{:->30}{:->30}".format("", ""))
    for name, operation in activeDb.GetFilteredColumns(["name", "operation"], filtered=True):
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
    print("  delete           - Delete selected presets (take care!)")
    print("  changeDb         - Open a different DB")
    print("  removeBackups    - Remove all backups of this DB automatically created")
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
        print()

        filt = filtIn.split(" ")[0]
        try:
            param = filtIn.split(" ")[1]
        except IndexError:
            param = ""
        if filt == "namelike":
            activeDb.AddDatabaseFilter(FilterType.NameLike, param)
        elif filt == "operationis":
            activeDb.AddDatabaseFilter(FilterType.OperationIs, param)
        elif filt == "reset":
            activeDb.ResetDatabaseFilter()
        else:
            print("Illegal Input: {}\n".format(filtIn))

    elif cmd == "export":

        exportPath = pathlib.Path(input("Enter export file path (extension .db): "))
        print()
        exportDb = DarktableDb(exportPath)
        if exportDb.dbExists:
            print("Selected db exists, data is appended\n")
            exportDb.OpenExisting()
        else:
            print("Selected db does not exist, it is created\n")
            exportDb.CreateNew(activeDb)
        exportDb.InsertEntries(activeDb.GetFilteredColumns(["*"], filtered=True))
        exportDb.Close()

    elif cmd == "delete":
        activeDb.DeleteFilteredItems()
        activeDb.ResetDatabaseFilter()
        print("Deleted. Filter is automatically reset\n")

    elif cmd == "import":
        activeDb.CreateBackup()

        importPath = pathlib.Path(input("Enter import file path (extension .db): "))
        importDb = DarktableDb(importPath)
        importDb.OpenExisting()

        print()
        allPresets = importDb.GetFilteredColumns(["*"], filtered=False)
        for preset in allPresets:
            try:
                activeDb.InsertEntries([preset])
            except sqlite3.IntegrityError:
                print("Skipped: {} for operator {} (already exists)".format(preset[0], preset[2]))
        importDb.Close()
        print()

    elif cmd == "changeDb":
        activeDb.Close()
        activeDb = SetActiveDb()

    elif cmd == "removeBackups":
        activeDb.DeleteBackups()

    else:
        print("Illegal command, try again\n")
        continue


activeDb.Close()
exit()


