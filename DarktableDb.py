import sqlite3
import pathlib
from os import path
import os
import shutil
from enum import Enum
from typing import Iterable
import itertools

#TODO support anything else than "presets"

FILTER_INIT = "writeprotect='0'"

class FilterType(Enum):
    OperationIs = 1
    NameLike = 2

class DarktableDb:



    def __init__(self, dbpath : pathlib.Path):
        """
        Initialize a DB (either a backup or the library.db from darktable9

        The DB must be connected by CreateNew() or OpenExisting() before it can be used.

        :param dbpath: Path of the DB
        :type dbpath: pathlib.Path
        """
        self.path = pathlib.Path(path.expanduser(str(dbpath)))
        self._con = None
        self.ResetDatabaseFilter()

    def CreateNew(self, proto):
        """
        Create a new DB file at the path given during construction

        :param proto: DarktableDb to coppy column names and types from
        :type proto: DarktableDb
        """
        if self.dbExists:
            raise FileExistsError("File {} already exists".format(self.path))
        if not path.exists(str(self.path.parent)):
            os.mkdir(str(self.path.parent))
        self._con = sqlite3.connect(str(self.path))
        c = self._con.cursor()
        colNameType = zip(proto.colNames, proto.colTypes)
        sqlCmd = "CREATE TABLE presets ({}, CONSTRAINT u1 UNIQUE (name, operation, op_version));".format(", ".join(["{} {}".format(x[0], x[1]) for x in colNameType]))
        c.execute(sqlCmd)
        self._con.commit()
        self._InitDbInfo()


    def OpenExisting(self):
        """
        Open an existing DB file at the path given during construction
        """
        if not self.dbExists:
            raise FileNotFoundError("File {} does not exist".format(self.path))
        self._con = sqlite3.connect(str(self.path))
        self._InitDbInfo()

    def CreateBackup(self) -> str:
        """
        Create a backup of the db in the same directory. The name of the backup is <dbName>_back#.db where # is
        replaced by the lowest number not yet existing. So the backups created for a db named x.db would be named:
        1. Execution: x_back0.db
        2. Execution: x_back1.db

        :return: Name of the backup file created
        :rtype: str
        """
        extCnt = 0
        while True:
            backupPath = self.path.parent / (self.path.stem + "_back" + str(extCnt) + self.path.suffix)
            if path.exists(str(backupPath)):
                extCnt += 1
            else:
                break
        shutil.copy(str(self.path), str(backupPath))
        return backupPath.name

    def DeleteBackups(self):
        raise NotImplementedError()

    def ResetDatabaseFilter(self):
        """
        Reset the filter of the database
        """
        self._filter = FILTER_INIT

    def AddDatabaseFilter(self, type : FilterType, value : str):
        """
        Add a filter (AND) to the database
        :param type: Type of the filter
        :type type: FilterType
        :param value: Value of the filtering expression
        :type value: str
        """
        if type is FilterType.OperationIs:
            self._filter += " AND operation='{}'".format(value)
        elif type is FilterType.NameLike:
            self._filter += " AND name LIKE '{}'".format(value)
        else:
            raise ValueError("Illegal type passed")

    def GetFilteredColumns(self, columnNames : Iterable[str], filtered : bool):
        """
        Get columns of active filters

        :param columnNames: Column names to return
        :type columnNames: Iterable[str]
        :param filtered: True = only return filtered items, False = return all items
        :type filtered: bool

        :return: Values of all columns for all rows not filtered out
        """
        self._ChekConnected()
        cols = ",".join(columnNames)
        sqlCmd = "SELECT {} FROM presets".format(cols)
        if filtered:
            sqlCmd += " WHERE {}".format(self._filter)
        return self._con.cursor().execute(sqlCmd).fetchall()

    def InsertEntries(self, entries):
        """
        Insert new entries into TB

        :param entries: entries acquired by "GetFilteredColumns(*)"
        """
        self._ChekConnected()
        sqlCmd = "INSERT INTO presets VALUES ({})".format(",".join(itertools.repeat("?", len(self.colNames))))
        self._con.cursor().executemany(sqlCmd, entries)
        self._con.commit()

    def DeleteFilteredItems(self):
        """
        Delete all items that are currently not filtered out
        """
        self._ChekConnected()
        c = self._con.cursor()
        for name, operation in self.GetFilteredColumns(["name", "operation"], filtered=True):
            sqlCmd = "DELETE FROM presets WHERE (name='{}' AND operation='{}')".format(name, operation)
            c.execute(sqlCmd)
        self._con.commit()

    def Close(self):
        self._ChekConnected()
        self._con.close()

    @property
    def dbExists(self) -> bool:
        """
        Check if the path of the DB already exists

        :return: True: file exists, False: file does not exist
        """
        return path.exists(str(self.path))

    def _ChekConnected(self):
        """
        Check if the DB is connected and can be used.

        :return:
        """
        if self._con is None:
            raise ConnectionError("Database not yeet onnected, use CreateNew() or OpenExisting() first")

    def _InitDbInfo(self):
        """
        Initializes the information about the DB (must be executed after connection)
        """
        ti = self._con.cursor().execute("PRAGMA table_info(presets)").fetchall()
        self.colNames = [x[1] for x in ti]
        self.colTypes = [x[2] for x in ti]


