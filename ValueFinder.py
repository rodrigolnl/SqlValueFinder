import time
import pyodbc
import warnings
import threading
import pandas as pd
from math import ceil
from unidecode import unidecode

warnings.simplefilter(action='ignore', category=UserWarning)


class ValueFinder:
    def __init__(self, server: str, number_of_threads: int = 1, connection_string: str = None):
        if connection_string:
            self.conn_string = connection_string
        else:
            self.conn_string = f'Driver={{SQL Server}};'f'Server={server};''Database=#database#;''Trusted_Connection=yes;'

        self.result: list[dict] = []

        self.number_of_threads = number_of_threads if number_of_threads > 0 else 1

        self.threads: list[dict[str, int | None | threading.Thread]] = [{'id': x, 'task': None}
                                                                        for x in range(self.number_of_threads)]

        self.database: list[int | None] = [None for x in range(self.number_of_threads)]
        self.conn: list[pyodbc.Connection | None] = [None for x in range(self.number_of_threads)]
        self.tables_info: dict = {}

        self.free_threads = [x for x in range(self.number_of_threads)]

    def find_value(self, value, databases: list = None, tables: list = None, exact_match: bool = False):
        execution = time.time()
        databases = databases if databases and len(databases) > 0 else self.__get_all_databases()
        database_count = 0
        for database in databases:
            database_count += 1
            print('Database[%i/%i]: %s' % (database_count, len(databases), database))
            query = 'SELECT DATA_TYPE, COLUMN_NAME,CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS'
            self.tables_info[database] = pd.read_sql(query, pyodbc.connect(self.conn_string.replace('#database#', database)))
            tables = tables if tables and len(tables) > 0 else self.__get_all_tables(database)
            count = 1
            total = len(tables)
            while len(tables) > 0:
                print('\r[%i/%i] Scanning Tables' % (count, total), end='')
                if self.number_of_threads > 1:
                    if len(self.free_threads) > 0:
                        thread_id = self.free_threads.pop(0)
                        table = tables.pop(0)
                        count += 1
                        self.threads[thread_id]['task'] = threading.Thread(target=self.__finder,
                                                                           args=(value, database, table, thread_id,
                                                                                 exact_match), daemon=True)
                        self.threads[thread_id]['task'].start()
                    else:
                        time.sleep(0.1)
                else:
                    count += 1
                    self.free_threads.pop(0)
                    self.__finder(value, database, tables.pop(0), 0, exact_match)
            print('\n')
        while len(self.free_threads) != self.number_of_threads:
            time.sleep(0.5)
        print(('Execution Time: %s seconds\n' % str(ceil(time.time() - execution))))
        print('Results: ')
        for result in self.result:
            print(result)
        return self.result

    def __get_all_tables(self, database):
        conn = pyodbc.connect(self.conn_string.replace('#database#', database))
        query = 'SELECT name FROM SYSOBJECTS WHERE xtype = \'U\''
        df = pd.read_sql(query, conn)
        return list(df['name'])

    def __get_all_databases(self):
        conn = pyodbc.connect(self.conn_string.replace('#database#', 'master'))
        query = 'SELECT name FROM sys.databases'
        df = pd.read_sql(query, conn)
        black_list = ['master', 'tempdb', 'model', 'msdb']
        for black in black_list:
            df = df.loc[df['name'] != black]
        return list(df['name'])

    def __finder(self, value, database, table, id: int, exact_match: bool):
        try:
            if database == self.database[id]:
                conn = self.conn[id]
            else:
                if type(self.conn[id]) == pyodbc.Connection:
                    self.conn[id].close()
                self.conn[id] = conn = pyodbc.connect(self.conn_string.replace('#database#', database))
                self.database[id] = database

            df = self.tables_info[database][self.tables_info[database]['TABLE_NAME'] == table]
            select_type = None

            base_query = 'SELECT TOP 1 #column# FROM %s WHERE #condition#' % table

            if type(value) is int:
                df = df[(df['DATA_TYPE'] == 'int') | (df['DATA_TYPE'] == 'bigint') | (df['DATA_TYPE'] == 'tinyint') |
                        (df['DATA_TYPE'] == 'numeric')]
                df = df[df['NUMERIC_PRECISION'] >= len(str(value))]
                select_type = int

            elif type(value) is float:
                df = df[df['DATA_TYPE'] == 'numeric']
                df = df[df['NUMERIC_PRECISION'] >= len(str(value))]
                select_type = int

            elif type(value) is str:
                df = df[(df['DATA_TYPE'] == 'varchar') | (df['DATA_TYPE'] == 'char') | (df['DATA_TYPE'] == 'nvarchar')]
                df = df[df['CHARACTER_MAXIMUM_LENGTH'] >= len(str(value))]
                select_type = str

            if df['COLUMN_NAME'].empty:
                return
            columns = str((list(df['COLUMN_NAME']))).replace('[', '').replace(']', '').replace('\'', '')

            condition = ''
            for i, column in enumerate(list(df['COLUMN_NAME'])):
                if i > 0:
                    condition += ' OR '

                if select_type == str:
                    if exact_match:
                        condition += '%s = \'%s\'' % (column, value)
                    else:
                        condition += '%s LIKE \'%%%s%%\'' % (column, value)
                else:
                    condition += '%s = %i' % (column, value)

            query = base_query.replace('#column#', columns).replace('#condition#', condition)
            query += ' GROUP BY %s ORDER BY 1' % columns

            df = pd.read_sql(query, conn)
            columns = []
            if not df.empty:
                for column in df.columns:
                    if select_type == str:
                        if unidecode(value.lower()) in unidecode(str(df[column][0]).lower()):
                            columns.append(column)
                    else:
                        if df[column][0] == value:
                            columns.append(column)

            if len(columns) != 0:
                self.result.append({'database': database, 'table': table, 'columns': columns})

        except pd.errors.DatabaseError as e:
            # print(e)
            pass

        finally:
            self.free_threads.append(id)
