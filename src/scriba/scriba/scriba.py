import mysql.connector as mtor
from mysql.connector.abstracts import MySQLCursorAbstract
from typing import List, Dict, Union, Tuple, Optional, Any
import csv

class DbManager:
    # the exsistence of the table is taken as an assunption
    # but during initialization if the table doesn't exist is not a problem
    def __init__(self,
                 db: str,
                 source: str,
                 reference_table: str, # used in case of data should be organized through a reference table
                 resolution: Optional[str] = None, # used in case of OHLC data
                 table: Optional[str] = None) -> None:

            self.db = db
            self._table = table
            self.source = source
            self.reference_table = reference_table
            self.resolution = resolution

    @property
    def table(self) -> str:
        return self._table

    @table.setter
    def table(self, new_table : str) -> None:
        self._table = new_table

    @staticmethod
    def _type_map() -> Dict[type, str]:
        """this method provides a way to map python to mysql types"""

        # Date time must be added in next commit
        py_sql_map = {
            int: 'INT',
            float: 'DOUBLE',
            str: 'TEXT'
        }

        return py_sql_map

    def _map_sql_to_py(self, mysql_type: Union[bytes, str]) -> Optional[type]:

        # Note: check if caller of this method is able to handle a None
        """This method provides a way to map bytes type provided by the query 'describe' to python's type"""
        byte_type_mapping = {
                                b'int': int,
                                b'text': str,
                                b'double': float
                            }
        if isinstance(mysql_type, str):
            py_to_sql_string = {val : key for key, val in self._type_map().items()}
            return py_to_sql_string.get(mysql_type)
        if isinstance(mysql_type, bytes):
            return byte_type_mapping.get(mysql_type, None)

        raise TypeError(f'{mysql_type} must be bytes or str')

    def _map_py_to_sql(self, py_type: Union[str, int, float]) -> Optional[str]:

        """Cast py_type to str, int or float"""

        if not isinstance(py_type, (str, int, float, type(None))):
            raise ValueError(f"Unexpected type {type(py_type)}. Expected str, int, or float.")
        
        try: 
            int(py_type)
            return self._type_map().get(int)
        except (ValueError, TypeError):
            pass
        
        try:
            float(py_type)
            return self._type_map().get(float)
        except (ValueError, TypeError):
            pass

        return self._type_map().get(type(py_type))

    @staticmethod
    def _condense(data : List[Dict[str, Union[str, int, float]]]) -> Dict[str, Union[str, int, float]]:
        """Aggregate all the keys from the list of dictionaries took in input. The order of the values is kept inserting None if needed."""

        condensed = {}

        for num, diz in enumerate(data):
            current_keys = set(diz.keys())
            for key in diz.keys():
                if key not in condensed.keys():
                    gap = [None for _ in range(num)]
                    gap.append(diz[key])
                    condensed[key] = gap
                else:
                    condensed[key].append(diz[key])
            condensed_keys = set(condensed.keys())
            put_none = condensed_keys.difference(current_keys)
            for el in put_none:
                condensed[el].append(None)
        return condensed

    @staticmethod
    def cast(val : Union[str, int, float]):
        try: 
            return int(val)
        except (ValueError, TypeError):
            pass
        try:
            return float(val)
        except (ValueError, TypeError):
            pass

        return val

    def read_csv(self, path: str) -> Union[List[Dict[str, Any]], Dict[str, Any]]:

        """Read a csv file and return a list of dict or just a dict"""

        # this method should be updated to handle cases where the header is given in a separate file
        
        with open(path, mode='r') as file:

            csv_reader = csv.reader(file)

            header = next(csv_reader)
            header = [f'{str(key).replace(" ", "_")}' for key in header] # <-- This should be a function or a method since is super useful

            data = [{key : self.cast(val) for key, val in zip(header, row)} for row in csv_reader]

        return data

    def _table_exist(self, crs = MySQLCursorAbstract) -> bool:

        """a query to check if the specified table exist"""

        query = f"""SELECT COUNT(*) FROM information_schema.tables
                    WHERE
                    table_schema = '{self.db}'
                    AND
                    table_name = '{self.table}'"""

        crs.execute(query)
        res = crs.fetchone()

        return True if res[0] else False
    
    def _describe(self, crs: MySQLCursorAbstract) -> List[Tuple]:

        """This method execute a DESCRIBE query then return a list of tuple, each tuple consists of two elements,
        the first is the name of the column, the latter the bytes type of the column"""

        query = f'DESCRIBE {self.table};'
        crs.execute(query)
        res = crs.fetchall()

        a = [el[0:2] for el in res]
        return a

    def _alter(self, crs: MySQLCursorAbstract, col_to_add: List[Tuple], col_to_mod: List[Tuple]) -> None:

        """This method alters the db's table by adding columns, modifying columns or both"""
        
        # white space replacement should be done before when building the add_column list
        # or maybe better before add column, as a step to make the keys compliant to the db/python syntax
        
        add_column = [f"ADD COLUMN {key} {type_}" for key, type_ in col_to_add] # type specified in query is a python type
        mod_column = [f"MODIFY COLUMN {key} {type_}" for key, type_ in col_to_mod]

        join_commands = lambda commands: ",\n".join(commands)

        add_column = join_commands(add_column)
        mod_column = join_commands(mod_column)
        print(f'\nadd column inside alter\n{add_column}')

        query = lambda table, command: f'ALTER TABLE {table}\n{command}'

        multi_flag = lambda array: True if len(array) == 1 else False

        if col_to_add:
            crs.execute(query(table= self.table, command= add_column), multi=multi_flag(add_column))
            print(f'query di alter1 \n\n{query(self.table, add_column)}\n')
        if col_to_mod:
            crs.execute(query(table= self.table, command= mod_column), multi=multi_flag(mod_column))
            print(f'query di alter2 \n\n{query(self.table, mod_column)}\n')

    # this method should be renamed as '_insert_data' because there's a new method 
    # that insert data using to the ref table

    def _insert(self, crs: MySQLCursorAbstract, data: Dict[str, List[Union[str, float, int]]] = None) -> None:

        """sql query to insert data"""

        columns = list(data.keys())

        placeholders = ', '.join(['%s' for _ in columns])

        print(f'hey sono in insert')

        values_to_insert = []
        for row_idx in range(len(list(data.values())[0])):  
            row_values = [data[column][row_idx] for column in columns]
            values_to_insert.append(row_values)

        insert_query = f'INSERT INTO {self.table} ({", ".join(columns)}) VALUES ({placeholders})'

        print(insert_query)

        crs.executemany(insert_query, values_to_insert)
    
    def _get_id(self,
                meta_table : str,
                meta_id_column: str,
                meta_value_column: str,
                value: str,
                crs: MySQLCursorAbstract,
                cnx : mtor.MySQLConnection,
                ) -> int:

        """takes a meta table name as input, checks if name is in table then return its ID or raise an error if not"""

        query = f"SELECT {meta_id_column} FROM {meta_table} WHERE {meta_value_column} = '{value}'"

        crs.execute(query)
        result = crs.fetchone()

        return result # result should be returned properly sliced and casted to a string 

    def _insert_meta(self) -> None:
        self._get_id()


    def create(self, crs: MySQLCursorAbstract) -> None:

        # So here I'm using create as an util to use when importing this class,
        # it would be probably better to use it inside the class (_create) and 
        # adding directly all the columns here.

        """Create a table in sql"""

        primary_key = 'id INT AUTO_INCREMENT PRIMARY KEY' 
        create = f'CREATE TABLE `{self.table}` ({primary_key})'
        crs.execute(create)

    def data_types(self, data: Dict[str, Union[str, int, float]]) -> List[str]:
        """Takes in input the data to be inserted to determin the type of each column then a list of sql types is returned, each element as string"""

        final_type = []
        for values in data.values():
            types = set(list(map(self._map_py_to_sql, values)))
            print(types)
            if None in types and len(types)  == 2:
                final_type.append(types.difference({None}).pop())
            elif None not in types and len(types) == 1:
                final_type.append(types.pop())
            else: final_type.append('TEXT')
        
        for el in final_type: print(el)
        return final_type

    def _validate_data(self, data : Union[List[Dict[str, Union[str, float, int]]], Dict]) -> Dict[str, Union[str, float, int]]:
        
        """validate data checks if data is in the proper format then condense the list of dict to single dict if necessary"""
        
        if isinstance(data, list):
            for _ in data:
                if isinstance(_, dict):
                    pass
                else: raise TypeError("Data should be list[dict]")
            data = self._condense(data= data)
        elif isinstance(data, Dict):
            pass    
        else: raise TypeError(f'Data must be Union[List[Dict], Dict]')

        return data

    # correctnes of the data to be written is taken as assumption
    def write(self, crs: MySQLCursorAbstract, cnx : mtor.MySQLConnection, data: Union[List[Dict], Dict]) -> None:

        # a List[Dict] or Dict is passes as input, in the first case data is condensed in one Dict

        data = self._validate_data(data= data)
        datas_type = self.data_types(data= data)

        if not self._table_exist(crs=crs):
            self.create(crs=crs)
            cnx.commit()

        print(f'\ndata types \n{datas_type}')
    
        try :
                print("\n\nhello inside write try")
                described_table = self._describe(crs= crs)

                # code could be optimized by describing the table only when modification are done
                # or by keeping trace of the modification so that a description of the table is needed only at the first iteration

                dbs_columns_name = [_[0] for _ in described_table]
                dbs_columns_type = [_[1] for _ in described_table]
                print(f'\ndb column name \n{dbs_columns_name}')
                print(f'\ndb column type \n{dbs_columns_type}')
                column_to_add = [(col, el) for col, el in zip(data.keys(), datas_type) if not (col in dbs_columns_name)]
                print(f'\ncolumn to add \n{column_to_add}')

                column_to_modify = [(col, _type)
                                    for col, _type in zip(data.keys(), datas_type)
                                    if
                                        col in dbs_columns_name and
                                        self._map_sql_to_py(_type) != self._map_sql_to_py(dbs_columns_type[dbs_columns_name.index(col)])]
                
                for col, _type in zip(data.keys(), datas_type):
                    if col in dbs_columns_name and self._map_sql_to_py(_type) != self._map_sql_to_py(dbs_columns_type[dbs_columns_name.index(col)]):
                        print(f'\n\n\ntipo da inserire {self._map_sql_to_py(_type)}\ntipo in tabella {self._map_sql_to_py(dbs_columns_type[dbs_columns_name.index(col)])}')
                
                print(f"")

                print(f'\n\ncolumn to modify\n\n {column_to_modify}')

                if column_to_add or column_to_modify:
                    self._alter(crs= crs, col_to_add= column_to_add, col_to_mod= column_to_modify)
                    cnx.commit() # it should work also without this commit
                self._insert(crs=crs, data=data)
        except mtor.errors.ProgrammingError as err:
            if err.errno == 1146:
                return f'Table {self.db}.{self.table} doesn\'t exist.'
            else: 
                return f'{err}'