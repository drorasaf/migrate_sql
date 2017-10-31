import sqlalchemy
import MySQLdb.cursors

class SQLAdapter(object):
    
    def __init__(self, uri):
        """ URI should include schema """
        self.sql_type = self._get_sql_type(uri)
        self.conn = self._create_server_side_cursor_conn(uri, self.sql_type)
        
    def _create_server_side_cursor_conn(self, uri, sql_type):
        if sql_type == 'mysql':
            connect_args={'cursorclass': MySQLdb.cursors.SSCursor}
            return sqlalchemy.create_engine(uri, connect_args)
        return  sqlalchemy.create_engine(uri)

    def _get_sql_type(self, uri):
        sql_type_maps = {'sqlite': 'sqlite',
                         'mysql': 'mysql',
                         'mssql': 'mssql',
                         'postgres': 'postgres'}

        for sql_type in sql_type_maps:
            if uri.startswith(sql_type):
                 return sql_types_maps[sql_type]
    # MySQL implmentation
    def scan_schema():
        """ returns an iterator of the names of tables ni the schema """
        # TODO: postgres has issues with rowcount
        query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE " + \
            " TABLE_TYPE='BASE_TABLE' AND TABLE_SCHMEA=" + self.schema + ";"
        sql_result = self.conn.execute(query)
        yield sql_result

    def get_table_size(self, table):
        """ returns an int representing the amount of lines in the table """
        # TODO: postgres has issues with rowcount
        query = "SELECT * FROM " + str(table) + ";"
        sql_result = self.conn.execute(query)
        return sql_result.rowcount

    def get_table_column_names(self, table):
        query = "SELECT * FROM " + str(table) + " LIMIT 1;"
        cur = self.conn.execute(query)
        columns = map(lambda x:x[0], cur.cursor.description)
        return columns

    def get_table_cursor(self, table):
        """ returns an sqlalchemy cursor """
        query = "SELECT * FROM " + str(table) + ";"
        cur = self.conn.execute(query)
        return cur

    def get_table_cursor_by_index(self, table):
        """ scans table to get indexes with highest cardinality and use it to iterate table by chunks """
        pass
