import pymongo
from sql_adapter import SQLAdapter

BATCHSIZE=1000

class MigrateTableToCollection():

    def __init__(self, sql_uri, mongo_uri, progress_bar):
        self.sql = SQLAdapter(sql_uri)
        self.mongo = pymongo.MongoClient(mongo_uri)
        self.progress_bar = progress_bar
        self.log = logging.getLogger('Migration')

    def move_a_table(self, table):
        table_size = self.sql_get_table_size(table)
        cursor = self.sql.get_table_cursor(table)
        column_names = self.sql.get_table_column_names(table)
        cursor_exhausted = False
        batch = 0
        self.progress_bar.start()
        self.log.info('started migration table: ' + str(table))

        while cursor_exhausted is False:
             try:
                 sql_result = cursor.fetchmany(size=BATCHSIZE)
             except sqlalchemy.exc.ResourceClosedError:
                 cursor_exhausted = True
             if sql_result is None or len(sql_result) == 0:
                 cursor_exhausted = True

             # encoding issues
             # any other data issues should be handled here
             # data = convert_encoding(sql_result)
             data = self._convert_result_to_mongo(sql_result)
             self._insert_docs(self.mogno, data)
             batch += 1
        
             self.progress_bar.update(batch)

        self.progress_bar.finish()
        self.log.info('Migrated table: ' + str(table) + 'successfully')

    def _insert_docs(self, mongo, data):
        try:
            self.mongo.insert_many(data)
        except pymongo.errors.PyMongoError:
            for doc in data:
                try:
                    self.mongo.insert_one(doc)
                except pymongo.errors.PyMongoError:
                    self.log.error('Unable to insert doc: ' + str(doc))

    def _convert_result_to_mongo(self, data, column_names):
        return map(lambda x: dict(zip(column_names, x)), data)
