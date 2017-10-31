import progressbar
from sql_adapter import SQLAdapter
from migrate_table import MigrateTableToCollection

class MigrateSQLToMongo(object):
    def __init__(self, sql_uri, mongo_uri):
        self.sql = sql_uri
        self.mongo = mongo_uri
    
    def scan_schema(self):
        sql_adapt = SQLAdapter(sql_uri)
        yield from sql_adapt.scan_schema()

    def queue_table(self, table):
        bobo.delay(self.sql, self.mongo, table)


# TODO: use distributed progress bars, currently each task has its progress bar
@app.task
def bobo(sql_uri, mongo_uri, table):
    bar = progressbar.ProgressBar()
    migrate = MigrateTableToCollection(sql_uri, mongo_uri, bar)
    migrate.move_a_table(table)


mongo_uri = "mongodb://localhost:27017"# get_args()
sql_uri = "sqlite:////tmp/wtf.db"# get_args()
migrate = MigrateSQLToMongo(sql_uri, mongo_uri)
for table in migrate.scan_schema():
    migrate.queue_table(table)

# TODO: wait for consumer to execute
