


# native
from infrastructure.util.database import BaseDB



class BaseStoredProc(object):
    """
    Base class for representations of database tables. Given a database and table name, this class
    will reflect the table and then provide accessors to the columns and a generic query function.
    Custom or complex sql queries can use table_def to build the query.
    """
    config_section = None
    schema = 'dbo'
    proc_name = None
    _database = None

    def __init__(self):
        """
        Initialize BaseStoredProc object

        :returns: None
        """

        if not self.config_section:
            raise RuntimeError('Instances of BaseStoredProc must provide a config_section which contains DB connection details')

        if not self.proc_name:
            raise RuntimeError('Instances of BaseStoredProc must set proc_name')

        # If we're overriding the environment we need to recreate the database
        self._database = BaseDB(self.config_section)

    @property
    def cn(self):  # Class name. Avoids having to print/log type(self).__name__.
        return type(self).__name__

