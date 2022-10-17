import pymysql
from sqlalchemy import create_engine
import pandas as pd

class PyMySQL:
    pymysql.install_as_MySQLdb()
    host = '127.0.0.1'
    user = 'root'
    passwd = 'asdhgf@0000'
    db_name = 'da'
    port = 3306
    db = pymysql.connect(host=host, user=user, passwd=passwd, db=db_name, port=port, charset='utf8', autocommit=True)
    db.ping(True)  # Use mysql ping to check connection and reconnect automatically on timeout
    cur = db.cursor()
    engine = create_engine('mysql+pymysql://' + user + ':' + passwd + '@' + host + ':' + str(port) + '/' + db_name + '?charset=utf8')

    @classmethod
    def append_df_to_db(cls, df, name, con=engine, if_exists='append', schema=db_name):
        pd.io.sql.to_sql(frame=df, name=name, con=con, if_exists=if_exists, schema=schema, index=False)

    @classmethod
    def get_from_db(cls, sql):
        return pd.read_sql(sql, con=cls.cur.connection)