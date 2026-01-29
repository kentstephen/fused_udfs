




def get_con():
    import duckdb
    con = duckdb.connect()
    con.sql(""" INSTALL h3 from community;
                LOAD h3;
                INSTALL spatial;
                LOAD spatial;
                INSTALL httpfs;
                LOAD httpfs;
                SET s3_region='us-west-2';""")
    return con