import duckdb
def get_con():
    con = duckdb.connect(config={"allow_unsigned_extensions": True})
    con.sql(""" INSTALL h3ext FROM 'https://pub-cc26a6fd5d8240078bd0c2e0623393a5.r2.dev';
                LOAD h3ext;
                INSTALL spatial;
                LOAD spatial;
                INSTALL httpfs;
                LOAD httpfs;""")
    return con