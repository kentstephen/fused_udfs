@fused.udf
def udf(res:int= 9,path: str = "s3://fused-sample/demo_data/housing/housing_2024.csv"):
    import pandas as pd

    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/be3bc93/public/common/"
    ).utils
    qr = f"""select h3_cell_to_parent('8628f1757ffffff', 6) as hex"""
    con = utils.duckdb_connect()
    df = con.query(qr).df()
    # df =  pd.DataFrame('hex': "8628f1757ffffff")
    return df