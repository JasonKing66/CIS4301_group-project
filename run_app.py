import cx_Oracle
import pandas
import tqdm

root = 'system'
passwd = '686893Wj'
host = 'localhost'
port = '1521'
sid = 'orcl'
tns = cx_Oracle.makedsn(host, port, sid)
db = cx_Oracle.connect(root, passwd, tns)

tables_name = "feature_car".upper()


def check_sql(cursor, sql):
    try:
        cursor.parse(sql)
        return True
    except Exception as er:
        print(er)
        return False


def search(sql):
    cursor = db.cursor()
    if check_sql(cursor, sql):
        cursor.execute(sql)
        columns = [col[0] for col in cursor.description]
        cursor.rowfactory = lambda *args: dict(zip(columns, args))
        return cursor.fetchall()
    else:
        return


def tables_show():
    sql = f"select * from tabs"
    tables = []
    for tab in search(sql):
        tables.append(tab['TABLE_NAME'])
    # print(tables)
    return tables


def creat_table(kv):
    cursor = db.cursor()
    ziduan = {x: "VARCHAR2(500 BYTE) VISIBLE" for x in kv}
    sq = ", ".join([f"{x} {ziduan[x]}" for x in ziduan])

    sql = f"""create table {tables_name} ({sq})"""
    # print(sql)
    # a = check_sql(cursor, sql)
    # if a:
    cursor.execute(sql)
    tables = []
    # print(cursor.fetchall())
    return tables
    # else:
    #     print("错误")


def inserts(ci_id, ju_ids):
    if ju_ids:
        cursor = db.cursor()
        it_head = ", ".join([f"{x}" for x in ci_id])
        into = f"""INTO {tables_name} ({it_head}) VALUES %s"""

        sql = """INSERT %s"""
        ne_sql = "\n".join([into % (x,) for x in ju_ids])
        user_sql = sql % (ne_sql,)
        # print(user_sql)
        cursor.execute(user_sql)
        db.commit()
        cursor.close()


if tables_name not in tables_show():
    fram = pandas.read_csv("used_cars_data.csv")
    drop_key = [
        'bed', 'bed_height', 'bed_length', 'city_fuel_economy', 'combine_fuel_economy', 'description', 'fleet',
        'frame_damaged', 'has_accidents', 'highway_fuel_economy', 'isCab', 'is_certified', 'is_cpo',
        'is_oemcpo',
        'main_picture_url', 'cabin', 'owner_count', 'salvage', 'vehicle_damage_category', 'theft_title',
        'major_options'
    ]

    for k in drop_key:
        fram[k] = ''
        # fram = fram.drop(k, axis=1)

    k_v = {x.upper(): x for x in fram.keys() if "UNNAMED: " not in str(x)}
    creat_table(k_v)

    hea = [x for x in k_v]
    values = []
    shap = fram.shape
    for item in tqdm.tqdm(range(shap[0])):
        tx = lambda x: str(fram[k_v[x]][item]).replace("'", "")
        it = f"(" + ", ".join([f"'{tx(x)}'" for x in hea]) + ")"
        values.append(it)
        if len(values) >= 1:
            inserts(hea, values)
            values = []
    if values:
        inserts(hea, values)

    print("Dataset imported. Application initialized: ", tables_name)

# print(cursor.fetchall())

db.close()  # 关闭数据库
