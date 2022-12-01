import base64
import json
import pprint
import re
import uuid

import cx_Oracle
import matplotlib.pyplot as plt
import pandas
import seaborn as sns
import tqdm
import plotly.express as px
import dash
import plotly.graph_objs as go
from dash import Dash, dcc, html, Input, Output, dash_table
from flask_caching import Cache
from plotly.subplots import make_subplots
import dash_bootstrap_components as dbc

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
        return []


def tables_show(whe=None):
    if whe is None:
        whe = {'city'.upper(): 'Bay Shore', 'body_type'.upper(): 'SUV / Crossover'}
    sqls = f'SELECT "{root.upper()}"."{tables_name}".*,ROWID "NAVICAT_ROWID" FROM "{root.upper()}"."{tables_name}"'
    wheres = " AND ".join([f'"{x}" = \'{whe[x]}\'' for x in whe])
    where = f'WHERE {wheres} OFFSET 0 ROWS FETCH NEXT 1000 ROWS ONLY'
    on = 'OFFSET 0 ROWS FETCH NEXT 1000 ROWS ONLY'
    sql = f"""{sqls} {where}"""
    tables = []
    for tab in search(sql):
        tables.append(tab)
    # print(tables)
    return tables


def get_org_result(opt=None):  # 条件查询 获取 获取 并 转换为 pandas
    result = tables_show(opt)
    frame = pandas.DataFrame(result)
    return frame, result


def table_show_where(ci, whe=None):
    """"""
    if whe is None:
        sql = f"SELECT DISTINCT {ci} FROM {tables_name} ORDER BY {ci}"
    else:
        wheres = " AND ".join([f'"{x}" = \'{whe[x]}\'' for x in whe])
        sql = f"SELECT DISTINCT {ci} FROM {tables_name} WHERE {wheres} ORDER BY {ci}"
    tables = []
    for tab in search(sql):
        if str(tab[ci]) == "nan":
            continue
        tables.append(tab[ci])
    return tables


Body_Type = table_show_where("Body_type".upper())

Area_Code = table_show_where("dealer_zip".upper())
Year = [int(x) for x in table_show_where("Year".upper())]

Model_Name = table_show_where("model_name".upper())

Make_Name = table_show_where("Make_Name".upper())
# print(Model_Name)
# print(Body_Type)    # accord


def get_date_where_what(wha, whe=None):
    if whe is None:
        whe = {'city'.upper(): 'Bay Shore', 'body_type'.upper(): 'SUV / Crossover'}
    sqls = f'SELECT %s FROM "{root.upper()}"."{tables_name}"'

    what = ", ".join([f"{tables_name.upper()}.{x.upper()}" for x in wha])
    wheres = " AND ".join([f'"{x.upper()}"=\'{whe[x]}\'' for x in whe])  # 条件

    where = f'WHERE {wheres} OFFSET 0 ROWS FETCH NEXT 1000 ROWS ONLY'
    sql = f"""{sqls % (what,)} {where}"""
    # print(sql)
    tables = []
    # for tab in search(sql):
    #     tables.append(tab)
    return tables


def analyse1(Model_Name=None):
    if not Model_Name:
        Model_Name = "Accord"
    get_date_where_what(wha=["listing_ID", 'Body_type'], whe={"Model_Name": Model_Name})
    sql_it = f'SELECT COUNT({tables_name.upper()}.LISTING_ID) AS "SIZE", BODY_TYPE FROM "{root.upper()}"."{tables_name.upper()}" WHERE "MODEL_NAME"=\'{Model_Name}\' GROUP BY BODY_TYPE'
    Net = {}
    resul = search(sql_it)
    for item in resul:
        bd = item['BODY_TYPE']
        Net[bd] = []
        size_it = item['SIZE']
        Net[bd].append({"label": f"{Model_Name} 的 {bd} SIZE", "value": size_it})
        temp_sql = f'SELECT COUNT({tables_name.upper()}.LISTING_ID) AS "MAX_SIZE" FROM "{root.upper()}"."{tables_name.upper()}" WHERE "BODY_TYPE"=\'{bd}\''
        temp_resul = search(temp_sql)[0]
        max_size = temp_resul['MAX_SIZE']
        Net[bd].append({"label": f"Other models total number in {bd} SIZE", "value": max_size - size_it})
    return Net


def analyse2(Area_Code=None):
    if Area_Code is None:
        Area_Code = ["01060"]
    re_res = []
    are = ", ".join([f"'{x}'" for x in Area_Code])
    sql = f'SELECT {tables_name.upper()}.LISTED_DATE, {tables_name.upper()}.LISTING_ID, {"DEALER_ZIP".upper()} FROM {tables_name.upper()} WHERE {tables_name.upper()}.DEALER_ZIP IN ({are})'
    resul = search(sql)
    fram = pandas.DataFrame(resul)
    fram["mouth"] = fram['LISTED_DATE'].apply(lambda x: int(x.split("-")[1]))
    for area_code in Area_Code:
        re_res.append((area_code, fram[fram["DEALER_ZIP"] == area_code].groupby(['mouth'])[['LISTING_ID']].count()))

    # result1 = fram.groupby(['mouth'])[['LISTING_ID']].count()  # .sort_values('mouth')
    # print(result1)
    return re_res


def analyse3(body_type=None, year='2021'):
    if body_type is None:
        body_type = "Convertible"
    years = [x + int(year) for x in [-4, -3, -2, -1, 0]]
    in_year = ", ".join([f"'{x}'" for x in years])
    print(years, year)
    sql = f"SELECT FEATURE_CAR.PRICE, FEATURE_CAR.YEAR, FEATURE_CAR.BODY_TYPE FROM FEATURE_CAR WHERE FEATURE_CAR.BODY_TYPE='{body_type}' AND FEATURE_CAR.YEAR IN ({in_year})"
    resul = search(sql)
    if resul:
        fram = pandas.DataFrame(resul)
        fram['PRICE'] = pandas.to_numeric(fram["PRICE".upper()], downcast="float")
        fram['YEAR'] = pandas.to_numeric(fram["YEAR".upper()], downcast="integer")

        result = fram.groupby(['YEAR'])[['PRICE']].mean().sort_values("year".upper())
        vauel = {x: [x1 for x1 in result['PRICE']][y] for y, x in enumerate(result.index)}
        pt = lambda x: 0 if x not in vauel else vauel[x]
        pts = [int(pt(x)) for x in years]
        print(vauel, pts, "判断是你")
        trace1 = go.Bar(x=years, y=pts, name=f'{body_type} 五年的均价波动',
                        marker=dict(color=[color_scale[0]] * len(result)), yaxis='y',
                        )
        return trace1
    else:
        trace1 = go.Bar(x=years, y=[0, 0, 0, 0, 0], name=f'{body_type} 五年的均价波动',
                        marker=dict(color=[color_scale[0]] * 2), yaxis='y',
                        )
        return trace1


# analyse3()
# analyse2()


app = dash.Dash(__name__)
app.css.config.serve_locally = True

colors = ['0 255 127', '0 238 118', '0 205 102', '0 139 69', '192 255 62', '0 100 0', '152 251 152', '50 205 50',
          '0 255 0', '144 238 144']
color_scale = []
for color in colors:
    r, g, b = color.split(" ")
    color_scale.append("rgba({},{},{},{})".format(r, g, b, 0.6))
color_scale = color_scale * 2

head = html.Div([
    html.Span(" used car sales\n\n\n\n".format("CIS4301-Project Demo"), className='app-title'),
], className="row header")

app.layout = html.Div([
    head,
    html.P("Year selector："),
    dcc.RangeSlider(Year[0], Year[-1], 1, count=1,
                    marks={i: '{}'.format(i) for i in range(int(Year[0]), int(Year[-1]) + 1, 5)},
                    persistence=True, value=[Year[0], Year[-1]], id='river'),  # 年限 区间

    html.Div([
        html.P("Model_Name：", style={"width": "8%", 'display': 'inline-block'}),
        dcc.Dropdown(id="choice_line", options=[x for x in Model_Name], value=None, placeholder=str(Model_Name[0]),
                     style={"width": "80%", "top": "15px", 'display': 'inline-block'}),

        html.P("Market share by model based on sales numbers"),  # 图 标签
        dcc.Graph(id="mix", style={"width": "100%", 'display': 'inline-block'}, config=dict(displayModeBar=False)),

        html.P("Manufacturer analysis：", style={"width": "8%", 'display': 'inline-block'}),
        dcc.Dropdown(id="small_item1", options=[x for x in Make_Name], placeholder=Make_Name[0],  # multi=True,
                     style={"width": "100%", 'display': 'inline-block'}),

        dcc.Graph(id="mix1", style={"width": "50%", 'display': 'inline-block'}, config=dict(displayModeBar=False)),

        dcc.Graph(id="mix2", style={"width": "50%", 'display': 'inline-block'}, config=dict(displayModeBar=False)),

        html.P("Zipcode：", style={"width": "8%", 'display': 'inline-block'}),
        dcc.Dropdown(id="small_item2", options=[x for x in Area_Code], placeholder=Area_Code[0], multi=True,
                     style={"width": "40%", "top": "15px", 'display': 'inline-block'}),

        html.P("BODY_TYPE ：", style={"width": "6%", 'display': 'inline-block'}),
        dcc.Dropdown(id="small_item3", options=[x for x in Body_Type], placeholder=Body_Type[0],
                     style={"width": "20%", "top": "15px", 'display': 'inline-block'}),

        html.P("YEAR：", style={"width": "6%", 'display': 'inline-block'}),
        dcc.Dropdown(id="small_item4", options=[x for x in Year[5:]], placeholder="2021",
                     style={"width": "18%", "top": "15px", 'display': 'inline-block'}),

        dcc.Graph(id="mix3", style={"width": "50%", 'display': 'inline-block'}, config=dict(displayModeBar=False)),

        dcc.Graph(id="mix4", style={"width": "50%", 'display': 'inline-block'}, config=dict(displayModeBar=False)),

        dbc.Container([
            dbc.Label('Top 20 entries based on search creteria'),
            dash_table.DataTable(
                id='dash-table',
                page_size=15,
                page_action='custom',
                page_current=0,
                style_header={
                    'font-family': 'Times New Romer',
                    'font-weight': 'bold',
                    'text-align': 'center'
                },
                style_data={
                    'font-family': 'Times New Romer',
                    'text-align': 'center'
                },
            )

        ])

    ], className="col-4 chart_div", )
])


@app.callback(
    Output('mix', 'figure'),
    Input("choice_line", "value")
)
def get_analyse1_pei(choice_line):
    cafe_colors = ['rgb(200, 75, 126)', 'rgb(18, 36, 37)', 'rgb(134, 53, 101)',
                   'rgb(136, 55, 57)', 'rgb(206, 4, 4)']
    data1 = []
    al1 = analyse1() if choice_line is None else analyse1(choice_line if type(choice_line) == str else choice_line[0])
    x = 1
    specs = [[{'type': 'domain'} for i in range(len(al1))]]
    fig = make_subplots(rows=1, cols=len(al1), specs=specs)
    for i in al1:
        frame1 = pandas.DataFrame(al1[i])
        fig.add_trace(go.Pie(labels=frame1['label'],
                             values=frame1['value'],
                             title=f"{i}",
                             marker_colors=cafe_colors), 1, x)
        x += 1
        # break
    fig.update_traces(hoverinfo='label+percent+name', textinfo='value')  # ['label', 'text', 'value', 'percent']

    layout = go.Layout(
        margin=dict(l=60, r=60, t=30, b=50),
        showlegend=False,
        title="分析图",
        annotations=[
            {
                'font': {'size': 18},
                'showarrow': False,
                'text': 'AU.SHF多头持仓',
                'x': 0.45,
                'y': 0.754
            },
            {
                'font': {'size': 18},
                'showarrow': False,
                'text': 'AU.SHF多头持仓',
                'x': 0.794,
                'y': 0.754

            },
            {
                'font': {'size': 18},
                'showarrow': False,
                'text': 'AU.SHF多头持仓',
                'x': 0.255,
                'y': 0.23
            },
            {
                'font': {'size': 18},
                'showarrow': False,
                'text': 'AG.SHF空头持仓',
                'x': 0.6,
                'y': 0.23
            }],
    )

    fig.update(layout_showlegend=True)

    # fig = go.Figure(fig)
    fig1 = go.Figure(fig, layout=layout)

    return fig1


@app.callback(
    Output('mix1', 'figure'), Output('dash-table', 'data'), Output('mix2', 'figure'),
    Input("river", "value"), Input("small_item1", "value")
)
def get_mix(river, choice_line):
    print(river, choice_line)
    data = []
    data1 = []

    if not choice_line:
        its = [{'MAKE_NAME': "Acura"}]
    elif type(choice_line) == str:
        its = [{'MAKE_NAME': choice_line}]
    elif type(choice_line) == list:
        its = [{'MAKE_NAME': x} for x in choice_line]
    else:
        its = []
        print("异常", choice_line)

    for it in its:
        result, result_dat = get_org_result(opt=it)
        result["year".upper()] = pandas.to_datetime(result["year".upper()]).apply(lambda x: x.strftime('%Y'))
        year_min = str(min(river)) <= result['Year'.upper()]
        year_max = result['Year'.upper()] <= str(max(river))

        result = result[year_min]
        result = result[year_max]
        if result_dat:
            result['price'] = pandas.to_numeric(result["price".upper()], downcast="float")
            result['dealer_zip'] = pandas.to_numeric(result["dealer_zip".upper()], downcast="integer")

            result1 = result.groupby(["year".upper()])[['price']].mean().sort_values("year".upper())

            result2 = result.groupby(["year".upper()])[['dealer_zip']].count().sort_values("year".upper())

            result3 = pandas.DataFrame(result['MODEL_NAME'].value_counts(sort=False))
            print(result3)
            # result_items[y][0]

            # trace1 = go.Bar(x=result1.index, y=result1['price'], name=f'{it["MAKE_NAME"]} 均价',marker=dict(color=[color_scale[0]] * len(result1)), yaxis='y',)
            data.append(go.Bar(x=result1.index, y=result1['price'], name=f'{it["MAKE_NAME"]} 均价',
                               marker=dict(color=[color_scale[0]] * len(result1)), yaxis='y', ))

            # trace2 = go.Scatter(x=result2.index, y=result2['dealer_zip'], name=f'{it["MAKE_NAME"]} 销售量', yaxis='y2')

            data1.append(
                go.Scatter(x=result2.index, y=result2['dealer_zip'], name=f'{it["MAKE_NAME"]} 销售量', yaxis='y2'))

    layout = go.Layout(
        margin=dict(l=60, r=60, t=30, b=50),
        showlegend=False,
        xaxis=dict(
            title='by year',
        ),
        yaxis=dict(
            side='left',
            title='average price',
        ),
        yaxis2=dict(
            showgrid=False,  # 网格
            title='listing numbers',
            anchor='x',
            overlaying='y',
            side='right'
        ),
    )

    GT = pandas.DataFrame()
    Vale = ["Year".upper(), "MAKE_NAME".upper(), "MODEL_NAME".upper(), "Price".upper(), "Mileage".upper(),
            "Dealer_zip".upper()]
    for yi in Vale:
        GT[yi] = result[yi]
    fig1 = go.Figure(data=data, layout=layout)
    fig2 = go.Figure(data=data1, layout=layout)
    table_opt = GT.iloc[:20].to_dict('records')
    return fig1, table_opt, fig2


@app.callback(
    Output('mix3', 'figure'),
    Input("small_item2", "value")
)
def get_analyse2_pei(code):
    if not code:
        its = None
    elif type(code) == str:
        its = [code]
    elif type(code) == list:
        its = [x for x in code[-3:]]
    else:
        its = None

    results = analyse2(its)
    data1 = []
    for cod, result in results:
        print(cod, result)
        data1.append(
            go.Scatter(x=result.index, y=result['LISTING_ID'], name=f'{cod} 区域 统计', yaxis='y2'))

    layout = go.Layout(
        margin=dict(l=60, r=60, t=30, b=50),
        showlegend=False,
        yaxis=dict(
            side='left',
            title='LISTING_ID COUNT',
        ),
        yaxis2=dict(
            side='left',
            title='LISTING_ID COUNT',
        ),
        # yaxi3=dict(
        #     side='right',
        #     title='喜欢',
        # ),
        xaxis=dict(
            title='by month',
        ),
    )
    fig1 = go.Figure(data=data1, layout=layout)
    return fig1


@app.callback(
    Output('mix4', 'figure'),
    Input("small_item3", "value"), Input("small_item4", "value")
)
def get_analyse3_pei(body_type, year):
    if year is None:
        year = "2021"

    layout = go.Layout(
        margin=dict(l=60, r=60, t=30, b=50),
        showlegend=False,
        yaxis=dict(
            side='right',
            title='average price',
        ),
        xaxis=dict(
            title='by year',
        ),
    )

    fig = analyse3(body_type, year)
    fig1 = go.Figure(data=[fig], layout=layout)
    return fig1


if __name__ == '__main__':
    app.run_server(debug=True, threaded=True, port=7777)
