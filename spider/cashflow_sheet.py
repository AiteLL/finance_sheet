"""

cashflow -

Author: LiJuNbO
Time: 2022/10/18 10:45
"""
import json

import pandas as pd
import requests
from pymyorm.database import Database

from pymyorm.model import Model


class User(Model):
    tablename = 't_cashflow'


def cashflow_sheet(code: str = '600481', DATE_TYPE_CODE: str = '') -> pd.DataFrame:
    """
    :DATE_TYPE_CODE: 报告期，choice of {
        '全部报告期': '',
        '年报': '(DATE_TYPE_CODE="001")',
        '中报': '(DATE_TYPE_CODE="002")',
        '一季报': '(DATE_TYPE_CODE="003")',
        '三季报': '(DATE_TYPE_CODE="004")'}
    :code: 股票代码
    :return: 利润表
    """

    # code = input('请输入股票代码：')
    # code_ = input('请输入报告期：')
    # DATE_TYPE_CODE = f'(DATE_TYPE_CODE="{code_}")'

    url = 'https://datacenter-web.eastmoney.com/api/data/v1/get'
    params = {
        'callback': 'jQuery112305243412442554345_1666057225776',
        'sortColumns': 'REPORT_DATE',
        'sortTypes': -1,
        'pageSize': 50,
        'pageNumber': 1,
        'columns': 'ALL',
        'filter': f'(SECURITY_CODE="{code}"){DATE_TYPE_CODE}',
        'reportName': 'RPT_DMSK_FN_CASHFLOW'
    }

    resp = requests.get(url, params=params)
    data_dict = json.loads(resp.text[42:-2])

    big_df = pd.DataFrame(data_dict['result']['data'][0], index=[0])

    for i in range(1, len(data_dict['result']['data'])):
        temp_df = pd.DataFrame(data_dict['result']['data'][i], index=[i])

        big_df = pd.concat([big_df, temp_df])

    for page in range(2, data_dict['result']['pages']+1):
        params.update(
            {
                "pageNumber": page,
            }
        )
        resp = requests.get(url, params=params)
        data_dict = json.loads(resp.text[42:-2])

        for i in range(len(data_dict['result']['data'])):
            temp_df = pd.DataFrame(data_dict['result']['data'][i], index=[i])

            big_df = pd.concat([big_df, temp_df])

    big_df.columns = [
        "TS 股票代码",
        "-",
        "-",
        "-",
        "名称",
        "行业名称",
        "-",
        "-",
        "-",
        "-",
        "-",
        "-",
        "公告日期",
        "报告日期",
        "经营性现金流量净额",
        "经营性现金流量净额占比",
        "销售商品、提供劳务收到的现金金额",
        "销售商品、提供劳务收到的现金占比",
        "支付员工现金金额",
        "支付员工现金占比",
        "投资性现金流量净额",
        "投资性现金流量净额占比",
        "取得投资收益收到的现金金额",
        "取得投资收益收到的现金占比",
        "构建固定资产、无形资产和其他长期资产支付的现金金额",
        "构建固定资产、无形资产和其他长期资产支付的现金占比",
        "融资性现金流量净额",
        "融资性现金流量净额占比",
        "净现金流",
        "净现金流同比",
        "-",
        "-",
        "-",
        "-",
        "-",
        "-",
        "-",
        "-",
        "-",
        "-",
        "-",
        "-",
        "-",
        "-",
        "-",
        "-",
        "-",
        "-"
    ]
    big_df = big_df[
        [
            "TS 股票代码",
            "名称",
            "行业名称",
            "公告日期",
            "报告日期",
            "净现金流",
            "净现金流同比",
            "经营性现金流量净额",
            "经营性现金流量净额占比",
            "销售商品、提供劳务收到的现金金额",
            "销售商品、提供劳务收到的现金占比",
            "支付员工现金金额",
            "支付员工现金占比",
            "投资性现金流量净额",
            "投资性现金流量净额占比",
            "取得投资收益收到的现金金额",
            "取得投资收益收到的现金占比",
            "构建固定资产、无形资产和其他长期资产支付的现金金额",
            "构建固定资产、无形资产和其他长期资产支付的现金占比",
            "融资性现金流量净额",
            "融资性现金流量净额占比"

        ]
    ]
    big_df.set_index('报告日期', inplace=True)
    big_df.sort_index(inplace=True)
    return big_df


def cashflow():
    big_df = cashflow_sheet()
    big_df.fillna(0, inplace=True)
    big_df.reset_index(inplace=True)

    config = dict(
        source='mysql',
        host='localhost',
        port=3306,
        user='root',
        password='admin11',
        database='finance',
        charset='utf8',
        debug=True
    )

    # 连接数据库
    Database.connect(**config)

    sql = 'drop table if exists `t_cashflow`'

    Database.execute(sql)

    with open('../sql/cashflow.sql', encoding='utf-8') as fp:
        sql = fp.read()
    Database.execute(sql)

    fields = big_df.columns.tolist()
    values = big_df.values.tolist()
    User.insert(fields, values)


if __name__ == '__main__':

    cashflow()
