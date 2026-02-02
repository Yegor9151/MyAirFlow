from airflow.models import DAG
from airflow.settings import AIRFLOW_HOME
from airflow.decorators import task, task_group

from datetime import datetime, timedelta
import time
from src import utils, specific

import pandas as pd
import json


# Параметры дага
DAG_ID = "partners_telegram_daily"
DESCRIPTION = "Ежедневно пушит сообщение в TG каналы"
SCHEDULE_INTERVAL = "0 5 * * *"
OWNER = "Sorokin Yegor"
START_DATE = datetime(2025, 6, 19)
TAGS = ["bot", "partners"]

# Куда отправлять сообщения
CHAT_ID = "ChatId"
# CHAT_ID = "ChatId_TEST"

# Домашняя дирректория дага
DAG_HOME = AIRFLOW_HOME + "/dags/partners/partners_data_daily"
DATA_HOME = AIRFLOW_HOME + "/data/datafiles"

# Яндекс
YADISK_LIB = {
    "cloud": "/Partners/Data-Lens/finance/Общий файл/FranchiseBot_ChatId.xlsx",
    "local": DATA_HOME + "/FranchiseBot_ChatId.xlsx"
}

# Запросы для экстрактора
SQL_FILES = {
    "oper_metrics": DAG_HOME + "/CH_oper_metrics_v3.sql",
    "minute_pass": DAG_HOME + "/CH_minute_pass.sql"
}


class DataExtraction:
    def __init__(self):
        self.__client_ch = specific.Client.clickhouse()
        self.__client_ya = specific.Client.yadisk()

    def ch_query(self, sql_file, ds):
        ds = pd.to_datetime(ds).date()

        query = utils.open_file(sql_file)
        query = utils.Temp(query).replace({
            "{COMPANY_TYPES}": "'Franchise'",
            "{DS}": ds,
            "{DE}": ds + timedelta(days=1)
        })
        print(query)

        df = self.__client_ch.query_df(query)
        print(df.info())

        return df.to_json()

    def lib(self):
        self.__client_ya.download(YADISK_LIB["cloud"], YADISK_LIB["local"])
        df = pd.read_excel(YADISK_LIB["local"], sheet_name="СПР")
        df = df[df["in bot"] == 1]
        df = df[["CompanyName", CHAT_ID]]
        print(df.info())
        return df.to_json()


class DataTransform:
    def report(self, oper_metrics, minute_pass, lib):
        df_oper_metrics = pd.DataFrame(json.loads(oper_metrics))
        df_minute_pass = pd.DataFrame(json.loads(minute_pass)).drop(["dt"], axis=1)

        oper_metrics_pivot = df_oper_metrics.groupby(["dt", "CompanyName", "VendorId"], dropna=False).agg({
            "Total_bikespark": "sum",
            "work": "sum",
            "avail": "sum",
            "ActiveUsers": "sum",
            "PaidRides": "sum",
            "GMVRides_RUB": "sum",
            "Cash_RUB": "sum",
            "PaidBonus_RUB": "sum",
            "FreeBonus_RUB": "sum",
        }).reset_index()
        minute_pass_pivot = df_minute_pass.groupby(["VendorId"]).agg({"GMVMinutePass_RUB": "sum"}).reset_index()

        result = oper_metrics_pivot.merge(minute_pass_pivot, on="VendorId", how="left").fillna(0)
        result["rides_by_oper"] = result['PaidRides'] / result['work']
        result["rides_by_user"] = result['PaidRides'] / result['ActiveUsers']
        result["gmv_by_oper"] = (result['GMVRides_RUB'] + result['GMVMinutePass_RUB']) / result['work']
        result["avail_by_oper"] = result['avail'] / result['work']
        result["GMVTotal_RUB"] = result['GMVRides_RUB'] + result['GMVMinutePass_RUB']

        print(result.info())

        return result.to_json()

    def text(self, label, df):
        dates = pd.to_datetime(df["dt"]).dt.date.unique()
        msg = f"""
<b>{label}</b>
<b>{' - '.join([dates[0], dates[-1]]) if dates.shape[0] > 1 else dates[0]}</b>

GMV Total: <b>{(df['GMVTotal_RUB']).round().fillna(0).values[0]:_.0f} руб</b>
⟶ GMV MinutePass: <b>{df['GMVMinutePass_RUB'].round().fillna(0).values[0]:_.0f} руб</b>
⟶ GMV Rides: <b>{df['GMVRides_RUB'].round().values[0]:_.0f} руб</b>
    ⤷ Cash: <b>{df['Cash_RUB'].round().values[0]:_.0f} руб</b>
    ⤷ PaidBonus: <b>{df['PaidBonus_RUB'].round().values[0]:_.0f} руб</b>
    ⤷ FreeBonus: <b>{df['FreeBonus_RUB'].round().values[0]:_.0f} руб</b>
Rides: <b>{df['PaidRides'].values[0]:_}</b>
Общий парк: <b>{df['Total_bikespark'].values[0]:_}</b>
Операционный парк: <b>{df['work'].values[0]:_}</b>
Доступный парк: <b>{df['avail'].values[0]:_}</b>
Платных поездок на самокат (от опер парка): <b>{(df["rides_by_oper"]).fillna(0).values[0]:_.2f}</b>
GMV на 1 самокат (от опер парка): <b>{(df["gmv_by_oper"]).fillna(0).values[0]:_.2f}</b>
Платных поездок на активного пользователя: <b>{(df["rides_by_user"]).fillna(0).values[0]:_.2f}</b>
Доступность (от опер парка): <b>{(df["avail_by_oper"] * 100).fillna(0).values[0]:_.2f}%</b>

{df["CompanyName"].iloc[0]}
""".replace('_', ' ')
        return msg

    def messages(self, data):
        df = pd.DataFrame(json.loads(data))
        print(df.info())

        all_msgs = {}
        for company in df["CompanyName"].unique():
            print("Компания:", company)
            df_company = df[df["CompanyName"] == company]
            all_msgs[company] = []

            for vendor in df_company["VendorId"].unique():
                print("Вендор:", vendor)
                df_vendor = df_company[df_company["VendorId"] == vendor]

                if df_vendor.shape[0] == 0:
                    continue
                if df_vendor["GMVRides_RUB"].values[0] == 0 and df_vendor["GMVMinutePass_RUB"].values[0] == 0:
                    continue

                text = self.text(vendor, df_vendor)
                print(text)

                all_msgs[company].append(text)

        return all_msgs


class DataLoad:
    def __init__(self):
        self.__BOT_TOKEN = specific.Creds.telegram()
        self.__telegram_bot = utils.TelegramBot(self.__BOT_TOKEN)

    def send_message(self, chat_id, messages):
        url = None
        for msg in messages:
            url = self.__telegram_bot.send_message(chat_id, msg)
        return url
    

with DAG(
    dag_id=DAG_ID,
    description=DESCRIPTION,
    schedule_interval=SCHEDULE_INTERVAL,
    default_args={
        "owner": OWNER,
        "start_date": START_DATE,
    },
    max_active_runs=1,
    tags=TAGS,
    catchup=False
) as partners_telegram_daily:

    @task_group(group_id="data_extraction")
    def data_extraction():
        extraction = DataExtraction()

        @task
        def ch_query(sql_file, ds=None):
            return extraction.ch_query(sql_file, ds)

        @task
        def lib():
            return extraction.lib()

        data = {}
        for name, file in SQL_FILES.items():
            data[name] = ch_query.override(task_id=name)(sql_file=file)

        data["lib"] = lib.override(task_id="lib")()
        return data

    @task_group(group_id="data_transform")
    def data_transform(data):
        transform = DataTransform()

        @task
        def report(data):
            return transform.report(**data)

        @task
        def messages(data):
            return transform.messages(data)

        data = report.override(task_id="report")(data=data)
        company_messages = messages.override(task_id="messages")(data=data)
        return company_messages

    @task_group(group_id="data_load")
    def data_load(company_massages, lib):
        load = DataLoad()

        @task
        def send_message(company_massages, lib):
            df_lib = pd.DataFrame(json.loads(lib))
            company_chat_id = dict(df_lib[["CompanyName", CHAT_ID]].values)
            for company, chat_id in company_chat_id.items():
                print(company, chat_id)

                if company not in company_massages:
                    print(company, "Данные отсутствуют в DWH")
                    continue

                messages = company_massages[company]
                load.send_message(chat_id, messages)

        send_message.override(task_id="send_message")(company_massages=company_massages, lib=lib)


    # ETL graph
    extracted = data_extraction()
    transformed = data_transform(extracted)
    data_load(transformed, extracted["lib"])