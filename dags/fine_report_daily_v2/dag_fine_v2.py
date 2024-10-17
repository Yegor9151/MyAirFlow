"""
ДОРАБОТАТЬ:
- добавлять недастающие даты, а не переписывать всю таблицу (push_result)
"""
# Здесь импортируем только те библиотеки, что необходимы для создания дага
# Внутри дага импортируем остальные
from airflow.models import DAG
from datetime import datetime


with DAG(
    "fine_report_daily_v2",
    "Ежедневно пушит отчет по штрафам за предыдущий день на гугл диск",
    schedule_interval="0 5 * * *", # Запускается каждый день в 8:00 UTC+3 
    default_args={
        "owner": "Sorokin Yegor",
        "email": "e.sorokin@urent.ru",
        "email_on_failure": True,
        "email_on_retry": True,
        "start_date": datetime(2024, 10, 4), # Если нужно более ранние даты добавить, меняем дату старта
    },
    tags=["google", "mongodb", "cLickhouse"]
) as fine_report_daily_v2:


    # Остальные библиотеки
    from airflow.models import Variable
    from airflow.settings import AIRFLOW_HOME
    from airflow.decorators import task

    from src import utils

    import clickhouse_connect, pymongo, json, gspread
    import pandas as pd
    from datetime import datetime, timedelta


    #### SQL ЗАПРОС
    SQL_FINE = AIRFLOW_HOME + "/dags/fine_report_daily_v2/CH_fine_v2.sql"
    #### Таблица с результатами https://docs.google.com/spreadsheets/d/1clPnDRubX5Fs0JtflagAdU1wxImxw_hQGoECyGcoPz0/edit?gid=0#gid=0
    GOOGLE_TABLE = {
        "key": "1clPnDRubX5Fs0JtflagAdU1wxImxw_hQGoECyGcoPz0",
        "title": "fine_v2"
    }


    #### ЗАДАЧИ
    @task(task_id="get_creds")
    def get_creds(ds=None, **kwargs) -> dict:
        """Доступы к сервисам"""

        creds = {
            "click_house": {
                "host": Variable.get('CLICKHOUSE_HOST'),
                "port": Variable.get('CLICKHOUSE_PORT'),
                "user": Variable.get('CLICKHOUSE_USER'),
                "password": Variable.get('CLICKHOUSE_PASSWORD'),
                "send_receive_timeout": 1800
            },
            "mongodb": {
                "host": Variable.get('MONGODB_HOST'),
                "port": int(Variable.get('MONGODB_PAYMENT_PORT')),
                "username": Variable.get('MONGODB_USER'),
                "password": Variable.get('MONGODB_PASSWORD'),
                "authsource": "admin",
                "readpreference": "secondary",
                "directconnection": True
            },
            "google": Variable.get("GOOGLE_DRIVE_TOKEN", deserialize_json=True)
        }
        return creds


    @task(task_id="collect_fines")
    def collect_fines(ch_cred, sql_file, ds=None, **kwargs):
        """собираем штрафы из CH"""
        
        client = clickhouse_connect.get_client(**ch_cred)
        ds = pd.to_datetime(ds).date()
        query_temp = utils.Temp(utils.open_file(sql_file)) # берем sql запрос как шаблон
        
        print(ds, ds + timedelta(days=1))

        # assemble query
        query = query_temp.replace({ # вставляем даты
            "<DS>": ds,
            "<DE>": ds + timedelta(days=1)
        })
        print(query)

        # collect fine
        df_fines = client.query_df(query)

        assert df_fines.shape[0] > 0, "Данные не собрались - проверьте их наличие и SQL запрос"

        print(df_fines.info())

        return df_fines.to_json()


    @task(task_id="collect_vendors")
    def collect_vendors(mdb_cred, ds=None, **kwargs):
        """добавляем INN к штрафам"""

        mdb_client = pymongo.MongoClient(**mdb_cred)

        # collect INN
        df_vendors = pd.DataFrame(mdb_client.payment_api.vendors.find())

        assert df_vendors.shape[0] > 0, "Данные не собрались - проверьте их наличие в payment_api.vendors"

        df_vendors["INN"] = pd.json_normalize(df_vendors["ReceiptSystem"])["Inn"]
        df_vendors["_id"] = df_vendors["_id"].astype("str")
        df_vendors = df_vendors[["_id", "INN"]].rename(columns={"_id": "VendorId"})

        print(df_vendors.info())

        return df_vendors.to_json()


    @task(task_id="assemple_result")
    def assemple_result(data_fines, data_vendors, ds=None, **kwargs):

        # assemble result
        df_fines = pd.DataFrame(json.loads(data_fines))
        print(df_fines.info())
        df_vendors = pd.DataFrame(json.loads(data_vendors))
        print(df_vendors.info())

        df_result = df_fines.merge(df_vendors, on="VendorId").drop(["VendorId"], axis=1)
        df_result = df_result[[
            "Date", "Country", "City", "Vendor", "Company", "INN", "Currency",
            "Comment", "Transactions", "Users", "Fine", "GMV", "Compensation"
        ]]
        print(df_result.info())

        return df_result.to_json()


    @task(task_id="push_result")
    def push_result(google_cred, key, title, data_new, ds=None, **kwargs):
        """пушим результат в гугл таблицу:
        https://docs.google.com/spreadsheets/d/1clPnDRubX5Fs0JtflagAdU1wxImxw_hQGoECyGcoPz0/edit?gid=0#gid=0
        """

        google_client = gspread.service_account_from_dict(google_cred)
        worksheet = google_client.open_by_key(key).worksheet(title)
        table = worksheet.get_values()

        df_table = pd.DataFrame(columns=table[0], data=table[1:])
        for col in df_table.columns[-5:]:
            df_table[col] = df_table[col] \
                .str.replace('\xa0', '') \
                .str.replace(',', '.') \
                .astype("float64")
        print(df_table.info())

        df_result = pd.DataFrame(json.loads(data_new)).fillna(0)
        print(df_result.info())

        # # Проверки
        assert df_table.shape[1] == df_result.shape[1], "нужно пересобрать данные"

        if (~df_table.columns.isin(df_result.columns)).sum(): # все ли колонки совпадают
            df_table.columns = df_result.columns # если нет переписываем колонки

        if (df_result["Date"].isin(df_table["Date"])).sum(): # есть ли новая дата в таблице
            return f"{ds} - дата уже есть" # если да, останавливаем задачу

        # Если проверки пройдены
        df_result = pd.concat([df_table, df_result]).sort_values(["Date"])

        worksheet.clear()
        worksheet.append_row(list(df_result.columns))
        worksheet.append_rows(list(map(list, df_result.values)))

        return df_result.info()


    #### ВЫЗОВ ЗАДАЧ
    # подключения к сервисам
    creds = get_creds()
    # собираем штрафы из CH
    data_fines = collect_fines(
        creds["click_house"],
        SQL_FINE
    )
    # собираем вендоры
    data_vendors = collect_vendors(
        creds["mongodb"]
    )
    # собираем результат
    data_result = assemple_result(
        data_fines,
        data_vendors
    )
    # пушим результат в гугл таблицу
    push_result = push_result(
        creds["google"],
        GOOGLE_TABLE["key"],
        GOOGLE_TABLE["title"],
        data_result
    )

    #### ГРАФ ВЫПОЛНЕНИЯ
    creds >> data_fines >> data_result
    creds >> data_vendors >> data_result
    data_result >> push_result
