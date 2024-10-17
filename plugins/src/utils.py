from os import listdir, mkdir
from pandas import read_csv, DataFrame
from datetime import date, timedelta
from requests import post

import smtplib, json
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText


def open_file(path, mode="r", encoding=None, text=None):
    with open(path, mode=mode, encoding=encoding) as file:
        if mode == "r" or mode == "rb":
            return file.read()
        elif mode == "w":
            file.write(text)


def read_json(path, encoding=None):
    return json.loads(open_file(path, mode="r", encoding=encoding))


def create_dir(path: str) -> bool:
    """Create dir
    
    params: 
        path: str - path to dir
    return: bool"""
    
    try:
        mkdir(path)
        return True
    except FileExistsError as e:
        print(e)
        return False


def create_path(path):

    files = path.split('/') # разбиваем путь на файлы
    if '.' in files[-1]: # если последний файл имеет расширение
        files = files[:-1] # тогда исключаем этот файл из пути

    path = files[0] # берем 1й файл
    for d in files[1:]: # перебираем последующие в пути файлы
        path += f"/{d}" # к предыдущему файлу в пути прибавляем следующий
        create_dir(path) # достраиваем путь

    return path


def this_month():
    date2 = date.today()
    date1 = date(year=date2.year, month=date2.month, day=1)
    return date1, date2


def last_month():
    today = date.today()
    date2 = today - timedelta(days=today.day)
    date1 = date2 - timedelta(days=date2.day - 1)
    return date1, date2


def half_month():
    today = date.today()
    date1 = date(year=today.year, month=today.month, day=1)
    date2 = date(year=today.year, month=today.month, day=15)
    return date1, date2


def load_manager(path: str=None, func=None, low_memory:bool=True, memory_map:bool=False, parse_dates:list=False):
    """
    Need to ones preprocess data and save ones, then automaticaly upload precessed data
    params:
        path: str - path to save or upload data
        func: - lambda function to processing data

        https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html
        low_memory: bool - internally process the file in chunks, if memory not enough to upload
        memory_map: bool - can help to upload massive data
        parse_datas: list - parse input columns to datetime type
    """

    # 1. if path not input, then use lambda function ewerytime
    if not path:
        return func()
    
    # 2. if path input, and file exist then upload file
    parts = path.split('/')
    if parts[-1] in listdir("/".join(parts[:-1])):
        return read_csv(path, low_memory=low_memory, memory_map=memory_map, parse_dates=parse_dates)
    
    # 3. if path input, but file not exist, then run lambda function, and save .csv result
    data = func()
    try:
        if data.shape[0]:
            data.to_csv(path, index=False)
    except TypeError as err:
        open_file(path, 'w', text=data)
        print(err)

    return data


class TeleBot:

    def __init__(self, token, chat_id):

        self.__TOKEN = token
        self.__CHAT_ID = chat_id

    def send_documents(self, files_dir):
        """push data in telegram iteratively
        
        Params:
            :files_dir - direction with files to upload in telegram chat
        Return: True if all is done"""

        URL = f'https://api.telegram.org/bot{self.__TOKEN}/sendDocument?chat_id={self.__CHAT_ID}'

        for file in listdir(files_dir):
            if '.zip' in file:
                path = f'{files_dir}/{file}'
                file = {'document': open(path, 'rb')}
                post(URL, files=file, verify=False)

        return URL

    def send_message(self, text):

        URL = f'https://api.telegram.org/bot{self.__TOKEN}/sendMessage?chat_id={self.__CHAT_ID}&text={text}'

        post(URL, verify=False)

        return URL


def parse_url(url):

    root, params = url.split("?")
    params = dict([p.split("=", 1) for p in params.split("&")])
    
    return root, params


def reduce_memory(df: DataFrame):
    
    print("memory before:", round(df.memory_usage().sum() / (2**20), 4), "GB")

    for col in df.columns:
        
        if "int" in str(df[col].dtype):
            
            if not (df[col] != df[col].astype(f"int8")).sum():
                df[col] = df[col].astype(f"int8")
            elif not (df[col] != df[col].astype(f"int16")).sum():
                df[col] = df[col].astype(f"int16")
            elif not (df[col] != df[col].astype(f"int32")).sum():
                df[col] = df[col].astype(f"int32")
            elif not (df[col] != df[col].astype(f"int64")).sum():
                df[col] = df[col].astype(f"int64")
        
        elif "float" in str(df[col].dtype):
            
            if not (df[col] != df[col].astype(f"float16")).sum():
                df[col] = df[col].astype(f"int16")
            elif not (df[col] != df[col].astype(f"float32")).sum():
                df[col] = df[col].astype(f"int32")
            elif not (df[col] != df[col].astype(f"float64")).sum():
                df[col] = df[col].astype(f"int64")
                
    print("memory after:", round(df.memory_usage().sum() / (2**20), 1), "GB")
    
    return df


class Temp:
    
    def __init__(self, temp):
        """
        Assemble query

        params:
            :temp: str = query template 
        """
        self.temp = temp

    def __repr__(self):
        return self.temp

    def replace(self, repl: dict):
        """
        Assemble query for DWH

        params:
            :repl: dict = {pattern_to_replace: new_value}
        return: str
        """
        temp = self.temp
        for tag, val in repl.items():
            temp = temp.replace(tag, str(val))
        return temp
    
    def add(self, lines: list[str], sep="\n"):
        """
        Add lines in end of text
        
        params:
           :lines: list = [string1, string2, ...]
        return: str
        """
        for l in lines:
            if l not in self.temp:
                self.temp += sep + l
        return self.temp


class Email:
    
    def __init__(self, host, port, user, password):
        
        self.__HOST = host
        self.__PORT = port
        self.__USER = user
        self.__PASSWORD = password
        
        self.__msg = MIMEMultipart()
        
    def __repr__(self):
        return f"HOST={self.__HOST}, PORT={self.__PORT}, USER={self.__USER}"
        
    def add_document(self, path):
        
        part = open_file(path, "rb")
        part = MIMEApplication(part)
        part['Content-Disposition'] = f"attachment; filename=\"{path.split('/')[-1]}\""

        self.__msg.attach(part)
        
        return self.__msg
        
    def add_message(self, text):
        
        self.__msg.attach(MIMEText(text))
        
        return self.__msg
        
    def push(self, from_address:str, to_address:list, title=None):
        
        with smtplib.SMTP(self.__HOST, self.__PORT) as server:
            server.starttls()
            server.login(self.__USER, self.__PASSWORD)
            
            self.__msg["From"] = from_address
            self.__msg["To"] = ", ".join(to_address)
            self.__msg["Subject"] = title

            server.send_message(self.__msg)

            del self.__msg
            
            return True
