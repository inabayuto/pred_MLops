# ライブラリのインポート
from urllib.request import urlopen
import pandas as pd
from tqdm.notebook import tqdm
import os
from bs4 import BeautifulSoup
import re
import time

# スクレイピング
# getHTMLrace（）
def getHTMLRace(race_id_list: list, skip: bool = True):
    """
    netkeiba.comのraceディレクトリのhtmlをスクレイピングしてdata/html/raceに保存する関数
    """
    html_path_list =  []
    for race_id in tqdm(race_id_list):
        url =  "https://db.netkeiba.com/race/" + race_id #race_idからurlを作る 
        html =  urlopen(url).read() #スクレイピングを実行
        filename =  "data/html/race/" + race_id + ".bin"
        html_path_list.append(filename)
        if skip and os.path.isfile(filename): #skipがTrueで、かつbinファイルが既に存在する場合は飛ばす
            print("race_id {} skipped".format(race_id))
            continue
        html =  urlopen(url).read()
        with open(filename, "wb") as f:
            f.write(html) #保存
        time.sleep(1) #サーバーに負担をかけないように1秒待機する
    return html_path_list


# getRowDataResults()
def getRowDataResults(html_path_list: list):  
    
    """
    raceページのhtmlを受け取って、レース結果テーブルに変換する関数
    """
    race_results = {}
    for html_path in tqdm(html_path_list):
        with open(html_path, "rb") as f:
            html = f.read() #保存してあるbinファイルを読み込む
            #メインとなるテーブルデータを取得
            df = pd.read_html(html)[0] #メインとなるレース結果テーブルデータを取得
            soup = BeautifulSoup(html, "html.parser") #htmlをsuopオブジェクトに変換

        
            #馬ID、騎手IDをスクレイピング
            horse_id_list = []
            horse_a_list = soup.find("table", attrs={"summary": "レース結果"}).find_all(
                "a", attrs={"href": re.compile("^/horse")}
            )
            for a in horse_a_list:
                horse_id = re.findall(r"\d+", a["href"])
                horse_id_list.append(horse_id[0])
            jockey_id_list = []
            jockey_a_list = soup.find("table", attrs={"summary": "レース結果"}).find_all(
                "a", attrs={"href": re.compile("^/jockey")}
            )
            for a in jockey_a_list:
                jockey_id = re.findall(r"\d+", a["href"])
                jockey_id_list.append(jockey_id[0])
            df["horse_id"] = horse_id_list
            df["jockey_id"] = jockey_id_list

            #インデックスをrace_idにする
            race_id = re.findall("\d+", html_path)[0]
            df.index = [race_id] * len(df)

            race_results[race_id] = df
            
    #pd.DataFrame型にして一つのデータにまとめる
    race_results_df = pd.concat([race_results[key] for key in race_results])

    return race_results_df


# getRawDataInfo
def getRowDataInfo(html_path_list: list):
    
    """
    raceページのhtmlを受け取って、レース情報テーブルに変換する関数
    """
    race_infos = {}
    for html_path in tqdm(html_path_list):
        with open(html_path, "rb") as f:
            html = f.read() #保存してあるbinファイルを読み込む
            #メインとなるテーブルデータを取得
            soup = BeautifulSoup(html, "html.parser") #htmlをsuopオブジェクトに変換
            
             #天候、レースの種類、コースの長さ、馬場の状態、日付をスクレイピング
            texts = (
                soup.find("div", attrs={"class": "data_intro"}).find_all("p")[0].text
                + soup.find("div", attrs={"class": "data_intro"}).find_all("p")[1].text
            )
            info = re.findall(r'\w+', texts)
            df = pd.DataFrame()
            for text in info:
                if text in ["芝", "ダート"]:
                    df["race_type"] = [text]
                if "障" in text:
                    df["race_type"] = ["障害"]
                if "m" in text:
                    df["course_len"] = [int(re.findall(r"\d+", text)[0])]
                if text in ["良", "稍重", "重", "不良"]:
                    df["ground_state"] = [text]
                if text in ["曇", "晴", "雨", "小雨", "小雪", "雪"]:
                    df["weather"] = [text]
                if "年" in text:
                    df["date"] = [text]
                    
            #インデックスをrace_idにする
            race_id = re.findall("\d+", html_path)[0]
            df.index = [race_id] * len(df)

            race_infos[race_id] = df
            
    #pd.DataFrame型にして一つのデータにまとめる
    race_infos_df = pd.concat([race_infos[key] for key in race_infos])

    return race_infos_df

# getRawDataReturn
def getRowDataReturn(html_path_list: list):
    
    """
    raceページのhtmlを受け取って,払い戻しテーブルに変換する関数
    """
    return_tables = {}
    for html_path in tqdm(html_path_list):
        with open(html_path, "rb") as f:
            html = f.read() #保存してあるbinファイルを読み込む
            
            html = html.replace(b'<br /n>', b'br')
            dfs = pd.read_html(html)
            
            #dfsの一番目に単勝〜馬番、2番目にワイドの~三連単がある
            df = pd.concat([dfs[1], dfs[2]])
            
            race_id = re.findall('\d+', html_path)[0]

            df.index = [race_id] * len(df)
            return_tables[race_id] = df
    
     #pd.DataFrame型にして一つのデータにまとめる
    return_tables_df = pd.concat([return_tables[key] for key in return_tables])
    return return_tables_df

# getHTMLrace（）
def getHTMLHorse(horse_id_list: list, skip: bool = True):
    """
    netkeiba.comのhorseページのhtmlをスクレイピングしてdata/html/horseに保存する関数
    """
    html_path_list =  []
    for horse_id in tqdm(horse_id_list):
        url =  "https://db.netkeiba.com/horse/" + horse_id #horse_idからurlを作る 
        html =  urlopen(url).read() #スクレイピングを実行
        filename =  "data/html/horse/" + horse_id + ".bin"
        html_path_list.append(filename)
        if skip and os.path.isfile(filename): #skipがTrueで、かつbinファイルが既に存在する場合は飛ばす
            print("horse_id {} skipped".format(horse_id))
            continue
        with open(filename, "wb") as f:
            f.write(html) #保存
        time.sleep(1) #サーバーに負担をかけないように1秒待機する
    return html_path_list

def getRawDataHorseResults(html_path_list: list):
    """
    horseページのhtmlを受け取って、馬の過去成績DataFrameに変換する関数
    """
    horse_results = {}
    for html_path in tqdm(html_path_list):
        with open(html_path,  'rb') as f:
            html =  f.read()
            
            df =  pd.read_html(html)[3]
            #受賞歴がある場合、3番目に受賞歴テーブルがくるために、4番目のdデータがを取得する
            if df.columns[0] == "受賞歴":
                df = pd.read_html(html)[4]
                
            # 正規表現「肯定後読み」
            # あるパターンの後にある文字列を抜き出す
            horse_id = re.findall('(?<=horse/)\d+', html_path)[0]
                
                
            df.index = [horse_id] * len(df)
            horse_results[horse_id] =  df
           
    #pd.DataFrame型にして一つのデータにまとめる
    horse_results_df = pd.concat([horse_results[key] for key in horse_results])
    return horse_results_df

def getHTMLPed(horse_id_list: list, skip: bool = True):
    """
    netkeiba.comのhorse/pedページのhtmlをスクレイピングしてdata/html/raceに保存する関数
    """
    html_path_list =  []
    for horse_id in tqdm(horse_id_list):
        url =  "https://db.netkeiba.com/horse/ped/" + horse_id #race_idからurlを作る 
        html =  urlopen(url).read() #スクレイピングを実行
        filename =  "data/html/ped/" + horse_id + ".bin"
        html_path_list.append(filename)
        if skip and os.path.isfile(filename): #skipがTrueで、かつbinファイルが既に存在する場合は飛ばす
            print("horse_id {} skipped".format(horse_id))
            continue
        with open(filename, "wb") as f:
            f.write(html) #保存
        time.sleep(1) #サーバーに負担をかけないように1秒待機する
    return html_path_list


def getRawDataPeds(html_path_list: list):
    """
    horseページのhtmlを受け取って、馬の過去成績DataFrameに変換する関数
    """
    peds = {}
    for html_path in tqdm(html_path_list):
        with open(html_path,  'rb') as f:
            html =  f.read()
            
            df =  pd.read_html(html)[0]
            
            #重複を削除して1列のSeries型データに直す
            generations = {}
            
            # 正規表現「肯定後読み」
            # あるパターンの後にある文字列を抜き出す
            horse_id = re.findall('(?<=ped/)\d+', html_path)[0]
            
            for i in reversed(range(5)):
                generations[i] = df[i]
                df.drop([i], axis=1, inplace=True)
                df = df.drop_duplicates()
            ped = pd.concat([generations[i] for i in range(5)]).rename(horse_id)

            peds[horse_id] = ped.reset_index(drop=True)
           
    #pd.DataFrame型にして一つのデータにまとめる
    #列名をpeds_0, ..., peds_61にする
    peds_df = pd.concat([peds[key] for key in peds],
                        axis=1).T.add_prefix('preds_')

    return peds_df