import os

from bs4 import BeautifulSoup
import pandas as pd
import requests

from parser.yandex import YandexGeocoder

YANDEX_KEY = "06523bd7-53cd-4b56-b34c-6daf2bbc2c24"
YANDEX_KEY_RESERVE = "71139686-5946-4ce6-9bbb-568d605f2b08"

columns = [
    "Расстояние",
    "Кузов",
    "Время загрузки",
    "Время выгрузки",
    "Цена",
    "Откуда",
    "Куда",
    "Масса",
    "Объем",
]


result_columns = [
    "id",
    "Город откуда",
    "Регион откуда",
    "Откуда",
    "Время загрузки",
    "Город куда",
    "Куда",
    "Время выгрузки",
    "Расстояние",
    "Стоимость",
    "Тариф",
    "Масса",
    "Объем",
    "Кузов",
]


def open_file(path: str, sep: str) -> pd.DataFrame:
    return pd.read_csv(path, sep=sep)


def save_res(name: str, res):
    df = pd.DataFrame(res, columns=columns)
    df.to_excel(f"./parser/data/results/{name}.xlsx")


def parse_wiki():
    res = []
    url = "https://ru.wikipedia.org/wiki/%D0%A1%D1%83%D0%B1%D1%8A%D0%B5%D0%BA%D1%82%D1%8B_%D0%A0%D0%BE%D1%81%D1%81%D0%B8%D0%B9%D1%81%D0%BA%D0%BE%D0%B9_%D0%A4%D0%B5%D0%B4%D0%B5%D1%80%D0%B0%D1%86%D0%B8%D0%B8"  # noqa
    resp = requests.get(url)
    soupe = BeautifulSoup(resp.text, features="html.parser")
    rows = soupe.select("table.standard tr")
    for row in rows:
        if row.get("class") is not None:
            continue
        tds = row.select("td")
        if len(tds) == 0:
            continue
        res.append((tds[1].text, tds[6].text))

    columns = ["Субъект", "Центр"]
    df = pd.DataFrame(res, columns=columns)
    df.to_csv("./parser/data/Субъекты_raw.csv", sep=";")


def add_files():
    res = []
    i = 0
    for filename in os.listdir("./parser/data/results/"):
        df = pd.read_excel("./parser/data/results/" + filename, engine="openpyxl")
        print(filename)
        for _, row in df.iterrows():
            i += 1
            dist = row["Расстояние"].split(" ")[0] if str(row["Расстояние"]) != "nan" else ""
            costs = str(row["Цена"]).split(";")
            if len(costs) > 1:
                costs[0] = costs[0][:-1].replace(" ", "")
                costs[1] = costs[1][:-4]
            else:
                costs = ["", ""]
            city_from = row["Откуда"].split(",")[0]
            city_to = row["Куда"].split(",")[0]
            res.append(
                [
                    f"ati-{i}",
                    city_from,
                    filename[:-5],
                    row["Откуда"],
                    row["Время загрузки"],
                    city_to,
                    row["Куда"],
                    row["Время выгрузки"],
                    dist,
                    costs[0],
                    costs[1],
                    row["Масса"],
                    row["Объем"],
                    row["Кузов"],
                ]
            )
    res_df = pd.DataFrame(res, columns=result_columns)
    res_df.to_excel("./parser/data/all_results.xlsx")


def geocod_ati_data():
    df = pd.read_excel("./parser/data/all_results.xlsx", engine="openpyxl")
    addresses = df["Откуда"].tolist()
    addresses += df["Куда"].tolist()
    geocoder = YandexGeocoder(api_key=YANDEX_KEY, api_key_reserve=YANDEX_KEY_RESERVE)
    res = geocoder.batch_geocode(addresses)
    from_lat, from_lon, to_lat, to_lon = [], [], [], []
    for _, row in df.iterrows():
        from_coords = res[row["Откуда"]] if row["Откуда"] in res else {"lat": "", "lon": ""}
        to_coords = res[row["Куда"]] if row["Куда"] in res else {"lat": "", "lon": ""}
        from_lat.append(from_coords["lat"])
        from_lon.append(from_coords["lon"])
        to_lat.append(to_coords["lat"])
        to_lon.append(to_coords["lon"])
    df["Откуда широта"] = from_lat
    df["Откуда долгота"] = from_lon
    df["Куда широта"] = to_lat
    df["Куда долгота"] = to_lon
    df.to_excel("./parser/data/all_results_geo.xlsx")
