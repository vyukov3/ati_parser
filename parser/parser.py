import time

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.keys import Keys

from parser.utils import add_files, geocod_ati_data, open_file, save_res, parse_wiki  # noqa

LOGIN_URL = "https://id.ati.su/login/?next=https%3A%2F%2Floads.ati.su"
LOGIN = "vyukov3"
PASSWORD = "33hV5Di6nGeHEzT"
START = 0


def login(driver):
    driver.get(LOGIN_URL)

    login_input = driver.find_element("id", "login")
    password_input = driver.find_element("id", "password")
    apply_button = driver.find_element("id", "action-login")

    login_input.send_keys(LOGIN)
    password_input.send_keys(PASSWORD)
    apply_button.click()
    driver.implicitly_wait(3)
    driver.find_element("css selector", ".sessions-limit-continue-button").click()


# deprecated
def set_orig_and_dest(driver, directions: str):
    dirs = [dir.split("-")[-1].split(" ")[-1] for dir in directions.split("_")]
    if dirs[0] == "Петербург":
        dirs[0] = "Санкт-Петербург"
    if dirs[1] == "Петербург":
        dirs[1] = "Санкт-Петербург"
    from_input = driver.find_element("id", "from")
    to_input = driver.find_element("id", "to")

    from_input.clear()
    from_input.send_keys(dirs[0])
    driver.find_elements("css selector", "ul.dropdown-menu.ng-isolate-scope li")[0].click()

    to_input.clear()
    to_input.send_keys(dirs[1])
    driver.find_elements("css selector", "ul.dropdown-menu.ng-isolate-scope li")[0].click()

    to_input.send_keys(Keys.ENTER)


# deprecated
def process_line(item):
    distance = item.find_element("css selector", ".qvPbM").text
    transport = item.find_element("css selector", "._3qUC2 span").text

    dates = item.find_elements("css selector", "._35iFG")
    loading_date = "" if len(dates) == 0 else " ".join([j.text for j in dates[0].find_elements("css selector", "span")])
    unloading_date = (
        "" if len(dates) < 2 else " ".join([j.text for j in dates[1].find_elements("css selector", "span")])
    )

    costs = ""
    cost_lines = item.find_elements("css selector", ".bgYB4 ._3X-ib")
    for it in cost_lines:
        parts = it.find_elements("css selector", "span")
        if len(parts) > 3:
            costs += parts[1].text + ";" + parts[3].text
        else:
            costs += " ".join([i.text for i in parts])
        break

    places = item.find_elements("css selector", "._1viCR")
    places_strs = []
    for place in places:
        places_strs.append(", ".join([el.text for el in place.find_elements("css selector", "div")]))

    mesurments = item.find_element("css selector", "._2gLPf ._3lW-N").text.split("/")
    return (distance, transport, loading_date, unloading_date, costs, *places_strs, *mesurments)


def process_line_fast(soupe):
    distance_item = soupe.select_one(".qvPbM a")
    distance = distance_item.text if distance_item is not None else ""

    transport_item = soupe.select_one("._3qUC2 span")
    transport = transport_item.text if transport_item is not None else ""

    dates = soupe.select("._35iFG")
    loading_date = "" if len(dates) == 0 else " ".join([j.text for j in dates[0].select("span")])
    unloading_date = "" if len(dates) < 2 else " ".join([j.text for j in dates[1].select("span")])

    costs = ""
    cost_lines = soupe.select(".bgYB4 ._3X-ib")
    for it in cost_lines:
        parts = it.select("span")
        if len(parts) > 3:
            costs += parts[1].text.replace("\xa0", " ") + ";" + parts[3].text
        else:
            costs += " ".join([i.text for i in parts])
        break

    places = soupe.select("._1viCR")
    places_strs = []
    for place in places:
        places_strs.append(", ".join([el.text for el in place.select("div")]))

    mesurments_item = soupe.select_one("._2gLPf ._3lW-N")
    if mesurments_item is not None:
        mesurments = soupe.select_one("._2gLPf ._3lW-N").text.replace(" ", "").split("/")
    else:
        mesurments = ["", ""]
    return [distance, transport, loading_date, unloading_date, costs, *places_strs, *mesurments]


def set_dest(driver):
    to_input = driver.find_element("id", "to")
    to_input.send_keys("Россия")


def set_origin_district(driver, district: str):
    from_input = driver.find_element("id", "from")

    from_input.clear()
    from_input.send_keys(district)
    driver.find_elements("css selector", "ul.dropdown-menu.ng-isolate-scope li")[0].click()
    from_input.send_keys(Keys.ENTER)
    time.sleep(1)


def start():
    driver = webdriver.Chrome()
    directions = open_file("./parser/data/Субъекты.csv", ";")
    res = []

    login(driver)
    set_dest(driver)

    i = 0
    for _, row in directions.iterrows():
        try:
            i += 1
            print(f"Line {i}/{directions.shape[0]}. Direction: {row['Субъект']}")
            set_origin_district(driver, row["Субъект"])
            if len(driver.find_elements("css selector", ".search-not-found")) != 0:
                continue
            pagination_input = driver.find_elements("css selector", ".pagination-page input")[0]
            pages_str = driver.find_elements("css selector", ".pagination-last-page")[-1].text
            pages = 1 if pages_str == "" else int(pages_str)
            for page in range(pages):
                if pages > 1:
                    pagination_input.clear()
                    pagination_input.send_keys(str(page + 1))
                    pagination_input.send_keys(Keys.ENTER)
                    time.sleep(1)

                soupe = BeautifulSoup(driver.page_source, features="html.parser")
                items = soupe.select("#pretty-loads-holder div[data-app='pretty-load']")
                for item in items:
                    line = process_line_fast(item)
                    res.append(line)

        except Exception as e:
            print(f"Беда: {e}")
        finally:
            save_res(row["Субъект"], res)
            res.clear()

    driver.close()
