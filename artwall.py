import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
from typing import List
from joblib import Parallel, delayed

WAY_RUN_EXCEL: str = r"C:\Users\Public\Documents\artwall\catalog.xlsx"
WAY_LOG_EXCEL: str = r"C:\Users\Public\Documents\artwall\artwall.xlsx"
WAY_PROJECT: str = r'C:\Users\Public\Documents\artwall'


def list_catalog() -> List:
    excel = pd.read_excel(r"C:\Users\Public\Documents\artwall\catalog.xlsx")
    excel['names'] = excel['names'].mask(excel['names'].isna(), excel['adress'].str.split('/').str[-1])
    return excel.values.tolist()


def load_jpg_with_artwall_ru(url_catalog, dir_name):
    list_log: List[tuple] = []
    list_url_imges = []
    way_path = os.path.join(WAY_PROJECT, 'images', dir_name)
    # Создание папки с именем каталога
    if not os.path.exists(way_path):
        os.makedirs(way_path)

    # По всем доступным страницам каталога, получаем список товаров
    count_page = 1
    while True:
        url = f'{url_catalog}/page_{count_page}'
        response = requests.get(url)
        print(url)

        # Если старица не доступна, выходим
        if response.status_code != 200:
            print('Страница не найдена:', url, " продолжаем")
            break

        # Получаем список товаров
        soup_catalog = BeautifulSoup(response.text, "html.parser")
        try:
            if soup_catalog.find(class_='lost-page'):
                print('Страница не найдена:', url, " продолжаем")
                break
        except Exception as e:
            pass

        catalog_list = soup_catalog.findAll(class_='name')
        for i in catalog_list:
            href_url_img = i.find('a').get('href')
            list_url_imges.append(href_url_img)
        count_page += 1

    count_img = 1
    for i in list_url_imges:
        try:
            url_sale = "https://www.artwall.ru" + i

            response = requests.get(url_sale)
            if response.status_code != 200:
                print("Страница не найдена: ", url_sale)
                break
            soup_sale = BeautifulSoup(response.text, "html.parser")
            url_img = url_sale + '/image'
            name_sale = soup_sale.find(class_='catalog-page').find(class_='active').text
            widht = soup_sale.find(id='poster_width').get('value')
            height = soup_sale.find(id='poster_height').get('value')
            way_img = os.path.join(way_path, f'{dir_name}_{count_img}.jpg')


            response_img = requests.get(url_img)
            if response_img.status_code == 200:
                with open(way_img, 'wb') as f:
                    f.write(response_img.content)
                count_img += 1
            list_log.append((way_img, name_sale, url_sale, url_img, widht, height))

        except Exception as e:
            print("Ошибка, запистали пустой лог")
            list_log.append(('way_img', 'name_sale', 'url_sale', 'url_img', 'widht', 'height'))

    way_log = os.path.join(WAY_PROJECT, "logs", f'{dir_name}.xlsx')
    excel_log = pd.DataFrame(list_log, columns=['way_img', 'name_sale', 'url', 'url_img', 'widht', 'height'])
    excel_log.to_excel(way_log, index=False)
    print("Все идет хорошо, спарсили:", url_catalog)


def create_log():
    way_logs = os.path.join(WAY_PROJECT, 'logs')
    dir_log = os.listdir(way_logs)
    excel_log = pd.DataFrame(columns=['way_img', 'name_sale', 'url', 'url_img', 'widht', 'height'])
    for files in dir_log:
        excel_log = pd.concat([excel_log, pd.read_excel(os.path.join(way_logs, files))])
    excel_log.to_excel(os.path.join(WAY_PROJECT, "log.xlsx"), index=False)


if __name__ == '__main__':
    print("Начинаем")
    Parallel(n_jobs=4)(delayed(load_jpg_with_artwall_ru)(url_catalog, dir_name) for url_catalog, dir_name in list_catalog())
    create_log()
