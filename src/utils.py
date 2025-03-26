import requests
import psycopg2
from src.class_DBManager import DBManager
from src.config import config
import os

def get_vacancies_list():
    """Получение списка словарей о вакансиях и компаниях"""
    url = 'https://api.hh.ru/employers'
    headers = {'User-Agent': 'HH-User-Agent'}
    items_list = []
    print(f"Загружаю данные с сайта hh.ru...")
    employers_list = (9694561, 4219, 5919632, 5667343, 9301808, 10571093, 198614, 6062708, 78638, 1107346) #Избранные 10 компаний
    url_list = []
    for employer_id in employers_list:
        full_url = url + '/' + str(employer_id)
        url_list.append(full_url)
    for i in range(len(employers_list)):
        items = requests.get(url_list[i], headers=headers).json()
        items_list.append(items)
    vacancies = []
    for data in items_list:
        resp = requests.get(data['vacancies_url'], headers={'User-Agent': 'HH-User-Agent'}).json()['items']
        vacancies.extend(resp)
    final_list = []
    for vac in vacancies:
        vacancy = {'name': vac['name'], 'company': vac['employer']['name'], 'url': vac['alternate_url']}
        if vac.get('salary'):
            vacancy['from'] = vac.get('salary').get('from')
            vacancy['to'] = vac.get('salary').get('to')
            vacancy['currency'] = vac.get('salary').get('currency')
        else:
            vacancy['from'] = None
            vacancy['to'] = None
            vacancy['currency'] = None
        final_list.append(vacancy)

    return final_list

def upload_to_database(data_list: list, params):
    """Функция загружающая полученные данные в базу даных postgres в таблицу vacancies"""
    print(f"Заношу данные в таблицу...")
    conn = psycopg2.connect(**params)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS vacancies")
                cur.execute("CREATE TABLE IF NOT EXISTS vacancies"
                            "("
                            "id serial PRIMARY KEY, "
                            "name varchar(255) NOT NULL, "
                            "salary_from int, "
                            "salary_to int,"
                            "currency varchar(10), "
                            "company varchar(255) NOT NULL, "
                            "url varchar(255)"
                            ")")
                insert = (f"INSERT INTO vacancies (name, salary_from, salary_to, currency, "
                          f"company, url) VALUES(%s, %s, %s, %s, %s, %s)")
                for vac in data_list:
                    item_tuple = (vac['name'], vac['from'], vac['to'], vac['currency'],
                                  vac['company'], vac['url'])
                    cur.execute(insert, item_tuple)
        print(f"Данные успешно загружены")
    finally:
        conn.close()

def information_output(params):
    dbman = DBManager(params)
    commands = ('companies', 'vacancies', 'avg_salary',
                'higher_salary', 'keyword', 'stop')
    command = ''
    while True:
        print(f"\nВыберите команду:\n")
        print(f"companies - выводит список всех компаний и \nколичество вакансий у каждой компании;\n"
              f"vacancies - получает список всех вакансий с указанием названия компании, \nназвания вакансии и зарплаты и ссылки на вакансию;\n"
              f"avg_salary - получает среднюю зарплату по вакансиям;\n"
              f"higher_salary - получает список всех вакансий, у которых \nзарплата выше средней по всем вакансиям;\n"
              f"keyword - получает список всех вакансий, в названии которых \nсодержатся переданные в метод слова, например python;\n"
              f"stop - завершает работу приложения.")
        command = input().lower().strip()
        if command not in commands:
            print(f"\nТАКОЙ КОМАНДЫ НЕТ!\n")
        else:
            if command == 'companies':
                dbman.get_companies_and_vacancies_count()
            elif command == 'vacancies':
                dbman.get_all_vacancies()
            elif command == 'avg_salary':
                dbman.get_avg_salary()
            elif command == 'higher_salary':
                dbman.get_vacancies_with_higher_salary()
            elif command == 'keyword':
                dbman.get_vacancies_with_keyword()
            elif command == 'stop':
                print(f"Работа завершена!")
                quit()