from src.utils import get_vacancies_list, upload_to_database, information_output
from src.class_DBManager import DBManager #Добавляем импорт
from src.config import config
import os

def main():
    path_to_file = os.path.join('data', 'database.ini')
    params = config(path_to_file)

    #Сначала создаем БД и таблицы
    DBManager.create_database(params)
    DBManager.create_tables(params)

    #Получаем список вакансий
    vacancies_list = get_vacancies_list()

    #Загружаем вакансии в базу
    upload_to_database(vacancies_list, params)

    #Выводим информацию
    information_output(params)

if __name__ == '__main__':
    main()