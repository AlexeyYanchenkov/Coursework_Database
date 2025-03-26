import psycopg2

class DBManager:
    def __init__(self, params: dict):
        self.params = params
        DBManager.create_database(params)  # Вызываем статический метод через класс
        DBManager.create_tables(params)
        self.selected_companies = self.selecting_companies()

    def executing(self, cur):
        """Метод для вывода информации, запрошенной из таблицы"""
        rows = cur.fetchall()
        for row in rows:
            print(row)
        return rows

    @staticmethod
    def create_database(params):
        """Создает базу данных, если она не существует"""
        dbname = params["dbname"]
        params_copy = params.copy()
        params_copy.pop("dbname")  # Удаляем имя БД, чтобы подключиться без неё

        conn = psycopg2.connect(**params_copy)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{dbname}'")
            exists = cur.fetchone()
            if not exists:
                cur.execute(f"CREATE DATABASE {dbname}")
                print(f"База данных '{dbname}' создана")
        conn.close()

    @staticmethod
    def create_tables(params):
        """Создает таблицы organizations и vacancies"""
        conn = psycopg2.connect(**params)
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS organizations (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(255) UNIQUE NOT NULL
                    );
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS vacancies (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(255) NOT NULL,
                        salary_from INTEGER,
                        salary_to INTEGER,
                        currency VARCHAR(10),
                        url TEXT,
                        company_id INTEGER REFERENCES organizations(id)
                    );
                """)
                print("Таблицы созданы")
        conn.close()

    def insert_organization(self, company_name):
        """Добавляет организацию в таблицу organizations, если её там нет."""
        conn = psycopg2.connect(**self.params)
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("INSERT INTO organizations (name) VALUES (%s) ON CONFLICT (name) DO NOTHING", (company_name,))
        finally:
            conn.close()

    def selecting_companies(self):
        """Выбирает 10 компаний с наибольшим числом вакансий."""
        conn = psycopg2.connect(**self.params)
        companies = []
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT company, COUNT(*)
                        FROM vacancies
                        GROUP BY company
                        HAVING COUNT(*) > 1
                        ORDER BY COUNT(*) DESC
                        LIMIT 10
                    """)
                    rows = cur.fetchall()
        finally:
            conn.close()

        companies = [row[0] for row in rows]
        return companies

    def get_companies_and_vacancies_count(self):
        """получает список всех компаний и количество вакансий у каждой компании"""
        conn = psycopg2.connect(**self.params)
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT company, COUNT(*)
                        FROM vacancies
                        GROUP BY company
                        ORDER BY COUNT(*) DESC
                    """)
                    companies = self.executing(cur)
        finally:
            conn.close()
        return companies

    def get_avg_salary(self):
        """получает среднюю зарплату по вакансиям"""
        conn = psycopg2.connect(**self.params)
        tpl_companies = tuple(self.selected_companies)
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT company, AVG((salary_from + salary_to)/2) as total FROM vacancies "
                                f"WHERE company IN {tpl_companies} "
                                f"GROUP BY company "
                                f"ORDER BY total DESC")
                    self.executing(cur)
        finally:
            conn.close()

    def get_all_vacancies(self):
        """получает список всех вакансий с указанием названия компании,
        названия вакансии, зарплаты и ссылки на вакансию"""
        conn = psycopg2.connect(**self.params)
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT company, name, salary_from, salary_to, url FROM vacancies")
                    vacancies = self.executing(cur)
        finally:
            conn.close()
        return vacancies

    def get_vacancies_with_higher_salary(self):
        """получает список всех вакансий, у которых зарплата выше средней по всем вакансиям"""
        conn = psycopg2.connect(**self.params)
        try:
            with conn:
                with conn.cursor() as cur:
                    # Сначала находим среднюю зарплату по всем вакансиям
                    cur.execute("SELECT AVG((salary_from + salary_to) / 2) FROM vacancies WHERE currency = 'RUR'")
                    avg_salary = cur.fetchone()[0]
                    # Затем выбираем вакансии, где зарплата выше средней
                    cur.execute(f"""
                        SELECT name, salary_from, salary_to, company
                        FROM vacancies
                        WHERE (salary_from + salary_to) / 2 > {avg_salary}
                    """)
                    vacancies = self.executing(cur)
        finally:
            conn.close()
        return vacancies

    def get_vacancies_with_keyword(self):
        """получает список всех вакансий, в названии которых содержатся переданные в метод слова"""
        conn = psycopg2.connect(**self.params)
        print("Введите слово для отбора вакансий: ")
        word = input().lower().strip()  # Получаем ключевое слово от пользователя

        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        SELECT name, company
                        FROM vacancies
                        WHERE LOWER(name) LIKE '%{word}%'  -- Ищем слово в названии вакансии
                    """)
                    vacancies = self.executing(cur)
                    if len(vacancies) == 0:
                        print(f"По вашему запросу ничего не найдено!")
        finally:
            conn.close()
        return vacancies