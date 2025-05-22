Установка
```bash
pip install --upgrade pip
pip install -r requirements.txt
dbt debug  # проверка подключения
```

Загрузка данных
- Авторизоваться на kaggle для получения токена https://www.kaggle.com/docs/api
```bash
python load_linkedin_jobs.py
python load_job_summary.py
python load_job_skills.py
```

Packages
- dbt_utils
```bash
dbt deps # add packages 
```

Основные команды
```bash
dbt snapshot # Запускает модели типа snapshot для отслеживания изменений в данных (SCD Type 2). Создает или обновляет таблицы с историей изменений.
dbt compile # Компилирует Jinja-шаблоны в чистый SQL, но не выполняет его в БД. Результат сохраняется в папке target/compiled. Полезен для отладки SQL-кода перед запуском.
dbt run # Запускает преобразование данных
dbt build # Выполняет полный цикл работы: последовательно запускает run, test, seed (если есть) и snapshot (если есть) для всех моделей или указанных. 
dbt docs generate; # Создает статическую документацию на основе кода и комментариев (_row_jobs__sources.yml).
dbt docs serve # Запускает локальный веб-сервер для просмотра документации.
# http://localhost:8080/#!/overview
dbt run -m name_of_model
dbt run -m tag:name_tag  # Запускает все модели с тегом name_tag. Теги задаются в dbt_project.yml или в config модели
dbt test  # Запускает тесты данных (проверки на уникальность, NULL, кастомные SQL-тесты). Тесты определяются в _row_jobs__sources.yml или отдельных SQL-файлах.  
```

Github actions
- Настроены 2 workflows: dbt_ci (pull_request), dbt_deploy (branch main). 
- Для запуска необходимо настроить secret key github

Проверка локально
- Установить act
- Добавить .env
```bash
curl https://raw.githubusercontent.com/nektos/act/master/install.sh | sudo bash
./act -W .github/workflows/dbt_deploy.yml --env-file .env
```
