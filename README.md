# s-02 Project
[https://practicum.yandex.ru/learn/data-engineer/](https://practicum.yandex.ru/learn/data-engineer/courses/06c2ef78-3af1-44ca-a41d-5fa260a82feb/sprints/276961/topics/fbacb935-dfa7-4da3-a301-a3a89532ea03/lessons/6bb0202c-607e-4afc-ad6b-f6385672e00b/)
## Задача
1. Создать витрину customers
2. Написать запрос для инкрементального обновления витрины
## 1. Создание модели dwh
[create-tables.sql](https://github.com/dsyntez/yde/blob/main/s-02/create-tables.sql)
## 2. Вставка данных в таблицы измерений dwh из источников
[insert-source-dim.sql](https://github.com/dsyntez/yde/blob/main/s-02/insert-source-dim.sql)
## 3. Вставка данных в таблицу фактов dwh из источников
[insert-source-facts.sql](https://github.com/dsyntez/yde/blob/main/s-02/insert-source-facts.sql)
## 4. Создание модели Витрины customers
[datamart-ddl.sql](https://github.com/dsyntez/yde/blob/main/s-02/datamart-ddl.sql)
## 5. Инкрементальное обновление витрины
[increment-datamart.sql](https://github.com/dsyntez/yde/blob/main/s-02/increment-datamart.sql)
