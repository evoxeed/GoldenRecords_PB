# Perfect Bugs — Хаб, трек 1
```Python 3.12.7```
## Инструкция по установке
1. Установка всех зависимостей 
```
bash 
pip install -r requirements.txt
```
2. Файл с исходным датасетом должен называться dirty.csv и храниться в папке data/
  (путь до файла определён в файле main.py - переменная csvInputPath)

3. Запуск программы
```
bash 
python main.py
```
4. Готовая таблица по умолчанию сохраняется в файл golden_records.csv в папке data/
  (путь до файла определён в файле main.py - переменная csvOutputPath)

## Пояснения по алгоритму

1. Все данные берутся ПОЛНОСТЬЮ из исходной csv таблицы
2. Создается хэш-таблица, в которую мы записываем все строки с целевой переменной
3. В случае повтороений ключевого значения - мы смотрим на дату изменения записи. И если в более актуальной записи найдены НЕПУСТЫЕ значения - их переписываем в запись с данным ключом
4. Создаем csv таблицу из хэш-таблицы
5. profit

## Уточнения
1. Валидирующие методы замедляют работу программы до невозможности получения выходной csv таблицы
2. В данный момент нет возможности увеличить число целевых переменных в связи с усталостью команды
3. Мы ПРОПУСКАЕМ всех пользователей БЕЗ ИНН (стриминговые платформы)( по определению использования целевой переменной)
4. Многопоточность добавлена, но реализация не позволяет нагрузить командное железо, так как используются медленные модули памяти и получается упор в ОЗУ, а не в ЦП
