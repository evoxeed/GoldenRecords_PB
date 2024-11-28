import pandas as pd
import multiprocessing.dummy as mp
from concurrent.futures import ThreadPoolExecutor
import os

# Загружаем данные
csvInputPath = "data/dirty.csv"
csvOutputPath = "data/golden_records5.csv"
df = pd.read_csv(csvInputPath, low_memory=False)

# Извлекаем нужные столбцы в goldenRecord
goldenRecord = df[["client_id", "client_first_name", "client_middle_name", "client_last_name", "client_fio_full", 
                   "client_bday", "client_bplace", "client_cityzen", "client_resident_cd", "client_gender", 
                   "client_marital_cd", "client_graduate", "client_child_cnt", "client_mil_cd", "client_zagran_cd", 
                   "client_inn", "client_snils", "client_vip_cd", "contact_vc", "contact_tg", "contact_other", 
                   "contact_email", "contact_phone", "addr_region", "addr_country", "addr_zip", "addr_street", 
                   "addr_house", "addr_body", "addr_flat", "addr_area", "addr_loc", "addr_city", "addr_reg_dt", 
                   "addr_str", "fin_rating", "fin_loan_limit", "fin_loan_value", "fin_loan_debt", "fin_loan_percent", 
                   "fin_loan_begin_dt", "fin_loan_end_dt", "stream_favorite_show", "stream_duration", "create_date", 
                   "update_date", "source_cd"]]

# Преобразуем в нужный формат
goldenRecord['update_date'] = pd.to_datetime(goldenRecord['update_date'], errors='coerce')

# Множество для хранения уникальных ИНН 
seen_inns = {}
def inn_validation(inn):
    if str(inn).isdigit():  # Проверяем, состоит ли строка только из цифр
        if len(str(inn)) == 12 or len(str(inn)) == 10:  # Проверяем длину ИНН
            return True
    return False
def process_row(row_tuple):
    index, row = row_tuple  # Извлекаем индекс и строку
    inn = str(row['client_inn'])
    
    non_null_columns = row[(row.notnull()) & (row != "") ].index.tolist()
    
    if inn not in seen_inns:
        seen_inns[inn] = [row['update_date'], row]  # Сохраняем дату последней записи
    else:
        last_update = seen_inns[inn][0]  # Дата последней записи для этого ИНН
        
        # Обновляем данные
        if row['update_date'] > last_update:
            seen_inns[inn][0] = row['update_date']  # Обновляем дату для ИНН
            
            for i in non_null_columns:  # Дополняем ряд инфой
                seen_inns[inn][1][i] = row[i]

# Создаем пул потоков для обработки строк
pool = mp.Pool()

# Обрабатываем строки DataFrame в параллельном режиме
pool.map(process_row, goldenRecord.iterrows())

# Закрываем пул потоков
pool.close()
pool.join()

# Функция для записи строки в файл
def write_row_to_file(record):
    row = [record.get(column, '') for column in goldenRecord.columns]  # Получаем значения по заголовкам
    return '\t'.join(map(str, row)) + '\n'

max_workers = os.cpu_count()

# Записываем данные в файл с использованием многопоточности
with open(csvOutputPath, 'w', newline='', encoding='utf-8') as f:
    f.write('\t'.join(goldenRecord.columns) + '\n')  # Записываем заголовки
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Запускаем запись строк в файл параллельно
        futures = {executor.submit(write_row_to_file, data[1]): inn for inn, data in seen_inns.items()}
        for future in futures:
            f.write(future.result())  # Записываем результат в файл

print("'Золотая запись' была записана в 'golden_records1.csv'.")
