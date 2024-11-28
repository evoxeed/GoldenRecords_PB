import pandas as pd

pd.options.mode.chained_assignment = None
# Загружаем данные
csvInputPath = "data/dirty.csv"
csvOutputPath = "data/golden_records.csv"
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
#ИНН - целева переменная

# Множество для хранения уникальных ИНН 
seen_inns = {}
# Заполнем хэш-таблицу
for index, row in goldenRecord.iterrows():
    inn = str(row['client_inn'])

    # Проверка на ненулевые знч.
    non_null_columns = row[(row.notnull()) & (row != "")].index.tolist()

    # Если ИНН еще не записан, записываем его в хэш
    if inn not in seen_inns:
        seen_inns[inn] = [row['update_date'], row] # Сохраняем дату последней записи
        
    else:
        last_update = seen_inns[inn][0]  # Дата последней записи для этого ИНН
    
        # Обновляем данные
        if row['update_date'] > last_update:
            seen_inns[inn][0] = row['update_date']  # Обновляем дату для ИНН
        
            for i in non_null_columns: #Дополняем ряд инфой
                seen_inns[inn][1][i] = row[i]

 
# Заполняем файл
with open(csvOutputPath, 'w', newline='', encoding='utf-8') as f:
    f.write('\t'.join(goldenRecord.columns) + '\n')
    for inn, data in seen_inns.items():
        record = data[1]  # Достаем словарь с данными
        row = [record.get(column, '') for column in goldenRecord.columns]  # Получаем значения по заголовкам
        f.write('\t'.join(map(str, row)) + '\n')

print("'Золотая запись' была записана в 'golden_records.csv'.")