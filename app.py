from modules.validation import valid_cells
import time
# phoneFrame = pd.read_csv('dirty.csv', usecols=['contact_phone'])
# phoneFrame.to_csv('phone_output.txt', sep='\t', index=False)

# dataasd = ['', 'vfbf', '3425', '5566']


import pandas as pd

csvInputPath = "data/dirty.csv"
csvOutputPath = "data/golden_records.csv"
df = pd.read_csv(csvInputPath, low_memory=False)

# Извлекаем нужные столбцы в goldenRecord
goldenRecord = df[["client_first_name", "client_middle_name", "client_last_name", "client_fio_full",
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

goldenRecord['update_date'] = pd.to_datetime(goldenRecord['update_date'], errors='coerce')
#ИНН - целева переменная

# Множество для хранения уникальных ИНН
seen_inns = {}
# Заполнем хэш-таблицу

start_time = time.time()
for index, row in goldenRecord.iterrows():
    print(valid_cells(row[index]))
end_time = time.time()
print(f"Время выполнения программы: {end_time - start_time:.4f} секунд")
