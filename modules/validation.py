import pandas as pd
import re
import phonenumbers
from phonenumbers import carrier
from phonenumbers.phonenumberutil import number_type
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

def is_non_empty(input_value):
    return bool(str(input_value).strip())

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def validate_non_empty(validation_function, input_value):
    if not is_non_empty(input_value):  # strip() удаляет пробелы в начале и в конце
        return False
    else:
        # Если строка не пустая, выполняем валидацию
        return validation_function(input_value)
    
def client_bplace_validation(client_bplace): #Можно откоректировать
    return True

def date_validation(date_str):
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        return False

def phone_validation(phone_number):
    try:
        number = phonenumbers.parse(phone_number)
        if phonenumbers.is_mobile(number):
            return True
        return False
    except (phonenumbers.NumberParseException, ValueError):
        return False

def email_validation(email):
    if isinstance(email, str) and re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email):
        return True
    return False

def zip_code_validation(zip_code):
    if isinstance(zip_code, str) and re.match(r'^\d{6}$', zip_code):
        return True
    return False

def addr_street_validation(input_str):
    if is_number(input_str):
        return False
    return True

validationFunctions = {
    'client_id': is_non_empty,
    'client_first_name': is_non_empty,
    'client_middle_name': is_non_empty,
    'client_last_name': is_non_empty,
    'client_fio_full': is_non_empty,
    'client_bday': date_validation,
    'client_bplace': client_bplace_validation,
    'client_cityzen': is_non_empty,
    'client_resident_cd': is_non_empty,
    'client_gender': is_non_empty,
    'client_marital_cd': is_non_empty, 
    'client_graduate': is_non_empty,
    'client_child_cnt': is_non_empty,
    'client_mil_cd': is_non_empty,
    'client_zagran_cd': is_non_empty,
    'client_inn': is_non_empty,
    'client_snils': is_non_empty,
    'client_vip_cd': is_non_empty,
    'contact_vc': is_non_empty,
    'contact_tg': is_non_empty,
    'contact_other': is_non_empty,
    'contact_email': email_validation,
    'contact_phone': phone_validation,
    'addr_region': is_non_empty,
    'addr_country': is_non_empty,
    'addr_zip': zip_code_validation,
    'addr_street': addr_street_validation,
    'addr_str': is_non_empty,
    'addr_house': is_non_empty,
    'addr_body': is_non_empty,
    'addr_flat': is_non_empty,
    'addr_area': is_non_empty,
    'addr_loc': is_non_empty,
    'addr_city': is_non_empty,
    'addr_reg_dt': is_non_empty,
    'fin_rating': is_non_empty,
    'fin_loan_limit': is_non_empty,
    'fin_loan_value': is_non_empty,
    'fin_loan_debt': is_non_empty,
    'fin_loan_percent': is_non_empty,
    'fin_loan_begin_dt': date_validation,
    'fin_loan_end_dt': date_validation,
    'stream_favorite_show': is_non_empty,
    'stream_duration': is_non_empty,
    'create_date': date_validation,
    'update_date': date_validation,
    'source_cd': is_non_empty
}

def valid_cells(data):
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(validationFunctions[list(validationFunctions.keys())[index]], column) for index, column in enumerate(data)]
        results = [future.result() for future in futures]
    return [index + 1 for index, result in enumerate(results) if result]