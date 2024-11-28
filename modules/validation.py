import pandas as pd
import re
import phonenumbers
from phonenumbers import carrier
from phonenumbers.phonenumberutil import number_type
from datetime import datetime

def is_non_empty(input_value):
    # Проверяем, не является ли строка пустой
    if str(input_value).strip() == '':  # strip() удаляет пробелы в начале и в конце
        return False
    return True

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def validate_non_empty(validation_function, input_value):
    # Проверяем, не является ли строка пустой
    if not is_non_empty(input_value):  # strip() удаляет пробелы в начале и в конце
        return False
    else:
        # Если строка не пустая, выполняем валидацию
        return validation_function(input_value)
    
def client_bplace_validation(client_bplace): #Можно откоректировать
    return True

def date_validation(client_bday):
    # Регулярное выражение для проверки формата даты в формате 'YYYY-MM-DD'
    date_regex = r'^\d{4}-\d{2}-\d{2}$'

    # Проверка на соответствие регулярному выражению
    if re.match(date_regex, client_bday):
        try:
            # Попробуем преобразовать строку в объект datetime
            datetime.strptime(client_bday, '%Y-%m-%d')
            return True  # Если дата валидна
        except ValueError:
            return False  # Если дата не валидна (например, 2024-02-30)
    else:
        return False  # Если формат не соответствует

def phone_validation(phoneNumber):
    try:
        # Пробуем разобрать номер телефона
        number2Validate = phoneNumber
        # Проверяем, является ли номер мобильным
        if carrier._is_mobile(number_type(phonenumbers.parse(number2Validate))):
            return True  # Номер телефона валиден и мобильный
        else:
            return False  # Номер телефона валиден, но не мобильный
    except phonenumbers.NumberParseException:
        return False  # Невалидный номер телефона

def email_validation(email):
    if isinstance(email, str):  # Проверяем, является ли email строкой
        # Регулярное выражение для проверки формата электронной почты
        email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        # Проверяем, соответствует ли email регулярному выражению
        return re.match(email_regex, email) is not None
    return False  # Если email не строка, возвращаем False

def zip_validation(zip_code):
    if isinstance(zip_code, str):  # Проверяем, является ли zip_code строкой
        # Регулярное выражение для проверки формата почтового индекса
        zip_regex = r'^\d{6}$'  # Формат: 123456
        # Проверяем, соответствует ли zip_code формату
        return re.match(zip_regex, zip_code) is not None
    return False  # Если zip_code не строка, возвращаем False

def addr_street_validation(input_str):
    if is_number(input_str):
        return False
    return True

validationFunctions = {
    'client_id': False,
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
    'addr_zip': zip_validation,
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
    valid_cells_array = []
    for index, column in enumerate(data, start=1):
        if validationFunctions[list(validationFunctions.keys())[index]](column):
            valid_cells_array.append(index)
    return valid_cells_array