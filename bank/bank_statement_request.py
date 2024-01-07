import re
import os
import jwt
import time
import json
import dotenv
import base64
import pickle
import requests
import datetime


dotenv.load_dotenv('.env')

login_ms = os.environ['login_ms']
password_ms = os.environ['password_ms']
authorization_ms = base64.b64encode(bytes(f'{login_ms}:{password_ms}'.encode("UTF-8")))

header_for_ms = {"Content-Type": "application/json", "Authorization": f'Basic {authorization_ms.decode("UTF-8")}',
                 "Accept-Encoding": "gzip"}

url_ms = "https://api.moysklad.ru/api/remap/1.2/entity/"

application_key = os.environ['application_key']
application_password = os.environ['application_password']

tokens = {"token": os.environ['token'],
          "access_token": os.environ['access_token'],
          "refresh_token": os.environ['refresh_token']}

server_error_codes = ["<Response [500]>", "<Response [501]>", "<Response [502]>", "<Response [503]>",
                      "<Response [504]>", "<Response [520]>", "<Response [521]>", "<Response [522]>",
                      "<Response [523]>", "<Response [524]>"]

list_of_operations = []

contractors = {}


def loading_from_list_of_operations(list_operations):
    with open('config.pkl', 'rb') as f:
        loaded_list = pickle.load(f)

    for i in loaded_list:
        if i not in list_operations:
            list_operations.append(i)


def uploading_to_list_of_operations(uploading_dict):
    with open('config.pkl', 'wb') as f:
        pickle.dump(uploading_dict, f)


def refresh_access_token():
    # функция обновления access-токена, время жизни access-токена(токен доступа) - 60 минут,
    # время жизни refresh-токена(токен для обновления токена) - 23 часа 30 минут

    url = "https://api.priorbank.by:9344/authorize/v2/oauth2/refreshToken/token"

    header = {"Accept": "application/json",
              "Content-Type": "application/x-www-form-urlencoded",
              "Authorization": 'Bearer null'}

    body = f'clientID={application_key}&clientSecret={application_password}&refreshToken={tokens["refresh_token"]}'

    while True:
        result = requests.post(url, headers=header, data=body)
        if str(result) == "<Response [200]>":
            dictionary = json.loads(result.text)
            decoded_data = jwt.decode(dictionary["accessToken"], options={"verify_signature": False})
            tokens["access_token"] = decoded_data['jti']
            tokens["refresh_token"] = dictionary["refreshToken"]
            break
        elif str(result) in server_error_codes:
            time.sleep(30)


def bank_statement_request():  # функция получения выписки
    date = datetime.date.today()

    url = f'https://api.priorbank.by:9344/account/v1/transactions?accounts=' \
          f'C-1046248&dateFrom={date.year}-{date:%m}-{date:%d}&dateTo={date.year}-{date:%m}-{date:%d}'

    while True:
        header = {"Accept": "application/json",
                  "Content-Type": "application/x-www-form-urlencoded",
                  "Authorization": f'Bearer {tokens["access_token"]}'}
        result = requests.get(url, headers=header)
        if str(result) == "<Response [200]>":
            dictionary = json.loads(result.text)
            for i in dictionary["data"][0]["payments"]:
                if str(i["customerTaxNumber"]) not in contractors:
                    create_counterparty(i["customerTaxNumber"], header_for_ms, url_ms)

                if i["amount"]["creditAmount"] != 0:   # входящий платеж
                    credit_amount = 1
                    create_payment(credit_amount, header_for_ms, contractors[i["customerTaxNumber"]],
                                   i["docDate"][0:10], i["amount"]["creditAmount"], i["number"], i["description"],
                                   i["id"], url_ms)
                elif i["amount"]["debitAmount"] != 0:   # исходящий платеж
                    credit_amount = 0
                    create_payment(credit_amount, header_for_ms, contractors[i["customerTaxNumber"]],
                                   i["docDate"][0:10], i["amount"]["debitAmount"], i["number"], i["description"],
                                   i["id"], url_ms)
            break
        elif str(result) == "<Response [401]>":
            refresh_access_token()
            time.sleep(10)
        elif str(result) in server_error_codes:
            time.sleep(30)


def get_contractors(header, url_for_ms):
    url = f'{url_for_ms}counterparty?async=true&filter=inn!='

    while True:
        result = requests.get(url, headers=header)
        if str(result) == "<Response [202]>":
            array = [result.headers]
            get_result_async = array[0]['Location']
            get_status_async = array[0]['Content-Location']
            time.sleep(15)
            break
        elif str(result) in server_error_codes:
            time.sleep(30)

    while True:
        status = requests.get(get_status_async, headers=header)
        dictionary_2 = json.loads(status.text)

        if dictionary_2["state"] == "DONE":
            result = requests.get(get_result_async, headers=header)
            dictionary_2 = json.loads(result.text)
            for i in dictionary_2["rows"]:
                contractors[i["inn"]] = i["meta"]["href"]
            break
        elif dictionary_2["state"] == "PENDING" or "PROCESSING":
            time.sleep(15)
        elif dictionary_2["state"] == "ERROR" or "CANCEL" or "API_ERROR":
            time.sleep(10)
            result_v2 = requests.get(url, headers=header)
            array_2 = [result_v2.headers]
            get_result_async = array_2[0]['Location']
            get_status_async = array_2[0]['Content-Location']


def create_payment(credit_amount, header, counterparty, payment_date, payment_amount, payment_number, payment_text,
                   id_payment, url_for_ms):
    # Рекурсивная функция создания платежа, отрабатывает тогда, когда получены метаданные всех контрагентов,
    # а также получена выписка из банка.

    payment_amount_correct = 0

    if type(payment_amount) == int:
        payment_amount_correct = int(str(payment_amount) + "00")
    else:
        whole = int(payment_amount)
        fraction = round(payment_amount % 1 * 100)
        if fraction >= 10:
            payment_amount_correct = int(str(whole) + str(fraction))
        elif fraction < 10:
            payment_amount_correct = int(str(whole) + "0" + str(fraction))

    url = f'{url_for_ms}paymentin' if credit_amount == 1 else f'{url_for_ms}paymentout'

    body = {"organization": {
        "meta": {
            "href": f'{url_for_ms}organization/38bc7430-bc48-11ed-0a80-0860003d4961',
            "metadataHref": f'{url_for_ms}organization/metadata',
            "type": "organization",
            "mediaType": "application/json"
        }
    }, "agent": {
        "meta": {
            "href": f'{counterparty}',
            "metadataHref": f'{url_for_ms}counterparty/metadata',
            "type": "counterparty",
            "mediaType": "application/json"
        }
    }, "moment": f'{payment_date} 10:00:00.000',
        "name": f'{payment_number}',
        "sum": payment_amount_correct,
        "paymentPurpose": f'{payment_text}',
        "expenseItem": {
            "meta": {
                "href": f'{url_for_ms}expenseitem/8dbf9374-0a01-11e4-b9bf-002590a32f46',
                "metadataHref": f'{url_for_ms}expenseitem/metadata',
                "type": "expenseitem",
                "mediaType": "application/json"
            }
        },
        "project": {
            "meta": {
                "href": f'{url_for_ms}project/6e25e670-be99-11ed-0a80-0861000035da' if
                        credit_amount == 1 else
                        f'{url_for_ms}project/13fbdd1e-c0ef-11ed-0a80-0c6f00278452',
                "metadataHref": f'{url_for_ms}project/metadata',
                "type": "project",
                "mediaType": "application/json"
            }
        }
    }

    if credit_amount == 1:
        del body["expenseItem"]

    if id_payment not in list_of_operations:
        result = requests.post(url, headers=header, json=body)
        if str(result) == "<Response [412]>":
            count = 0
            dictionary = json.loads(result.text)
            if dictionary['errors'][0]['code'] == 3006:
                if "/" not in payment_number:
                    count += 1
                    payment_number_correct = f'{payment_number}/{count}'
                    body["name"] = payment_number
                    create_payment(credit_amount, header, counterparty, payment_date, payment_amount,
                                   payment_number_correct, payment_text, id_payment, url_ms)
                elif "/" in payment_number:
                    search = re.search("[/]\d{1,}", payment_number)  # находим последние числа платежа после "/"
                    count = int(search[0].replace("/", "")) + 1   # убираем "/" и присваеваем переменной count значение после "/", чтобы начать счет в цикле с нужного значения
                    payment_number_correct = f'{payment_number.replace(search[0], "")}/{count}'
                    create_payment(credit_amount, header, counterparty, payment_date, payment_amount,
                                   payment_number_correct, payment_text, id_payment, url_ms)
            else:
                pass
        elif str(result) == "<Response [200]>":
            list_of_operations.append(id_payment)
        elif str(result) in server_error_codes:
            print(f'{result} Сервис моего склада не доступен!')
            time.sleep(15)
            create_payment(credit_amount, header, counterparty, payment_date, payment_amount, payment_number,
                           payment_text, id_payment, url_ms)


def create_counterparty(counterparty, header, url_for_ms):
    # функция создания контрагента, отрабатывает, если в словаре "contractors" нету нужного УНП с метаданными

    url_nalog = f'https://www.portal.nalog.gov.by/grp/getData?unp={counterparty}&charset=UTF-8&type=json'

    url = f'{url_for_ms}counterparty'

    while True:
        result = requests.get(url_nalog)
        if str(result) == "<Response [200]>":
            dictionary = json.loads(result.text)
            break
        elif str(result) in server_error_codes:
            time.sleep(30)

    while True:
        result_2 = requests.post(url, headers=header, json={"name": f'{dictionary["row"]["vnaimp"]}',
                                                            "code": f'{dictionary["row"]["vunp"]}',
                                                            "legalTitle": f'{dictionary["row"]["vnaimp"]}',
                                                            "inn": f'{dictionary["row"]["vunp"]}'})
        if str(result) == "<Response [200]>":
            dictionary_2 = json.loads(result_2.text)
            contractors[dictionary["row"]["vunp"]] = dictionary_2["meta"]["href"]
            break
        elif str(result) in server_error_codes:
            time.sleep(30)


def main(list2):  # основная функция
    time_start = time.time()
    while True:
        try:
            time.sleep(1)
            date_now = datetime.datetime.now()
            day_today = date_now.weekday()
            if day_today <= 5:
                if 6 <= date_now.hour <= 15:
                    time_now = time.time()
                    if int(time_now.__round__()) - int(time_start.__round__()) > 50:
                        loading_from_list_of_operations(list2)
                        get_contractors(header_for_ms, url_ms)
                        bank_statement_request()
                        uploading_to_list_of_operations(list2)
                        time_start = time_now
                    time.sleep(1)
                elif date_now.hour == 21:
                    list2.clear()
                    uploading_to_list_of_operations(list2)
            elif day_today == 6:
                time_now = time.time()
                if int(time_now.__round__()) - int(time_start.__round__()) > 15000:
                    refresh_access_token()
                    time_start = time_now
        except requests.exceptions.RequestException as error:
            print(error)
            time.sleep(20)
        except json.decoder.JSONDecodeError as error_json:
            print(error_json)
            time.sleep(10)


main(list_of_operations)
