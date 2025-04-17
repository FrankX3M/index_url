import os
import csv
import requests
import json
from dotenv import load_dotenv
import logging
from time import sleep
from urllib.parse import urlparse
from google.oauth2 import service_account
from google.auth.transport.requests import Request

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("reindex.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Константы Яндекса
YANDEX_API_TOKEN = os.getenv("YANDEX_API_TOKEN")
if not YANDEX_API_TOKEN:
    logger.error("Не указан токен Яндекса в переменных окружения")
    raise ValueError("YANDEX_API_TOKEN не найден в .env файле")

SITE_URL = os.getenv("SITE_URL")
if not SITE_URL:
    logger.error("Не указан URL сайта в переменных окружения")
    raise ValueError("SITE_URL не найден в .env файле")

YANDEX_API_BASE = "https://api.webmaster.yandex.net/v4"
YANDEX_HEADERS = {
    "Authorization": f"OAuth {YANDEX_API_TOKEN}",
    "Content-Type": "application/json"
}

# Константы Google
SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE")
if not SERVICE_ACCOUNT_FILE or not os.path.exists(SERVICE_ACCOUNT_FILE):
    logger.error(f"Файл учетных данных Google не найден: {SERVICE_ACCOUNT_FILE}")
    raise ValueError(f"SERVICE_ACCOUNT_FILE не найден: {SERVICE_ACCOUNT_FILE}")

SCOPES = ["https://www.googleapis.com/auth/indexing"]
GOOGLE_API_BASE = "https://indexing.googleapis.com/v3/urlNotifications:publish"

# Инициализация учетных данных Google
try:
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
except Exception as e:
    logger.error(f"Ошибка при загрузке учетных данных Google: {e}")
    raise


def get_access_token():
    """Получает актуальный токен доступа Google API"""
    try:
        credentials.refresh(Request())
        return credentials.token
    except Exception as e:
        logger.error(f"Ошибка при получении токена Google: {e}")
        raise


def build_yandex_host_id(site_url):
    """Формирует host_id в формате 'https:example.com:443'"""
    try:
        parsed = urlparse(site_url)
        scheme = parsed.scheme
        netloc = parsed.netloc
        
        if not scheme or not netloc:
            raise ValueError(f"Некорректный URL сайта: {site_url}")
            
        port = "443" if scheme == "https" else "80"
        host_id = f"{scheme}:{netloc}:{port}"
        logger.info(f"Сформирован host_id: {host_id}")
        return host_id
    except Exception as e:
        logger.error(f"Ошибка при формировании host_id: {e}")
        raise


def get_yandex_user_id():
    """Получает user-id из API Яндекса"""
    try:
        response = requests.get(f"{YANDEX_API_BASE}/user", headers=YANDEX_HEADERS)
        response.raise_for_status()
        data = response.json()
        
        if "user_id" not in data:
            raise ValueError(f"В ответе API отсутствует user_id: {data}")
            
        user_id = data["user_id"]
        logger.info(f"Получен user-id: {user_id}")
        return user_id
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка при запросе user-id: {e}")
        raise


def send_reindex_yandex(user_id, host_id, url):
    """
    Отправляет запрос на переиндексацию URL в Яндексе
    и проверяет успешность отправки запроса
    """
    payload = {"url": url}
    try:
        api_url = f"{YANDEX_API_BASE}/user/{user_id}/hosts/{host_id}/recrawl/queue"
        logger.debug(f"Отправка запроса в Яндекс: {api_url} с данными {payload}")
        
        response = requests.post(
            api_url, 
            headers=YANDEX_HEADERS, 
            json=payload
        )
        response.raise_for_status()
        
        # Проверяем содержимое ответа
        response_data = response.json()
        logger.debug(f"Яндекс API ответ: {response_data}")
        
        # Проверка на наличие ошибок в ответе
        if "error" in response_data:
            return "неуспешно", f"Ошибка API: {response_data['error']}"
        
        # Проверка успешности добавления URL в очередь
        if "task_id" in response_data:
            return "успешно", None
        else:
            return "успешно (без task_id)", None
            
    except requests.exceptions.HTTPError as e:
        error_msg = f"Ошибка HTTP при переиндексации Яндекса: {e}"
        logger.error(error_msg)
        
        # Попытка получить детали ошибки из ответа
        try:
            error_details = response.json()
            logger.error(f"Детали ошибки Яндекс API: {error_details}")
            return "неуспешно", json.dumps(error_details)
        except Exception:
            return "неуспешно", str(e)
            
    except Exception as e:
        error_msg = f"Неожиданная ошибка при переиндексации Яндекса: {e}"
        logger.error(error_msg)
        return "неуспешно", str(e)


def publish_url_google(url, action="URL_UPDATED"):
    """
    Отправляет запрос на переиндексацию URL в Google
    и проверяет успешность отправки запроса
    """
    headers = {
        "Authorization": f"Bearer {get_access_token()}",
        "Content-Type": "application/json"
    }
    payload = {
        "url": url,
        "type": action
    }
    try:
        logger.debug(f"Отправка запроса в Google: {GOOGLE_API_BASE} с данными {payload}")
        
        response = requests.post(GOOGLE_API_BASE, headers=headers, json=payload)
        response.raise_for_status()
        
        # Проверяем содержимое ответа
        response_data = response.json()
        logger.debug(f"Google API ответ: {response_data}")
        
        # Проверка статуса ответа
        if "urlNotificationMetadata" in response_data:
            metadata = response_data["urlNotificationMetadata"]
            
            # Проверка на наличие ошибок в ответе
            if "latestUpdate" in metadata and "type" in metadata["latestUpdate"]:
                if metadata["latestUpdate"]["type"] == action:
                    return "успешно", None
                else:
                    return "неуспешно", f"Ожидался тип {action}, получен {metadata['latestUpdate']['type']}"
            else:
                return "успешно", None  # Если нет явных ошибок, считаем успешным
        else:
            return "неуспешно", "Неожиданный формат ответа от Google API"
            
    except requests.exceptions.HTTPError as e:
        error_msg = f"Ошибка HTTP при переиндексации Google: {e}"
        logger.error(error_msg)
        
        # Попытка получить детали ошибки из ответа
        try:
            error_details = response.json()
            logger.error(f"Детали ошибки Google API: {error_details}")
            return "неуспешно", json.dumps(error_details)
        except Exception:
            return "неуспешно", str(e)
            
    except Exception as e:
        error_msg = f"Неожиданная ошибка при переиндексации Google: {e}"
        logger.error(error_msg)
        return "неуспешно", str(e)


def process_urls(input_file, output_file):
    """
    Обрабатывает список URL из входного файла и записывает результаты в выходной файл
    """
    results = []

    try:
        # Получаем необходимые идентификаторы для API
        host_id = build_yandex_host_id(SITE_URL)
        user_id = get_yandex_user_id()
        logger.info(f"Инициализация завершена. User ID: {user_id}, Host ID: {host_id}")
    except Exception as e:
        logger.error(f"Ошибка при инициализации: {e}")
        return

    # Проверяем существование входного файла
    if not os.path.exists(input_file):
        logger.error(f"Входной файл не найден: {input_file}")
        return

    try:
        with open(input_file, mode="r", encoding="utf-8-sig", newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            
            # Обработка BOM и пробелов в именах полей
            if reader.fieldnames:
                reader.fieldnames = [f.strip().replace("\ufeff", "") for f in reader.fieldnames]
            
            # Проверка наличия поля URL
            if "URL" not in reader.fieldnames:
                logger.error(f"В файле отсутствует колонка 'URL'. Доступные колонки: {reader.fieldnames}")
                return

            total_urls = 0
            processed_urls = 0
            
            for row in reader:
                total_urls += 1
                url = row.get("URL", "").strip()
                if not url:
                    logger.warning(f"Пропуск пустого URL в строке {total_urls}")
                    continue
                
                logger.info(f"Обработка URL [{processed_urls+1}/{total_urls}]: {url}")

                # Отправка запросов на переиндексацию
                status_yandex, error_yandex = send_reindex_yandex(user_id, host_id, url)
                status_google, error_google = publish_url_google(url)

                # Сохранение результатов
                results.append({
                    "URL": url,
                    "Yandex_Status": status_yandex,
                    "Yandex_Error": error_yandex if error_yandex else "",
                    "Google_Status": status_google,
                    "Google_Error": error_google if error_google else ""
                })

                processed_urls += 1
                logger.info(f"Результат: Яндекс - {status_yandex}, Google - {status_google}")
                
                # Пауза между запросами для соблюдения ограничений API
                sleep(1)

        # Запись результатов в выходной файл
        with open(output_file, mode="w", encoding="utf-8", newline="") as csvfile:
            fieldnames = ["URL", "Yandex_Status", "Yandex_Error", "Google_Status", "Google_Error"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)

        logger.info(f"Обработка завершена. Обработано URL: {processed_urls}/{total_urls}")
        logger.info(f"Результаты записаны в {output_file}")
        
    except Exception as e:
        logger.error(f"Ошибка при обработке файла: {e}")


if __name__ == "__main__":
    input_file = "urls.csv"
    output_file = "results.csv"
    
    logger.info("Запуск процесса переиндексации URL")
    process_urls(input_file, output_file)
    logger.info("Процесс завершен")
