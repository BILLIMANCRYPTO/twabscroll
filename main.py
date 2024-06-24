import requests
import json
import logging
from web3 import Web3
from web3.middleware import geth_poa_middleware
from datetime import datetime, timedelta
from tqdm import tqdm
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
import csv
import os
from decimal import Decimal
from queue import Queue
from collections import OrderedDict
from settings import RPC_URLS, WORKERS, MAX_WALLETS, DATE, PRICE

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Инициализация Web3
web3_providers = [Web3(Web3.HTTPProvider(url)) for url in RPC_URLS]
for web3 in web3_providers:
    web3.middleware_onion.inject(geth_poa_middleware, layer=0)

# Проверка соединения с узлом
if not all(web3.is_connected() for web3 in web3_providers):
    raise ConnectionError("Failed to connect to one or more Ethereum nodes.")

# Чтение адресов кошельков из файла wallets.txt и преобразование их в checksummed адреса
def read_wallets(file_path):
    with open(file_path, 'r') as file:
        wallets = [Web3.to_checksum_address(line.strip()) for line in file.readlines()]
    return wallets

# Функция для получения временной метки блока
@lru_cache(maxsize=None)
def get_block_timestamp(web3, block_number):
    block = web3.eth.get_block(block_number)
    return block['timestamp']

# Биноминальный поиск блока по временной метке
@lru_cache(maxsize=None)
def find_block_by_timestamp(web3, target_timestamp, start_block, end_block):
    while start_block <= end_block:
        mid_block = (start_block + end_block) // 2
        mid_timestamp = get_block_timestamp(web3, mid_block)

        if mid_timestamp < target_timestamp:
            start_block = mid_block + 1
        elif mid_timestamp > target_timestamp:
            end_block = mid_block - 1
        else:
            return mid_block

    return start_block if start_block <= end_block else end_block

# Функция для получения баланса на определенную дату
def get_balance(web3, wallet_address, block_number):
    retry_count = 0
    while retry_count < 5:
        try:
            balance = web3.eth.get_balance(wallet_address, block_identifier=block_number)
            return web3.from_wei(balance, 'ether')
        except Exception as e:
            if '429' in str(e):
                retry_count += 1
                time.sleep(2 ** retry_count)  # Экспоненциальная задержка
            else:
                raise e
    raise Exception(f"Failed to get balance for wallet {wallet_address} at block {block_number} after several retries.")

# Функция для обработки одного дня для кошелька
def process_day(wallet, current_date, web3):
    target_end_timestamp = int(current_date.timestamp()) + 86400 - 1

    latest_block = web3.eth.block_number
    genesis_block = 0
    end_block = find_block_by_timestamp(web3, target_end_timestamp, genesis_block, latest_block)

    end_balance = get_balance(web3, wallet, end_block)
    return end_balance

# Функция для обработки одного кошелька
def process_wallet(wallet, start_date, end_date, total_days, eth_price, cache, web3, progress):
    total_balance = Decimal(0)
    cache_key = wallet.lower()
    cache_data = cache.get(cache_key, {})
    last_checked = cache_data.get('last_checked')
    cached_total_balance = Decimal(cache_data.get('total_balance', '0'))

    if last_checked:
        last_checked_date = datetime.fromisoformat(last_checked)
    else:
        last_checked_date = start_date

    if last_checked_date >= end_date:
        # Use cached data if it's already up to date
        total_balance = cached_total_balance
    else:
        # Calculate balance for missing dates
        day_chunks = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]
        missing_dates = [d for d in day_chunks if d > last_checked_date]

        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            futures = [executor.submit(process_day, wallet, current_date, web3) for current_date in missing_dates]
            for future in tqdm(as_completed(futures), total=len(futures), desc=f'Processing {wallet}'):
                end_balance = future.result()
                total_balance += end_balance
                progress.update(1)

        total_balance += cached_total_balance
        cache[cache_key] = {
            'total_balance': str(total_balance),
            'last_checked': end_date.isoformat()
        }

    average_balance = total_balance / total_days
    TWAB = average_balance * eth_price
    return wallet, TWAB

def load_cache(cache_file):
    if os.path.exists(cache_file):
        with open(cache_file, 'r') as file:
            return json.load(file)
    return {}

def save_cache(cache_file, cache):
    # Преобразование всех значений Decimal в строку перед сохранением
    cache_to_save = {
        k: {
            'total_balance': str(v['total_balance']),
            'last_checked': v['last_checked']
        } for k, v in cache.items()
    }
    with open(cache_file, 'w') as file:
        json.dump(cache_to_save, file, indent=4)

def worker(queue, start_date, end_date, total_days, eth_price, cache, web3, progress_positions):
    while not queue.empty():
        wallet = queue.get()
        try:
            progress = tqdm(total=total_days, desc=f'Analyzing Wallet {wallet}', position=progress_positions[wallet])
            logger.info(f'Starting analysis for wallet {wallet}')
            wallet_address, TWAB = process_wallet(wallet, start_date, end_date, total_days, eth_price, cache, web3, progress)
            results[wallet_address] = TWAB
            logger.info(f'Finished analysis for wallet {wallet}: TWAB {TWAB} USD')
        except Exception as e:
            logger.error(f'Error processing wallet {wallet}: {e}')
        finally:
            queue.task_done()

def main():
    # Путь к файлу с адресами кошельков
    wallets_file = 'wallets.txt'
    # Файл кеша
    cache_file = 'balance_cache.json'
    # Начальная и конечная дата
    start_date = datetime.strptime('2023-10-18', '%Y-%m-%d')
    end_date = datetime.strptime(DATE, '%Y-%m-%d')
    total_days = (end_date - start_date).days + 1
    eth_price = Decimal(PRICE)  # курс эфира

    # Чтение адресов кошельков
    wallets = read_wallets(wallets_file)

    # Загрузка кеша
    cache = load_cache(cache_file)

    global results
    results = OrderedDict()

    # Создание очереди задач
    queue = Queue()
    for wallet in wallets:
        queue.put(wallet)

    # Создание словаря для отслеживания позиций прогресс-баров
    progress_positions = {wallet: i for i, wallet in enumerate(wallets)}

    with ThreadPoolExecutor(max_workers=len(RPC_URLS) * MAX_WALLETS) as executor:
        for web3 in web3_providers:
            for _ in range(MAX_WALLETS):
                executor.submit(worker, queue, start_date, end_date, total_days, eth_price, cache, web3, progress_positions)

        queue.join()  # Ожидание завершения всех задач

    # Запись результатов в CSV файл
    with open('twab_results.csv', 'w', newline='') as csvfile:
        fieldnames = ['Index', 'Wallet', 'TWAB']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for idx, wallet in enumerate(wallets, start=1):
            twab = results.get(wallet, 'N/A')
            writer.writerow({'Index': idx, 'Wallet': wallet, 'TWAB': twab})

    # Сохранение кеша
    save_cache(cache_file, cache)

if __name__ == "__main__":
    main()
