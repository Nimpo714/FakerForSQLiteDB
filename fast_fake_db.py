import asyncio
import aiosqlite
from faker import Faker
import time

# --- Настройки ---
BATCH_SIZE = 100000  # Количество строк в одной пакетной вставке
DB_PATH = 'data.db'  # Название для базы данных
fake = Faker('en_US')  # Локаль
total_inserted_count = 0  # Всего вставлено нужно в будущем
start_time = None


def generate_fake_data_batch(count):
    """ Создаёт cписок с большим количеством имён, потом мы их разово добавляем в таблицу """
    data = []
    for _ in range(count):
        data.append((
            fake.first_name(),  # Случайное имя
            fake.country(),  # Рандомная странна
            fake.email(),  # Рандомная почта
            fake.date_of_birth(minimum_age=18, maximum_age=80),  # Рандомный возраст от - до
            fake.phone_number()  # Рандомный номер
        ))
    return data


async def setup_database(db_path):
    """ Асинхронное подключение к нашей таблице """
    conn = await aiosqlite.connect(database=db_path)

    await conn.execute('''
        CREATE TABLE IF NOT EXISTS users_db(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            country TEXT,
            email TEXT,
            date_of_birthday TEXT,
            number TEXT
        );
    ''')  # -- Создание таблицы если нету

    # wal (Write-Ahead Logging)
    await conn.execute("PRAGMA journal_mode = WAL;")
    # -- Увеличиваем размер кэша
    await conn.execute("PRAGMA cache_size = 10000000;")
    await conn.commit()
    print("[SQLite] База данных готова к работе.")
    return conn


async def insert_data_loop(conn):
    """ Цикл вставки данных """
    global total_inserted_count, start_time
    print(f"[INFO] Начало вставки данных пачками по {BATCH_SIZE} строк.")
    start_time = time.time()  # Текущее время для подсчета

    try:
        while True:
            #  Создаем список:)
            data_batch = generate_fake_data_batch(BATCH_SIZE)

            # Вставляем весь большой список разом
            await conn.executemany('''
                INSERT INTO users_db (name, country, email, date_of_birthday, number)
                VALUES (?, ?, ?, ?, ?)
            ''', data_batch)

            # Применяем изменения после добавки списка
            await conn.commit()

            total_inserted_count += BATCH_SIZE
            # Сколько вставили ну прогресс
            if total_inserted_count % (BATCH_SIZE * 10) == 0:
                elapsed_time = time.time() - start_time  #  Текущее время для подсчета
                speed = total_inserted_count / elapsed_time if elapsed_time > 0 else 0
                print(f"[INFO] Вставлено всего: {total_inserted_count} записей. Скорость: {speed:.0f} записей/сек.")


    except KeyboardInterrupt:
        print("\n[WARN] Процесс вставки остановлен пользователем (Ctrl+C).")
    except Exception as e:
        print(f'[ERROR] Привет! Произошла ошибка: {e}')
        # Откатываем транзакцию в случае ошибки перед закрытием
        await conn.rollback()
    finally:
        await conn.close()
        print("[WARN] Соединение с базой данных SQLite3 закрыто.")


async def main():
    """Основная асинхронная функция запуска."""
    connection = await setup_database(DB_PATH)
    await insert_data_loop(connection)


if __name__ == "__main__":
    try:
        asyncio.run(main())  # Запуск
    except KeyboardInterrupt:
        # При выходе например ctrl + c
        pass
    except Exception as e:
        print(f'[ERROR] {e}')
