#!/usr/bin/env python3
# Audio Recorder with Cloud Upload and Live Streaming
import os
import sys
import time
import signal
import subprocess
import logging
import shutil
import threading
from pathlib import Path
from datetime import datetime
import schedule

# ========================
# Конфигурация
# ========================
OUTPUT_DIR = "/opt/arec_p"
PENDING_DIR = os.path.join(OUTPUT_DIR, "pending")
LOG_FILE = os.path.join(OUTPUT_DIR, "arec.log")

# Аудио
SPLIT_TIME = 1800          # секунд
SAMPLE_RATE = 48000        # критически важная опция для микрофона Boya BY-LM40
SAMPLE_FORMAT = "S24_3LE"  # критически важная опция для микрофона Boya BY-LM40
MIC = "default"
AUDIO_FORMAT = "opus"      # opus, aac, mp3
BITRATE = 64               # kbps
FILE_PREFIX = "REC"        # префикс для имён файлов

# Облако
CLOUD_SERVICE = "yandex"   # "google", "yandex", "none" # ← если не нужна выгрузка в облако
DELETE_AFTER_UPLOAD = True
RETRY_DELAY = 300
MAX_RETRIES = 15
SLOW_NETWORK_RETRY_DELAY = 600
SLOW_NETWORK_MAX_RETRIES = 5
NETWORK_SPEED_THRESHOLD = 100
MAX_PARALLEL_UPLOADS = 3
CONNECTIVITY_TIMEOUT = 10
CONNECTIVITY_CHECK_INTERVAL = 180

# Хранилище
MAX_STORAGE_MB = 40960
QUEUE_WARNING_THRESHOLD = 80

# Google Drive
GOOGLE_REMOTE = "google.drive"
GOOGLE_DIR = "/Recordings"

# Яндекс.Диск
YANDEX_REMOTE = "yandex.disk"
YANDEX_DIR = "/Recordings"

# Расписание (опционально)
RECORDING_SCHEDULE_ENABLED = True
RECORDING_START_HOUR = 6
RECORDING_END_HOUR = 22

# Веб-стриминг
ENABLE_WEB_STREAM = True
ICECAST_HOST = "127.0.0.1"
ICECAST_PORT = 8000
ICECAST_PASSWORD = "password"  # ← должен совпадать с <source-password> в icecast.xml
STREAM_BITRATE = 64          # kbps

# ========================
# Настройка логгера
# ========================
os.makedirs(OUTPUT_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("arec")

# ========================
# Глобальные переменные
# ========================
shutdown_event = threading.Event()
stream_process = None
in_schedule_mode = False

# ========================
# Вспомогательные функции
# ========================

def run_cmd(cmd, quiet=True, timeout=None):
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        if not quiet:
            logger.debug(f"CMD: {cmd} → exit={result.returncode}")
        return result.returncode, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        logger.error(f"Команда превысила таймаут: {cmd}")
        return -1, "", "timeout"

def get_cloud_target():
    if CLOUD_SERVICE == "google":
        return f"{GOOGLE_REMOTE}:{GOOGLE_DIR}"
    elif CLOUD_SERVICE == "yandex":
        return f"{YANDEX_REMOTE}:{YANDEX_DIR}"
    return None

def get_cloud_name():
    return {
        "google": "Google Drive",
        "yandex": "Яндекс.Диск",
        "none": "локальное хранилище"
    }.get(CLOUD_SERVICE, "неизвестно")

def dir_size_mb(path):
    if not os.path.exists(path):
        return 0
    total = sum(f.stat().st_size for f in Path(path).rglob('*') if f.is_file())
    return total // (1024 * 1024)

def cleanup_temp_files():
    for marker in Path(OUTPUT_DIR).glob("*.recording"):
        marker.unlink(missing_ok=True)
    logger.info("Временные файлы очищены")

def graceful_shutdown(signum, frame):
    logger.info(f"Получен сигнал {signum}, завершение работы...")
    shutdown_event.set()
    global stream_process
    if stream_process and stream_process.poll() is None:
        stream_process.terminate()
        try:
            stream_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            stream_process.kill()
    cleanup_temp_files()
    pending_count = len(list(Path(PENDING_DIR).glob(f"{FILE_PREFIX}_*.{AUDIO_FORMAT}")))
    if pending_count:
        logger.info(f"Завершение с {pending_count} файлами в очереди")
    logger.info("Корректное завершение работы")
    sys.exit(0)

def sync_time():
    if any(Path("/dev").glob("rtc*")):
        code, _, _ = run_cmd("hwclock --hctosys", quiet=False)
        if code == 0:
            logger.info("Время синхронизировано с RTC")
        else:
            logger.warning("Ошибка синхронизации с RTC")
    else:
        logger.warning("RTC не найден")

    if check_internet_access():
        code, _, _ = run_cmd("ntpdate -s time.nist.gov", quiet=False)
        if code == 0:
            logger.info("Время синхронизировано через NTP")
            if any(Path("/dev").glob("rtc*")):
                run_cmd("hwclock --systohc")
        else:
            logger.warning("Ошибка синхронизации через NTP")
    else:
        logger.warning("Нет интернета для синхронизации времени")

def setup_mic():
    logger.info("Проверка микрофона...")
    cmd = f'arecord -D "{MIC}" -f {SAMPLE_FORMAT} -r {SAMPLE_RATE} -d 1 --quiet - >/dev/null'
    code, _, _ = run_cmd(cmd)
    if code != 0:
        logger.error(f"Ошибка доступа к микрофону {MIC}")
        sys.exit(1)
    logger.info(f"Микрофон настроен: {MIC}")

def check_internet_access():
    if CLOUD_SERVICE == "none":
        return True
    code, _, _ = run_cmd(f"ping -c 1 -W {CONNECTIVITY_TIMEOUT} 8.8.8.8")
    if code != 0:
        return False
    cloud_target = get_cloud_target().split(':')[0]
    cmd = f"timeout {CONNECTIVITY_TIMEOUT * 2} rclone about {cloud_target}:"
    code, _, _ = run_cmd(cmd)
    return code == 0

def check_network_speed():
    code, out, _ = run_cmd("ping -c 3 8.8.8.8", quiet=True)
    if code != 0:
        return "unknown"
    try:
        avg = float(out.split("avg = ")[1].split("/")[1])
        return "slow" if avg > NETWORK_SPEED_THRESHOLD else "fast"
    except:
        return "unknown"

def recover_interrupted_files():
    logger.info("Восстановление файлов после сбоя...")
    recovered = corrupted = 0
    pattern = f"{FILE_PREFIX}_*.{AUDIO_FORMAT}"
    for f in Path(OUTPUT_DIR).glob(pattern):
        if f.stat().st_size > 1024:
            target = Path(PENDING_DIR) / f.name
            f.rename(target)
            recovered += 1
            logger.info(f"Восстановлен: {f.name}")
        else:
            f.unlink()
            corrupted += 1
            logger.info(f"Удалён неполный файл: {f.name}")
    for marker in Path(OUTPUT_DIR).glob("*.recording"):
        marker.unlink()
    logger.info(f"Восстановление завершено: +{recovered}, -{corrupted}")

def start_recording(file_path: str):
    marker = Path(str(file_path) + ".recording")
    marker.write_text(f"{datetime.now()} - Запись начата")
    try:
        if AUDIO_FORMAT == "opus":
            encoder = f"ffmpeg -y -i - -c:a libopus -b:a {BITRATE}k -application voip -ac 1 '{file_path}' -hide_banner -loglevel error"
        elif AUDIO_FORMAT == "aac":
            encoder = f"ffmpeg -y -i - -c:a aac -b:a {BITRATE}k -ac 1 '{file_path}' -hide_banner -loglevel error"
        elif AUDIO_FORMAT == "mp3":
            encoder = f"ffmpeg -y -i - -c:a libmp3lame -b:a {BITRATE}k -q:a 5 -ac 1 '{file_path}' -hide_banner -loglevel error"
        else:
            raise ValueError(f"Неподдерживаемый формат: {AUDIO_FORMAT}")

        arecord_cmd = f'arecord -D "{MIC}" -f {SAMPLE_FORMAT} -r {SAMPLE_RATE} -d {SPLIT_TIME} --quiet -'
        full_cmd = f"{arecord_cmd} 2>> {LOG_FILE} | {encoder} 2>> {LOG_FILE}"
        code, _, _ = run_cmd(full_cmd, timeout=SPLIT_TIME + 10)
        if code != 0:
            logger.error(f"Ошибка записи: {file_path}")
            return False
        logger.info(f"Запись завершена: {file_path}")
        return True
    finally:
        marker.unlink(missing_ok=True)

def upload_to_cloud(file_path: str):
    if CLOUD_SERVICE == "none":
        return True
    if not os.path.exists(file_path):
        logger.error(f"Файл не найден: {file_path}")
        return False
    if not check_internet_access():
        return False

    network = check_network_speed()
    max_retries = SLOW_NETWORK_MAX_RETRIES if network == "slow" else MAX_RETRIES
    retry_delay = SLOW_NETWORK_RETRY_DELAY if network == "slow" else RETRY_DELAY

    cloud_target = get_cloud_target()
    for attempt in range(1, max_retries + 1):
        code, _, _ = run_cmd(f"rclone copy '{file_path}' '{cloud_target}' --quiet")
        if code == 0:
            logger.info(f"Успешно загружено: {file_path}")
            if DELETE_AFTER_UPLOAD:
                Path(file_path).unlink(missing_ok=True)
                logger.info(f"Файл удалён: {file_path}")
            return True
        logger.warning(f"Попытка {attempt}/{max_retries} не удалась: {file_path}")
        if attempt < max_retries:
            time.sleep(retry_delay)
    logger.error(f"Загрузка провалена после {max_retries} попыток: {file_path}")
    return False

def queue_for_upload(file_path: str):
    pending_path = Path(PENDING_DIR) / Path(file_path).name
    try:
        Path(file_path).rename(pending_path)
        logger.info(f"Файл добавлен в очередь: {pending_path}")
        return True
    except Exception as e:
        logger.error(f"Ошибка перемещения в очередь: {e}")
        return False

def process_upload_queue():
    if CLOUD_SERVICE == "none":
        return
    if not check_internet_access():
        pattern = f"{FILE_PREFIX}_*.{AUDIO_FORMAT}"
        pending_count = len(list(Path(PENDING_DIR).glob(pattern)))
        logger.info(f"Нет интернета. Файлов в очереди: {pending_count}")
        return

    pending_size = dir_size_mb(PENDING_DIR)
    if pending_size > MAX_STORAGE_MB:
        logger.warning("Превышен лимит хранилища, очистка...")
        pattern = f"{FILE_PREFIX}_*.{AUDIO_FORMAT}"
        files = sorted(Path(PENDING_DIR).glob(pattern), key=lambda x: x.stat().st_mtime)
        for f in files:
            if dir_size_mb(PENDING_DIR) <= MAX_STORAGE_MB:
                break
            f.unlink()
            logger.info(f"Удалён старый файл из очереди: {f}")

    pattern = f"{FILE_PREFIX}_*.{AUDIO_FORMAT}"
    files = sorted(Path(PENDING_DIR).glob(pattern), key=lambda x: x.stat().st_mtime, reverse=True)[:10]
    if not files:
        return

    network = check_network_speed()
    max_workers = 1 if network == "slow" else MAX_PARALLEL_UPLOADS
    logger.info(f"Обработка очереди ({len(files)} файлов, сеть: {network}, потоков: {max_workers})")

    threads = []
    for f in files[:max_workers]:
        t = threading.Thread(target=upload_to_cloud, args=(str(f),), daemon=True)
        t.start()
        threads.append(t)
    for t in threads:
        t.join()

def process_recorded_file(file_path: str):
    if not os.path.exists(file_path):
        return
    if Path(file_path).stat().st_size < 1024:
        Path(file_path).unlink()
        logger.warning(f"Файл слишком маленький, удалён: {file_path}")
        return

    if CLOUD_SERVICE != "none":
        if check_internet_access() and upload_to_cloud(file_path):
            return
        else:
            queue_for_upload(file_path)

def in_recording_schedule():
    if not RECORDING_SCHEDULE_ENABLED:
        return True
    now = datetime.now().hour
    return RECORDING_START_HOUR <= now < RECORDING_END_HOUR

def start_web_stream():
    global stream_process
    if not ENABLE_WEB_STREAM:
        return

    cmd = (
        f'arecord -D "{MIC}" -f {SAMPLE_FORMAT} -r {SAMPLE_RATE} -c 1 - | '
        f'ffmpeg -f wav -i - -f mp3 -codec:a libmp3lame -b:a {STREAM_BITRATE}k '
        f'-content_type "audio/mpeg" '
        f'icecast://source:{ICECAST_PASSWORD}@{ICECAST_HOST}:{ICECAST_PORT}/live.mp3'
    )
    logger.info("Запуск веб-стрима...")
    stream_process = subprocess.Popen(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def check_and_cleanup_storage():
    """Проверяет и очищает хранилище независимо от интернета"""
    pending_size = dir_size_mb(PENDING_DIR)
    if pending_size > MAX_STORAGE_MB:
        logger.warning(f"Превышен лимит хранилища ({pending_size}MB > {MAX_STORAGE_MB}MB), принудительная очистка...")
        pattern = f"{FILE_PREFIX}_*.{AUDIO_FORMAT}"
        files = sorted(Path(PENDING_DIR).glob(pattern), key=lambda x: x.stat().st_mtime)
        for f in files:
            if dir_size_mb(PENDING_DIR) <= MAX_STORAGE_MB:
                break
            f.unlink()
            logger.info(f"Удалён старый файл из очереди: {f}")

# ========================
# Основной цикл
# ========================

def main_loop():
    global in_schedule_mode
    last_connectivity_check = 0
    last_schedule_log = 0

    while not shutdown_event.is_set():
        current_time = time.time()

        # === Проверка расписания ===
        if RECORDING_SCHEDULE_ENABLED:
            if in_recording_schedule():
                if not in_schedule_mode:
                    logger.info(f"Начало записи по расписанию ({RECORDING_START_HOUR}:00–{RECORDING_END_HOUR}:00)")
                    in_schedule_mode = True
            else:
                if in_schedule_mode or (current_time - last_schedule_log > 300):  # Лог каждые 5 минут вне расписания
                    logger.info(f"Вне расписания записи ({RECORDING_START_HOUR}:00–{RECORDING_END_HOUR}:00). Ожидание...")
                    in_schedule_mode = False
                    last_schedule_log = current_time
                time.sleep(60)
                continue

        # === Периодическая проверка интернета ===
        if current_time - last_connectivity_check >= CONNECTIVITY_CHECK_INTERVAL:
            last_connectivity_check = current_time
            if CLOUD_SERVICE != "none":
                threading.Thread(target=process_upload_queue, daemon=True).start()

        # === Проверка и очистка хранилища (важно: даже без интернета!) ===
        check_and_cleanup_storage()

        # === Запись аудио ===
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = os.path.join(OUTPUT_DIR, f"{FILE_PREFIX}_{ts}.{AUDIO_FORMAT}")
        logger.info(f"Начало записи: {file_path}")
        if start_recording(file_path):
            process_recorded_file(file_path)
            logger.info(f"✅ Запись завершена: {file_path}")
        else:
            logger.error("Не удалось выполнить запись")

        if shutdown_event.is_set():
            break

# ========================
# Точка входа
# ========================

def main():
    signal.signal(signal.SIGINT, graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(PENDING_DIR, exist_ok=True)

    if not os.access(OUTPUT_DIR, os.W_OK) or not os.access(PENDING_DIR, os.W_OK):
        logger.critical("Нет прав на запись в директории")
        sys.exit(1)

    sync_time()
    setup_mic()
    recover_interrupted_files()

    cloud_name = get_cloud_name()
    logger.info(f"▶ Запуск записи в формате {AUDIO_FORMAT} с выгрузкой на {cloud_name}")

    if ENABLE_WEB_STREAM:
        start_web_stream()

    try:
        main_loop()
    except KeyboardInterrupt:
        graceful_shutdown(signal.SIGINT, None)

if __name__ == "__main__":
    main()
