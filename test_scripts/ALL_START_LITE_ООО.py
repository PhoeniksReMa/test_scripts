import os
import time
import subprocess

scripts = [
    "(1)002.Tovar_info.py",
    "(1)003.Content_rating.py",
    "(1)006.Stock_FBO_FBS.py",
    "(1)007.Edet_na_FBO.py",
    "(1)013.Price_logistic.py",
]

for script in scripts:
    print(f"Запуск: {script}")
    
    if os.path.exists(script):
        try:
            # Простой запуск без перехвата вывода (чтобы избежать проблем с кодировкой)
            subprocess.run(['python', script], check=True)
            print("Успешно завершен")
        except:
            print("Завершен с ошибкой (проблемы с кодировкой вывода)")
    else:
        print("Файл не найден")
    
    if script != scripts[-1]:
        time.sleep(10)

print("Все скрипты выполнены")