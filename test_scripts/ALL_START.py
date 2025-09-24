import os
import time
import subprocess

scripts = [
    "(1)001.Product_offer_id.py",
    "(1)002.Tovar_info.py",
    "(1)003.Content_rating.py",
    "(1)004.Hranenie(xls).py",
    "(1)005.FBO_Oborachivaemost.py",
    "(1)006.Stock_FBO_FBS.py",
    "(1)007.Edet_na_FBO.py",
    "(1)008.FBO_Upravlenie_stocks.py",
    "(1)009.Zakazi_FBO_no_offset_180_days.py",
    "(1)010.Zakazi_FBO_no_old_year.py",
    "(1)011.Zakazi_FBS_no_offset.py",
    "(1)012.Zakazi_FBS_old_year.py",
    "(1)013.Price_logistic.py",
    "(1)014.Analytics_base.py",
    "(1)015.Analytic_date_premium.py"
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