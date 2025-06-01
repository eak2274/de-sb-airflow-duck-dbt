import os

def print_tree(start_path, exclude_folder):
    for root, dirs, files in os.walk(start_path):
        level = root.replace(start_path, "").count(os.sep)
        indent = " " * 4 * level

        folder_name = os.path.basename(root)
        
        if root.endswith(exclude_folder):  # Если это airflow\venv, просто выводим название папки
            print(f"{indent}{folder_name}/")
            continue

        print(f"{indent}{folder_name}/")  # Вывод названия папки

        sub_indent = " " * 4 * (level + 1)
        for file in files:
            print(f"{sub_indent}{file}")  # Вывод вложенных файлов

# Задаем путь к каталогу и папку для исключения
start_path = "D:\\Dev\\pet\\de-basket-euroleague-pipeline"
exclude_folder = "airflow\\venv"

print_tree(start_path, exclude_folder)