import os
import glob
from airflow.sensors.filesystem import FileSensor

class CustomFileSensor(FileSensor):
    def poke(self, context):
        file_found = super().poke(context)
        if file_found:
            file_list = glob.glob(self.filepath)
            
            # Фильтруем только файлы, исключая каталоги
            file_list = [f for f in file_list if os.path.isfile(f)]
            
            if file_list:
                file_list.sort(key=lambda f: os.stat(f).st_mtime)  # Сортируем по времени
                file_path = file_list[-1]  # Берем самый новый файл
                context["task_instance"].xcom_push(key="file_path", value=file_path)
                return True
        return False