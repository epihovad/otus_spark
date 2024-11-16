# Сборка витрины на Spark

## Настойка системы

Для работы программы были установлены:
- [Java SE Development Kit 8 Update 231](https://github.com/hmsjy2017/get-jdk/releases/download/v8u231/jdk-8u231-windows-x64.exe)
- [Spark 3.5.3](https://spark.apache.org/downloads.html)
- winutils.exe + hadoop.dll ([версия hadoop-3.6.6](https://github.com/cdarlint/winutils/tree/master/hadoop-3.3.6/bin))

## Установка локального окружения для Python

```
python -m venv venv
source venv/Scripts/activate
pip install pyspark
```

### Запуск

Для пошагового изучения можно поспользоваться скриптом <a href="PySpark.ipunb">PySpark.ipunb</a>

Для запуска через Python:
```
python main.py 'd:\otus_spark\data\input' 'd:\otus_spark\data\output'
```

Для запуска через spark-submit:
```
spark-submit main.py 'd:\otus_spark\data\input' 'd:\otus_spark\data\output'
```