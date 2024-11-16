from pathlib import Path
import sys
from pyspark.sql import SparkSession
from functools import reduce
import pyspark.sql.functions as funk
from pyspark.sql.window import Window

# Получение аргументов (пути к директориям с данными)
input_path = Path(sys.argv[1])  # входные данные
output_path = Path(sys.argv[2])  # выходные данные

csv_crime_path = (input_path / 'crime.csv').as_posix()
csv_offices_path = (input_path / 'offense_codes.csv').as_posix()
output_path = output_path.as_posix()

# Проверка существования файлов
if not Path(csv_crime_path).exists():
    raise FileNotFoundError(f'File not found: {csv_crime_path}')
if not Path(csv_offices_path).exists():
    raise FileNotFoundError(f'File not found: {csv_offices_path}')

# Создание SparkSession
spark = SparkSession.builder \
    .appName('CrimeData') \
    .getOrCreate()

# Чтение CSV
crime_data = spark.read.csv(csv_crime_path, header=True, inferSchema=True)

# Вывод первых строк данных
# crime_data.show(5)

# прочитаем схему данных
# crime_data.printSchema()

# Удаление дубликатов
# print(f'Количество строк ДО удаления дубликатов: {crime_data.count()}')
crime_data = crime_data.dropDuplicates()
# print(f'Количество строк ПОСЛЕ удаления дубликатов: {crime_data.count()}')

# Проверить наличие пропущенных значений
missing_data = crime_data.select([funk.col(c).isNull().alias(c) for c in crime_data.columns])
# missing_data_summary = missing_data.describe().show()

# Удалить строки с пропущенными критически важными данными
crime_data = crime_data.dropna(subset=['DISTRICT', 'OCCURRED_ON_DATE', 'Lat', 'Long'])

# Проверить наличие пропущенных значений
missing_data = crime_data.select([funk.col(c).isNull().alias(c) for c in crime_data.columns])
# missing_data_summary = missing_data.describe().show()

# Удалить строки с пропущенными критически важными данными
crime_data = crime_data.dropna(subset=['DISTRICT', 'OCCURRED_ON_DATE', 'Lat', 'Long'])

# Получаем данные из offices_codes.csv
offense_codes = spark.read.csv(csv_offices_path, header=True, inferSchema=True)

# Убираем дубликаты
offense_codes = offense_codes.dropDuplicates(['CODE'])

# Добавим колонку crime_type, содержащую первую часть строки из NAME (до первого разделителя ' - '):
offense_codes = offense_codes.withColumn('crime_type', funk.split(offense_codes['NAME'], ' - ').getItem(0))
# offense_codes.show(5)

# Присоединяем offense_codes к crime_data
crime_data = crime_data.join(offense_codes, crime_data['OFFENSE_CODE'] == offense_codes['CODE'], how='left')

# Сохраняем только нужные колонки
crime_data = crime_data.drop('CODE', 'NAME')
# crime_data.show(5)

# Просмотр первых строк обновлённого DataFrame
crime_data.select('DISTRICT', 'OFFENSE_CODE', 'crime_type', 'Lat', 'Long')
# crime_data.show(5)

# Общее количество преступлений (crimes_total)

crimes_total = crime_data.groupBy('DISTRICT').agg(funk.count('*').alias('crimes_total'))

# Медиана числа преступлений в месяц (crimes_monthly):

# Создаём колонку year_month для уникального идентификатора месяца
crime_data = crime_data.withColumn('year_month', funk.year('OCCURRED_ON_DATE') * 100 + funk.month('OCCURRED_ON_DATE'))

# Считаем количество преступлений в месяц для каждого района
monthly_crimes = crime_data.groupBy('DISTRICT', 'year_month').agg(funk.count('*').alias('monthly_crimes'))

# Считаем медиану числа преступлений в месяц для каждого района
crimes_monthly = monthly_crimes.groupBy('DISTRICT').agg(
    funk.percentile_approx('monthly_crimes', 0.5).alias('crimes_monthly')
)

# Три самых частых типа преступлений (frequent_crime_types):
# Считаем частоту каждого crime_type в районе
crime_types_count = crime_data.groupBy('DISTRICT', 'crime_type').count()

# Получаем топ-3 типов преступлений в районе
window = Window.partitionBy('DISTRICT').orderBy(funk.col('count').desc())

top_crime_types = crime_types_count.withColumn('rank', funk.row_number().over(window)) \
    .filter(funk.col('rank') <= 3) \
    .groupBy('DISTRICT').agg(
        funk.concat_ws(', ', funk.collect_list('crime_type')).alias('frequent_crime_types')
    )

# Построение витрины, подсчёт метрик

# crimes_total: общее количество преступлений в районе
crimes_total = crime_data.groupBy('DISTRICT').agg(funk.count('*').alias('crimes_total'))

# crimes_monthly: медиана числа преступлений в месяц
crime_data = crime_data.withColumn('year_month', funk.expr('YEAR * 100 + MONTH'))
crimes_monthly = crime_data.groupBy('DISTRICT', 'year_month').agg(funk.count('*').alias('monthly_crimes'))
crimes_median = crimes_monthly.groupBy('DISTRICT').agg(
    funk.percentile_approx('monthly_crimes', 0.5).alias('crimes_monthly')
)

# frequent_crime_types: три самых частых типа преступлений
frequent_crime_types = crime_data.groupBy('DISTRICT', 'crime_type').count() \
    .groupBy('DISTRICT').agg(
        funk.concat_ws(', ', funk.collect_list('crime_type')).alias('frequent_crime_types')
    )

# lat, lng: средние координаты
avg_coordinates = crime_data.groupBy('DISTRICT').agg(
    funk.mean('Lat').alias('lat'),
    funk.mean('Long').alias('lng')
)

# Объединение всех метрик
metrics = [crimes_total, crimes_monthly, top_crime_types, avg_coordinates]

# Объединяем все метрики по колонке 'DISTRICT'
final_aggregated_data = reduce(
    lambda df1, df2: df1.join(df2, on='DISTRICT', how='left'),
    metrics
)

# final_aggregated_data.show()

# Сохранение в формате .parquet
final_aggregated_data.write.mode('overwrite').parquet(output_path)

print('Done!')
