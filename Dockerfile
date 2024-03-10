FROM apache/airflow:2.8.2
COPY requirements.txt /requirements.txt 
RUN pip install --user --upgrade pip 
RUN pip install --user --no-cache-dir -r /requirements.txt