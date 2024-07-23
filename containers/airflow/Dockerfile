FROM apache/airflow:latest
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

# COPY quarto.sh /
# RUN cd / && bash /quarto.sh