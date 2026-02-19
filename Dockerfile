FROM jupyter/pyspark-notebook:latest
COPY . /home/jovyan/work
WORKDIR /home/jovyan/work
RUN pip install -r requirements.txt