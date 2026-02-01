FROM python:3.10
WORKDIR /app
COPY consumer.py .
RUN pip install kafka-python matplotlib
CMD ["python", "consumer.py"]
