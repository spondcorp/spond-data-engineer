FROM apache/spark-py:3.3.1

ENV PYSPARK_MAJOR_PYTHON_VERSION=3

USER root
WORKDIR /usr

COPY requirements.txt .
RUN pip3 install --user -r requirements.txt

COPY /coding-questions ./coding-questions
COPY /datalake ./datalake

CMD ["python3", "coding-questions/2-1/usa_admins_emails.py"]
CMD ["python3", "coding-questions/2-2/scd2.py"]
