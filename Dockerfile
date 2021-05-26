FROM python:3
RUN git clone https://github.com/hj22215/dockerPythonTest.git
RUN pip install mysqlclient
RUN pip install sqlalchemy
RUN pip install pandas==0.22.0
RUN pip install odo
RUN pip install pytz
