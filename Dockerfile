FROM python:3
RUN git clone https://github.com/hj22215/dockerPythonTest.git
RUN pip install -r /dockerPythonTest/requirements.txt
