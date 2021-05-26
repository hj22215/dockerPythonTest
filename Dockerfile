FROM python:3
RUN git clone https://github.com/hj22215/dockerPythonTest.git
WORKDIR "/dockerPythonTest"
RUN pip install -r requirements.txt
