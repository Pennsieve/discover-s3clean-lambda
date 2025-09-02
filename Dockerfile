FROM amazonlinux:latest AS build

RUN yum -y install git \
    findutils \
    python39 \
    python-pip \
    zip \
    unzip && \
    yum clean all

RUN python3 -m pip install boto3==1.40.21

WORKDIR lambda
RUN mkdir bin

COPY requirements.txt .
RUN python3 -m pip install -r requirements.txt --target .
RUN find . -name "*.pyc" -delete

COPY main.py .
RUN zip -r lambda.zip .


FROM amazonlinux:latest AS test

RUN yum -y install git \
    findutils \
    python39 \
    python-pip \
    zip && \
    yum clean all

RUN python3 -m pip install boto3==1.40.21

WORKDIR lambda

COPY requirements-test.txt .
RUN python3 -m pip install -r requirements-test.txt

COPY --from=build lambda .

COPY test.py .
COPY test.txt .
COPY pytest.ini .

CMD ["python3", "-m", "pytest", "-s", "test.py"]
