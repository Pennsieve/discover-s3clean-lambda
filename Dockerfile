FROM amazonlinux:latest AS build

RUN yum -y install git \
    findutils \
    python3.12 \
    python3.12-pip \
    zip \
    unzip && \
    yum clean all


# python3 points to the system python which now is 3.9, not the python 3.12 we just installed
RUN python3.12 -m pip install boto3==1.40.21

WORKDIR lambda
RUN mkdir bin

COPY requirements.txt .
RUN python3.12 -m pip install -r requirements.txt --target .
RUN find . -name "*.pyc" -delete

COPY main.py .
RUN zip -r lambda.zip .


FROM amazonlinux:latest AS test

RUN yum -y install git \
    findutils \
    python3.12 \
    python3.12-pip \
    zip && \
    yum clean all


RUN python3.12 -m pip install boto3==1.40.21

WORKDIR lambda

COPY requirements-test.txt .
RUN python3.12 -m pip install -r requirements-test.txt

COPY --from=build lambda .

COPY test.py .
COPY test.txt .

CMD ["python3.12", "-m", "pytest", "-s", "test.py"]
