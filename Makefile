.PHONY: package publish test

VERSION       ?= SNAPSHOT
ENVIRONMENT   ?= local
WORKING_DIR   ?= $(shell pwd)
SERVICE_NAME  ?= discover
LAMBDA_NAME   ?= s3clean
LAMBDA_BUCKET ?= "pennsieve-cc-lambda-functions-use1"
PACKAGE_BASE  ?= ${SERVICE_NAME}-${LAMBDA_NAME}
PACKAGE_NAME  ?= ${PACKAGE_BASE}-${VERSION}.zip
PACKAGE_ZIP   ?= $(WORKING_DIR)/$(PACKAGE_NAME)

clean:
	docker compose down --volumes

test:
	@echo "Testing..."
	docker compose stop
	docker compose rm -f
	docker compose build test
	docker compose up --exit-code-from test test
	docker compose stop

package:
	@echo "Building lambda..."
	docker build --target build --tag s3clean-build:latest .
	docker cp "$$(docker create s3clean-build:latest):/lambda/lambda.zip" ${PACKAGE_ZIP}

publish: package
	@echo "Publishing lambda to S3..."
	aws s3 cp ${PACKAGE_ZIP} s3://${LAMBDA_BUCKET}/${PACKAGE_BASE}/
