
build:
	./mvnw clean package -DskipTests

build-docker:
	docker build -t kafka-admin-boot .

up:
	$(MAKE) -C kafka-env up

down:
	$(MAKE) -C kafka-env down

cleanup:
	$(MAKE) -C kafka-env cleanup

download-kafka:
	$(MAKE) -C kafka-env download-kafka

apache-latest:
	$(MAKE) -C kafka-env apache-latest