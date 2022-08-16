
build:
	./mvnw clean package -DskipTests

build-docker:
	docker build -t kafka-admin-boot .