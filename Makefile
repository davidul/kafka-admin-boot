
build:
	./mvnw clean package -DskipTests
	docker build -t kafka-admin-boot .