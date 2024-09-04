
up:
	docker-compose up

cleanup:
	docker-compose rm --force --stop

apache-latest:
	docker run -d --name broker \
     	 -e KAFKA_NODE_ID=1 \
     	 -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP='CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT' \
    	 -e KAFKA_ADVERTISED_LISTENERS='PLAINTEXT_HOST://localhost:9092,PLAINTEXT://broker:19092' \
    	 -e KAFKA_PROCESS_ROLES='broker,controller' \
    	 -e KAFKA_CONTROLLER_QUORUM_VOTERS='1@broker:29093' \
    	 -e KAFKA_LISTENERS='CONTROLLER://:29093,PLAINTEXT_HOST://:9092,PLAINTEXT://:19092' \
    	 -e KAFKA_INTER_BROKER_LISTENER_NAME='PLAINTEXT' \
    	 -e KAFKA_CONTROLLER_LISTENER_NAMES='CONTROLLER' \
    	 -e CLUSTER_ID='4L6g3nShT-eMCtK--X86sw' \
    	 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    	 -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
    	 -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
    	 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
    	 -e KAFKA_LOG_DIRS='/tmp/kraft-combined-logs' \
    	 -p 9092:9092 \
    	 -p 19092:19092 \
    	 -p 29093:29093 \
    	 -h broker \
    	 apache/kafka:latest