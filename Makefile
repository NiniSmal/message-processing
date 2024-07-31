db:
	docker run --name postgres -p 8021:5432 -e POSTGRES_PASSWORD=dev -e POSTGRES_DB=postgres -d --restart=always --network app postgres:15.6

kafka:
	docker run -p 9092:9092 --network app apache/kafka:3.8.0

d_build:
	docker build -t message-processing:local .

d_run:
	docker rm -f messages && docker run --name messages  --network app -p 8081:8083 -v $(pwd)/config-docker.yaml:/app/config.yaml:ro -d message-processing:local

d_rm:
	docker rm -f messages