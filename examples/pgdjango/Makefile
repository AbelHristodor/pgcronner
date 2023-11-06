
.PHONY: build run start stop rm logs sh

DB_NAME=pgcron-django-db

build:
	docker build -t pgcron-django-db -f ./db/Dockerfile .
run:
	docker run -e POSTGRES_HOST_AUTH_METHOD=trust -p 5432:5432 --expose 5432 --name $(DB_NAME) -it pgcron-django-db
start:
	docker start $(DB_NAME)
stop:
	docker stop $(DB_NAME)
rm:
	docker rm $(DB_NAME)
logs:
	docker logs --follow $(DB_NAME)
sh:
	docker exec -it $(DB_NAME) psql -U postgres -h localhost
