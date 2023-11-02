
py:
	maturin develop


build:
	docker build -t pgcronner-db -f ./db/Dockerfile .
run:
	 docker run -e POSTGRES_HOST_AUTH_METHOD=trust -p 5432:5432 --expose 5432 --name pgcronner-db -it pgcronner-db
start:
	docker start pgcronner-db
stop:
	docker stop pgcronner-db
rm:
	docker rm pgcronner-db
logs:
	docker logs --follow pgcronner-db
sh:
	docker exec -it pgcronner-db psql -U postgres -h localhost
