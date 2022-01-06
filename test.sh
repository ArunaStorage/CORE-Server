docker-compose --file docker-compose.test.yml build
docker-compose --file docker-compose.test.yml run --rm cockroach-test
docker-compose --file docker-compose.test.yml rm -s -f
docker-compose --file docker-compose.test.yml run --rm postgres-test
docker-compose --file docker-compose.test.yml rm -s -f