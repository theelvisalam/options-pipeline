.PHONY: reset up down ps logs

reset:
	docker-compose down -v
	docker-compose up --build

up:
	docker-compose up --build

down:
	docker-compose down -v

ps:
	docker-compose ps -a

rebuild:
	docker-compose up --build $(SERVICE)

logs:
	docker-compose logs -f $(SERVICE)
