start:
	systemctl --user start docker-desktop.service

stop:
	systemctl --user stop docker-desktop.service

up: 
	docker compose -f ./docker/docker-compose.yml up --force-recreate --build

down: 
	docker compose -f ./docker/docker-compose.yml down
