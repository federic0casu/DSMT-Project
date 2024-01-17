start:
	systemctl --user start docker-desktop.service

stop:
	systemctl --user stop docker-desktop.service

up: 
	docker compose -f docker-compose.yml up --force-recreate --build --remove-orphans

down: 
	docker compose -f docker-compose.yml down
