SHELL := /bin/bash


all:  build

build:
	docker compose --env-file config.env up

stop:
	docker compose --env-file config.env stop
