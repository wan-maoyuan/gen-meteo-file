NAME := gen-meteo-file

VERSION := v0.0.1

# 目标输出目录
DIST_FOLDER := dist

# 版本构建目录
RELEASE_FOLDER := resources


build:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ./dist/gen-meteo-file ./cmd/gen-meteo-file


container:
	docker build -t nav-green/${NAME}:${VERSION} -f ./deploy/Dockerfile .;

restart-docker:
	docker compose -f deploy/compose/gen-meteo-file.yml down;
	docker rmi nav-green/${NAME}:${VERSION};
	docker build -t nav-green/${NAME}:${VERSION} -f ./deploy/Dockerfile .;
	docker compose -f deploy/compose/gen-meteo-file.yml up -d;

clean:
	-rm -rf ${DIST_FOLDER}
	-rm -rf api/**/*.go
	-go clean
	-go clean -cache
