# 程序编译镜像
FROM golang:1.24 AS build

ENV GO111MODULE=on                               \
    GOPROXY=https://goproxy.cn,direct

WORKDIR /go/release

ADD . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ./dist/gen-meteo-file ./cmd/gen-meteo-file

# 构建镜像
FROM alpine:3.20 AS prod

ENV TZ=Asia/Shanghai                                        \
    DEBIAN_FRONTEND=noninteractive                          \
    GIN_MODE=release                                        \
    LOG_FILE=""                                             \
    LOG_LEVEL="info"                                        \
    LOG_SIZE=20                                             \
    LOG_AGE=10                                              \
    LOG_BACKUPS=5                                           \
    NC_DIR="/nc-files"                                       \
    CSV_DIR="/csv-files"

# 在build阶段复制时区到
COPY --from=build /go/release/dist/gen-meteo-file    /gen-meteo-file

CMD [ "/gen-meteo-file"]