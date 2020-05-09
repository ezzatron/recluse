FROM node:12-alpine
COPY . /app
WORKDIR /app/example/bank/bin
ENTRYPOINT ["./run"]
