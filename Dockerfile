FROM public.ecr.aws/o2c0x5x8/application-base:go-mux-postgresql-gorm

WORKDIR /usr/src/app/src

ADD src ./

RUN CGO_ENABLED=0 GOOS=linux go build -o ./go-mux-postgresql-gorm

CMD ["./go-mux-postgresql-gorm"]