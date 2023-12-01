FROM ubuntu:latest
LABEL authors="ia"

ENTRYPOINT ["top", "-b"]