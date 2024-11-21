FROM quay.io/quarkus/ubi-quarkus-mandrel:22.3-java17
LABEL authors="F378515"

ENTRYPOINT ["top", "-b"]