FROM maven:3.9-eclipse-temurin-17 AS build

WORKDIR /build
COPY spring-api/pom.xml .
COPY spring-api/src ./src
RUN mvn -q -DskipTests package

FROM eclipse-temurin:17-jre

WORKDIR /app
COPY --from=build /build/target/stock-api-0.0.1-SNAPSHOT.jar /app/app.jar

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
