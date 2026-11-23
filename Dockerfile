FROM adoptopenjdk:8-jdk-openj9 AS GRADLE_BUILD
RUN mkdir -p ./build/libs/
RUN curl -L http://search.maven.org/remotecontent?filepath=org/jolokia/jolokia-jvm/1.6.2/jolokia-jvm-1.6.2-agent.jar -o ./jolokia-jvm-agent.jar
COPY ./ ./
RUN ./gradlew build

FROM openjdk:8-jre
COPY --from=GRADLE_BUILD ./build/libs/ /opt/aqueduct/bin
COPY --from=GRADLE_BUILD ./jolokia-jvm-agent.jar /opt/aqueduct
COPY --from=GRADLE_BUILD ./src/main/resources/log4j.xml /opt/aqueduct/etc/log4j.xml
COPY --from=GRADLE_BUILD ./src/main/resources/logback.xml /opt/aqueduct/etc/logback.xml
WORKDIR /opt/aqueduct
CMD ["java", "-cp", "bin/*:/work-dir/*", "org.pranay.aqueduct.launch.Main", "-server", "-Dlogback.configurationFile=etc/aqueduct/logback.xml", "-Xloggc:/var/log/aqueduct"]
