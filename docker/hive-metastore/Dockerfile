# Hive 3.1.3 runs on Java 8 only, so the metastore gets its own image.
FROM apache/hive:3.1.3
USER root
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar /opt/hive/lib/postgresql.jar
RUN chmod 644 /opt/hive/lib/postgresql.jar
COPY metastore-entrypoint.sh /metastore-entrypoint.sh
RUN chmod +x /metastore-entrypoint.sh
USER hive
ENTRYPOINT ["/metastore-entrypoint.sh"]
