# spark-etl-utilities
Spark ETL Utilities

### Build JAR
1. Clone repo
2. Build package
    ```sh
    cd /root/of/library && sbt clean package
    ```
3. Make sure to include depdendencies in [__build.sbt__](https://github.com/zaksamalik/spark-etl-utilities/blob/develop/build.sbt)

   (Optional) Build fat JAR containing all required dependencies    
    ```sh
    <project>
        <modelVersion>4.0.0</modelVersion>

        <groupId>com.civicboost.spark.etl.utilities</groupId>
        <artifactId>spark-etl-utilities-dependencies</artifactId>
        <version>0.1</version>
        <packaging>jar</packaging>

        <dependencies>
            <!-- https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws -->
            <dependency>
                <groupId>org.apache.hadoop</groupId>
                <artifactId>hadoop-aws</artifactId>
                <version>2.7.3</version>
                <exclusions>
                    <exclusion>
                        <groupId>*</groupId>
                        <artifactId>*</artifactId>
                    </exclusion>
                </exclusions>
            </dependency>
            <!-- https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk -->
            <dependency>
                <groupId>com.amazonaws</groupId>
                <artifactId>aws-java-sdk</artifactId>
                <version>1.7.4.2</version>
                <exclusions>
                    <exclusion>
                        <groupId>*</groupId>
                        <artifactId>*</artifactId>
                    </exclusion>
                </exclusions>
            </dependency>
            <!-- https://mvnrepository.com/artifact/com.google.guava/guava -->
            <dependency>
                <groupId>com.google.guava</groupId>
                <artifactId>guava</artifactId>
                <version>27.0.1-jre</version>
                <exclusions>
                    <exclusion>
                        <groupId>*</groupId>
                        <artifactId>*</artifactId>
                    </exclusion>
                </exclusions>
            </dependency>
        </dependencies>

        <build>
            <plugins>
                <plugin>
                    <groupId>org.apache.maven.plugins</groupId>
                    <artifactId>maven-assembly-plugin</artifactId>
                    <version>3.1.1</version>
                    <configuration>
                        <descriptorRefs>
                            <descriptorRef>jar-with-dependencies</descriptorRef>
                        </descriptorRefs>
                        <appendAssemblyId>false</appendAssemblyId>
                    </configuration>
                    <executions>
                        <execution>
                            <id>make-assembly</id>
                            <phase>package</phase>
                            <goals>
                                <goal>single</goal>
                            </goals>
                        </execution>
                    </executions>
                </plugin>
            </plugins>
        </build>
    </project>
    ```
