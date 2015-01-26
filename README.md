cloudfeeds-ballista
==========================



## How to Build
To build this component, we require:
* Gradle version 2.2 or above
* JDK 7
* Scala 2.10 or above


### Simple build
```
gradle clean build
```

## How to run the App
Run the build to create an installable app. Run the following command.

```
java -Dconfig.file=/<file path>/cloudfeeds-ballista.conf -jar build/libs/cloudfeeds-ballista-<version>.jar
```

## Configuration

The Cloud Feeds Ballista app uses the following configuration files:

### cloudfeeds-ballista.conf
This file configures the cloudfeeds-ballista.conf app itself. 

By default, the cloudfeeds-ballista app will try to find this file from classpath. This can be overriden by specifying the Java System properties ```-Dconfig.file=<path_to_file>```.

### core-site.xml
This file has the hadoop configuration.

By default, the cloudfeeds-ballista app will try to load this file from classpath. This can be overriden by editing the ```cloudfeeds-ballista.conf``` file above.

### hdfs-site.xml
This file has the HDFS configuration.

By default, the cloudfeeds-ballista app will try to load this file from classpath. This can be overriden by editing the ```cloudfeeds-ballista.conf``` file above.

### logback.xml
This file has the logging related configuration.

By default, the cloudfeeds-ballista app will try to load this file from classpath. This can be overriden by editing the ```cloudfeeds-ballista.conf``` file above.
