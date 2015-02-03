cloudfeeds-ballista
==========================

A command line utility to migrate data from RDBMS database to HDFS.


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
java -Dconfig.file="/<file path>/cloudfeeds-ballista.conf" -Dlogback.configurationFile="/<file path>/logback.xml"-jar build/libs/cloudfeeds-ballista-<version>.jar
```

## Command line options

  -d <value> | --runDate <value>
         runDate is a date in the format yyyy-MM-dd. Data belonging to this date will be exported. Default value is yesterday.           
  -n <value> | --dbNames <value>
         dbNames is comma separated list of database names to be exported. Default value is all the databases configured.           
  -o <value> | --overwrite <value>
         overwrite is a true/false flag. Set this to true to overwrite the output file if already present. Default value is false.           
  --help
         Use this option to get detailed usage information of this utility.

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
