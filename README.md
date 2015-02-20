cloudfeeds-ballista
==========================

A command line utility to migrate data from RDBMS databases to HDFS using SCP.


## How to Build
To build this component, we require:
* Gradle version 2.2 or above
* JDK 7
* Scala 2.10 or above


### Simple build
```
gradle clean build
```

### Build an RPM
```
gradle clean buildRpm
```

## How to run the App
Run a simple build to create an installable app. Run the following command.

```
java -Dconfig.file="/<file path>/cloudfeeds-ballista.conf" -Dlogback.configurationFile="/<file path>/logback.xml"-jar build/libs/cloudfeeds-ballista-<version>.jar
```
## How to run the App after installing the rpm
Run the command to build an RPM. Install rpm. Execute the below script   

sh /usr/share/cloudfeeds-ballista/bin/cloudfeeds-ballista.sh

The app expects the configuration files to be present at /etc/cloudfeeds-ballista

## Command line options

  -d <value> | --runDate <value>
         runDate is a date in the format yyyy-MM-dd. Data belonging to this date will be exported. Default value is yesterday.           
  -n <value> | --dbNames <value>
         dbNames is comma separated list of database names to be exported. Default value is all the databases configured.           
  --help
         Use this option to get detailed usage information of this utility.

## Configuration

The Cloud Feeds Ballista app uses the following configuration files:

### cloudfeeds-ballista.conf
This file configures the cloudfeeds-ballista.conf app itself. This sample [cloudfeeds-ballista.conf](https://github.com/rackerlabs/cloudfeeds-ballista/blob/master/src/main/resources/cloudfeeds-ballista.conf) file contains the necessary information to configure the app. 

The file can be specified when running the app by specifying the Java System properties ```-Dconfig.file=<path_to_file>```.

### logback.xml
This file has the logging related configuration.

The file can be specified when running the app by specifying the Java System properties ```-Dconfig.file=<logback.configurationFile>```.
