cloudfeeds-ballista
==========================

A command line utility to migrate data from RDBMS databases to HDFS using SCP.

The application can be configured to export data corresponding to multiple databases. For each 
such database the application performs the following steps

1.  Extracts yesterdays(by default; configurable with --runDate option) data from the database into a temporary
    gZip file and places it under a temporary location (configurable using cloudfeeds-ballista.conf file)
2.  Using SCP, transfers the gZip file to the remote HDFS server to a desired 
    location(configurable using cloudfeeds-ballista.conf file)    
3.  After successfully exporting data corresponding to all databases, creates _SUCCESS file and places it at each 
    of the remote output locations. The _SUCCESS file contains number of records exported from the corresponding 
    databases that are stored in the remote output location in the following format.   
    ```<dbName1>=<number of records exported>```  
    ```<dbName2>=<number of records exported>```

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
Run the command below to create a runnable jar. 
```
gradle clean build uberjar
```
Run the following command to run the app.

```
java -Dconfig.file=/<file path>/cloudfeeds-ballista.conf -Dlogback.configurationFile=/<file path>/logback.xml -jar build/libs/cloudfeeds-ballista-<version>.jar
```
## How to run the App after installing the rpm
Run the command to build an RPM. Install rpm. Execute the below script   

sh /opt/cloudfeeds-ballista/bin/cloudfeeds-ballista.sh

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
