# Osm Reader

An Android friedly tool + library to read on compressed Open Street Map data (bz2) extracts, convert it into a Sqlite database for easy querying.

# Author

Alex O'Ree, committer for osmdroid, Apache Software Foundation, and many other projects

# License

Apache Software License v2.0


# Usage

## Checkout this project

`git clone https://github.com/spyhunter99/osmreader.git`

## Build and install to maven local using Gradle Fury

`./gradlew clean install`

## Include this library in your project

Edit your $rootDir/build.gradle, add under allProjects, repositories, add mavenLocal()
Edit your app/module's build.gradle file and add

````
//Android only
compile ('org.osmdroid.reader:osm-reader-lib:1.0.0-SNAPSHOT'){
    exclude group: 'org.xerial'
}
compile 'org.sqldroid:sqldroid:1.0.3'

//Java JRE/JDK only
compile 'org.osmdroid.reader:osm-reader-lib:1.0.0-SNAPSHOT'
````

## Sample code - Importing the data

````
//needed on android only
DriverManager.registerDriver((Driver) (Class.forName(
        "org.sqldroid.SQLDroidDriver" , true,
        getClass().getClassLoader()).newInstance()));
Connection con = DriverManager.getConnection("jdbc:sqldroid:/sdcard/osmdata.sqlite");

Reader reader = new Reader();
//read input file to output database
reader.read("/sdcard/delaware-latest.osm.bz2", con);

````

Once this process is complete, you'll have yourself a database that's easy to query. It can take a very long time on a smart phone or tablet.

## How to query for POIs, Street names, City/Town/State names, etc

TODO

# Credits

Thanks to https://github.com/chrisdoyle/gradle-fury/ for making Gradle easier to work with.