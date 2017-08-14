# Osm Reader

An Android friedly tool + library to read on compressed Open Street Map data (bz2) extracts, convert it into a Sqlite database for easy querying.

# Author

Alex O'Ree, committer for osmdroid, Apache Software Foundation, and many other projects

# License

Apache Software License v2.0

# Development status

Still working on this in my spare time. Not really API or functionally stable right now.

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

Keyword search (everywhere)

```java
List<SearchResults> searchResults = QueryTools.search("new castle", 10, 0, connection);
for (int i=0; i < searchResults.size(); i++) {
     SearchResults record = searchResults.get(i);
     System.out.println(record.getName() + " " + searchResults.get(i).getType() + " " + searchResults.get(i).getLat() + "," + searchResults.get(i).getLon() + "," + searchResults.get(i).getDatabaseId());
 }
```

Keyword search within a bounds

```java
List<SearchResults> searchResults = QueryTools.search("new castle", 10, 0, connection, 40,-75,39,-76);
for (int i=0; i < searchResults.size(); i++) {
     SearchResults record = searchResults.get(i);
     System.out.println(record.getName() + " " + searchResults.get(i).getType() + " " + searchResults.get(i).getLat() + "," + searchResults.get(i).getLon() + "," + searchResults.get(i).getDatabaseId());
 }
```

Using the `databaseId` from above, get all "tags" for a given Node, Way, or Relation

```java
Map<String, String> tags = QueryTools.getTags(366753278L, connection, 1000, 0);
```

Using the `databaseId` from above, Street address, phone number and website extraction.
Note: this is a best effort extraction using commonly used keywords, since the osm data model is open ended.

```java
Address addr = QueryTools.getAddress(366753278L, connection);
```




## Where do i get the data from?

Try this source: https://download.geofabrik.de/ You want the `.osm.bz2` files.

## How much space do I need?

The smalled osm extract is for the state of Delaware, USA. Compressed, it's only 11MB and 7,564,957
xml elements which translates to about 2,483,857 database rows (with ways and relations). Uncompressed XML is around 150MB.
In database format, it takes about 100 MB or about 10x whatever the download size is. The whole planet
is around 50GB right now, which means you'd need 500GB of disk storage for the planet.

## Can we reduce the storage need?

What data is needed is up to the use case. If you're just using this tool for reverse geocoding, you 
probably don't care who edited what and when. This is a future optimization we could do (pull requests welcome!) 

## How long does it take to import

Processing time on :

| Implementation | CPU | Data set | Batch Size | Time wo/Ways & Relations | Time w/Ways & Relations
| -------------- | --- | -------- | ---------- | ------------------------ | ----------------------- |
| XML pull parser | AMD 8 core  with SSD | Delaware.bz2 | 500 | n/a | 102 sec
| XML pull parser | Galaxy S5  | Delaware.bz2 | 400 | 1 hr+  | 1 hr+
| XML pull parser | AMD 8 core  with SSD | Planet | 500 | about 66 days (est) |
| Osmosis | AMD 8 core  with SSD | Delaware.bz2 | 500 | n/a | 83 sec
| Osmosis | Galaxy S5 | Delaware.bz2 | 400 | n/a | n/a

**Notes**
Osmosis doesn't appear to work in an android environment. No idea why


# Credits

Thanks to https://github.com/gradle-fury/gradle-fury/ for making Gradle easier to work with.