generic-datastore
=================================

## Prerequisites

* Java (> v1.7.0)
* Gradle (> v2.2.0)

## How to use

### Get started

you can get this library from my Maven2 repository.
The latest version is 0.1.0 right now.

```
URL      : http://mychaelstyle.github.io/m2repos/
Group    : com.mychaelstyle.common
Artifact : generic-datastore
```

#### when using gradle

add repository and dependency to your build.gradle.

```
repositories {
  mavenCentral()
  maven {
    url "http://mychaelstyle.github.io/m2repos/"
  }
}
...
dependencies {
  compile 'com.mychaelstyle.common:generic-datastore:0.1.0'
}
```

#### when using SBT

add resolvers and libraryDependencies to your build.sbt.

```
resolvers += "Mychaelstyle common lib" at "http://mychaelstyle.github.io/m2repos/"


libraryDependencies ++= Seq(
  "com.mychaelstyle.common" % "generic-datastore" % "0.1.0"
)
```

### How to use

see JUnit test case, com.mychaelstyle.common.GenericDatastoreTest.java.


## Development

set environment valuable for AWS access.

```
$ export AWS_ACCESS_KEY='your_access_key'
$ export AWS_SECRET_KEY='your_secret_key'
$ export AWS_ENDPOINT_DYNAMODB='dynamodb.ap-northeast-1.amazonaws.com '
```

clone this repository to your machine.
after that, build by using gradle.

```
$ git clone git@github.com:mychaelstyle/generic-datastore.git
$ cd generic-datastore
$ ./gradlew dependencies
```

after that, if you use eclipse, type following command

```
$ ./gradlew dependencies
