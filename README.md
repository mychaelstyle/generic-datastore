generic-datastore
=================================

[ ![Download](https://api.bintray.com/packages/mychaelstyle/maven/generic-datastore/images/download.svg) ](https://bintray.com/mychaelstyle/maven/generic-datastore/_latestVersion)


RDBMとKVSを統合的に扱えるORMライブラリです。get, update, insert, deleteなど基本的な動作しか行えませんが、MySQL, MariaDB, Redis, AmazonDynamoDB, memcachedなどのデータストアを同じ操作で扱うことができます。

## Prerequisites

* Java (> v1.7.0)
* Gradle (> v2.2.0)

## How to use

### Get started

You can get this library from bintray maven2 repository.

bintrayのMavenリポジトリで公開しています。
mavenを利用する場合は (https://dl.bintray.com/mychaelstyle/maven/) をリポジトリに追加してください。

gradleを利用する場合は下記のようにbuild.gradleに追加します。


```
URL      : https://dl.bintray.com/mychaelstyle/maven/
Group    : com.mychaelstyle
Artifact : generic-datastoer
```

#### when using gradle

add repository and dependency to your build.gradle.

```
repositories {
  jcenter()
    maven {
      url "https://dl.bintray.com/mychaelstyle/maven/"
    }
  }
}
...
dependencies {
  compile 'com.mychaelstyle:generic-datastore:0.2.0'
}
```

#### when using SBT

add resolvers and libraryDependencies to your build.sbt.

```
resolvers += "Mychaelstyle Lib" at "http://mychaelstyle.github.io/m2repos/"


libraryDependencies ++= Seq(
  "com.mychaelstyle" % "generic-datastore" % "0.2.0"
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
