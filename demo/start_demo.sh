#!/bin/bash
# Create the require jars for the demo and copy them into a directory we'll mount in our notebook & spark container
cd .. && mvn install -DskipTests -T 2
mkdir -p demo/jars
cp hudi-support/utils/target/hudi-utils-0.1.0-SNAPSHOT.jar demo/jars
cp api/target/onetable-api-0.1.0-SNAPSHOT.jar demo/jars
cp core/target/onetable-core-0.1.0-SNAPSHOT.jar demo/jars
cp utilities/target/utilities-0.1.0-SNAPSHOT-bundled.jar demo/jars

cd demo
docker-compose up