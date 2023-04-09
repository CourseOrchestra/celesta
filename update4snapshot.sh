#!/bin/bash

find . -name "pom.xml" | while read file
do
    sed -i -E 's|(<version>7\.4\.[0-9]+)</version>|\1-SNAPSHOT</version>|g' "$file"
done
