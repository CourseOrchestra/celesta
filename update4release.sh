#!/bin/bash

find . -name "pom.xml" | while read file
do
    sed -i -E 's|([ ]*<version>7\.4\.)([0-9]+)-SNAPSHOT</version>|echo "\1$((\2 + 1))</version>"|ge' "$file"
done
