
gradle shadowJar

podman build -t unclepaul84/java-mmaped-proto-cache .

podman push unclepaul84/java-mmaped-proto-cache docker://docker.io/unclepaul84/java-mmaped-proto-cache