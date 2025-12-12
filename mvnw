#!/bin/sh
# Maven Wrapper script

set -e

MAVEN_PROJECTBASEDIR="${MAVEN_BASEDIR:-$(cd "$(dirname "$0")" && pwd)}"
export MAVEN_PROJECTBASEDIR

MAVEN_WRAPPER_JAR="$MAVEN_PROJECTBASEDIR/.mvn/wrapper/maven-wrapper.jar"
MAVEN_WRAPPER_PROPERTIES="$MAVEN_PROJECTBASEDIR/.mvn/wrapper/maven-wrapper.properties"

# Download maven wrapper if not present
if [ ! -f "$MAVEN_WRAPPER_JAR" ]; then
    if [ -f "$MAVEN_WRAPPER_PROPERTIES" ]; then
        WRAPPER_URL=$(grep 'wrapperUrl' "$MAVEN_WRAPPER_PROPERTIES" | cut -d'=' -f2)
        mkdir -p "$(dirname "$MAVEN_WRAPPER_JAR")"
        echo "Downloading Maven Wrapper from $WRAPPER_URL"
        if command -v curl > /dev/null 2>&1; then
            curl -fsSL -o "$MAVEN_WRAPPER_JAR" "$WRAPPER_URL"
        elif command -v wget > /dev/null 2>&1; then
            wget -q -O "$MAVEN_WRAPPER_JAR" "$WRAPPER_URL"
        else
            echo "Error: curl or wget is required to download Maven Wrapper"
            exit 1
        fi
    fi
fi

# Find java
if [ -n "$JAVA_HOME" ]; then
    JAVA_CMD="$JAVA_HOME/bin/java"
else
    JAVA_CMD="java"
fi

if [ ! -x "$JAVA_CMD" ] && ! command -v "$JAVA_CMD" > /dev/null 2>&1; then
    echo "Error: JAVA_HOME is not set and java is not in PATH"
    exit 1
fi

# Run Maven via wrapper or directly
if [ -f "$MAVEN_WRAPPER_JAR" ]; then
    exec "$JAVA_CMD" \
        -Dmaven.multiModuleProjectDirectory="$MAVEN_PROJECTBASEDIR" \
        -jar "$MAVEN_WRAPPER_JAR" "$@"
else
    # Fallback to system Maven
    exec mvn "$@"
fi


