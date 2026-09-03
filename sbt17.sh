#!/bin/bash
# Force sbt à démarrer avec le JDK 17 (requis par Spark 3.5.x), quelle que soit
# la version Java par défaut du poste. À utiliser à la place de `sbt` :
#   ./sbt17.sh compile
#   ./sbt17.sh console
#   ./sbt17.sh run
JAVA17=$(/usr/libexec/java_home -v 17 2>/dev/null)
if [ -z "$JAVA17" ]; then
  echo "Java 17 introuvable sur ce poste. Installe-le : brew install --cask temurin@17"
  exit 1
fi
export JAVA_HOME="$JAVA17"
exec sbt "$@"
