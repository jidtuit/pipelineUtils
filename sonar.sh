#!/usr/bin/env bash

echo "Starting sonarqube..."

./gradlew clean sonarqube \
  -Dsonar.projectKey=$SONAR_PROJECT_KEY \
  -Dsonar.organization=$SONAR_ORGANIZATION \
  -Dsonar.host.url=$SONAR_HOST_URL \
  -Dsonar.login=$SONAR_TOKEN \
  -Dsonar.branch.name=$TRAVIS_BRANCH


echo "End of sonarqube task."
