#!/usr/bin/env bash

./gradlew clean sonarqube \
  -Dsonar.projectKey=jidtuit_pipelineUtils \
  -Dsonar.organization=jidtuit-github \
  -Dsonar.host.url=https://sonarcloud.io \
  -Dsonar.login=228663e3d74d5f9ccba77b895849b58b491208be

