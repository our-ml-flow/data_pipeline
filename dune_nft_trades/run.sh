#!/bin/bash

read -p "Enter Prefect API Key: " PREFECT_API

read -p "Enter Prefect Workspace: " WORK_SPACE

prefect cloud login -k "$PREFECT_API" --workspace "$WORK_SPACE"

prefect agent start