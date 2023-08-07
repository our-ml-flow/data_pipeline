#!/bin/bash

read -p "Enter Prefect API Key: " PREFECT_API

read -p "Enter Prefect Workspace: " WORK_SPACE

read -p "Enter Prefect Workspace: " WORK_QUEUE

prefect cloud login -k "$PREFECT_API" --workspace "$WORK_SPACE"

prefect agent start --work-queue "$WORK_QUEUE"