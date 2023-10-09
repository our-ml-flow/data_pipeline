#!/bin/bash

prefect cloud login --key "$PREFECT_API" --workspace "$WORK_SPACE"

prefect agent start --work-queue "$WORK_QUEUE"