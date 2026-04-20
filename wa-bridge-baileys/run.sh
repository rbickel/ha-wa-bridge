#!/bin/bash

# Detect environment
if [ -d "/data" ]; then
    echo "Running in Home Assistant Add-on environment"
    export WA_DATA_PATH=/data
else
    echo "Running in Standard Docker environment"
    export WA_DATA_PATH=./.baileys_data
fi

echo "Starting WhatsApp Baileys Bridge..."
exec npm start
