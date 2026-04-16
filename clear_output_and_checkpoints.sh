#!/bin/bash
cd "$(dirname "$0")"
rm -rf output/raw_alerts/* output/raw_alerts/.* \
       checkpoints/raw_alerts/* checkpoints/raw_alerts/.*
