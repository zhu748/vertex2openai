#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
npx --yes tailwindcss@3.4.17 --input scripts/dashboard.css \
  --content app/templates/dashboard.html,app/static/request-monitor.js \
  --output app/static/dashboard.css --minify
