#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
# 与 package.json 的 build:css 保持同一通配符口径（app/templates/*.html +
# app/static/*.js），避免逐文件枚举清单与实际文件集漂移——曾遗漏
# console-models / console-overview / console-logs / console-bootstrap 四个
# JS，其新引入的工具类会被静默丢弃（2026-11 审查修复）。
npx --yes tailwindcss@3.4.17 --input scripts/dashboard.css \
  --content 'app/templates/*.html,app/static/*.js' \
  --output app/static/dashboard.css --minify
