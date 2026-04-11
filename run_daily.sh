#!/bin/bash
# 巴菲特選股日報 - 每日排程腳本
# 建議在台股收盤後 (14:30+) 執行以取得最新資料

set -e

export PATH="$HOME/.pyenv/bin:$HOME/.pyenv/shims:$PATH"
eval "$(pyenv init -)"
eval "$(pyenv virtualenv-init -)"

cd /Users/gpwang/project/buffet
pyenv activate buffet

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting daily report generation..."
python generate_report.py >> /Users/gpwang/project/buffet/logs/daily.log 2>&1
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Report generation complete."
