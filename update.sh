#!/bin/bash

# 获取更新描述
echo "请输入更新描述："
read message

# 自动执行 git 命令
git add .
git commit -m "更新: $message"
git push

echo "更新完成！"
#终端输入：./update.sh，更新版本