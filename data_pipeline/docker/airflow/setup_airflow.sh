#!/bin/bash

set -e  # 에러 발생 시 스크립트 즉시 종료

echo "🔧 STEP 1. 필수 패키지 설치 및 Docker 리포지토리 설정"

# 1. Docker 설치를 위한 기본 패키지
sudo apt-get remove -y docker docker-engine docker.io containerd runc || true
sudo apt-get update -y
sudo apt-get install -y ca-certificates curl gnupg lsb-release

# 2. Docker GPG 키 등록
sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
  sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

# 3. Docker apt 리포지토리 설정
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# 4. Docker 및 Compose 설치
sudo apt-get update -y
sudo apt-get install -y docker-ce docker-ce-cli containerd.io \
                        docker-buildx-plugin docker-compose-plugin

# 5. Docker 그룹 추가 및 권한 적용
sudo groupadd docker || true
sudo usermod -aG docker $USER
newgrp docker

echo "✅ Docker 및 docker compose 설치 완료"

echo "📦 STEP 2. 프로젝트 클론 or 최신화"

cd ~
if [ ! -d ~/sportsdrink-pipeline-spark-airflow ]; then
  git clone https://github.com/deokjani/sportsdrink-pipeline-spark-airflow.git
else
  cd ~/sportsdrink-pipeline-spark-airflow
  git pull origin main
fi

echo "✅ 프로젝트 준비 완료"

