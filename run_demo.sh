#!/usr/bin/env bash
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
cd "$DIR"

echo "============================================================"
echo "⚡ POC: Apache BookKeeper + Apache Pulsar + Python Producer"
echo "============================================================"

# 1. Subir container Docker
echo "[1/5] Verificando e subindo o container Apache Pulsar + BookKeeper..."
docker compose up -d

# Aguardar broker estar pronto
echo "Aguardando broker e bookies inicializarem..."
until docker exec pulsar-bookkeeper bin/pulsar-admin brokers healthcheck > /dev/null 2>&1; do
    printf "."
    sleep 2
done
echo " Pronto!"

# 2. Criar ambiente virtual se não existir
if [ ! -d "venv" ]; then
    echo "[2/5] Criando ambiente virtual Python..."
    python3 -m venv venv
    ./venv/bin/pip install --upgrade pip
    ./venv/bin/pip install -r requirements.txt
else
    echo "[2/5] Ambiente virtual venv detectado."
fi

# 3. Executar o Producer
echo -e "\n[3/5] Executando o Producer Python (Enviando eventos para o Ledger)..."
./venv/bin/python producer.py

# 4. Inspecionar o Ledger no Apache BookKeeper
echo -e "\n[4/5] Inspecionando o BookKeeper Ledger e os Bookies..."
./venv/bin/python inspect_ledger.py

# 5. Executar o Consumer
echo -e "\n[5/5] Executando o Consumer Python (Consumindo diretamente do Ledger)..."
./venv/bin/python consumer.py

echo "============================================================"
echo "🎉 PoC executada com sucesso!"
echo "Para reinspecionar os Ledgers: ./venv/bin/python inspect_ledger.py"
echo "Para parar os serviços: docker compose down"
echo "============================================================"
