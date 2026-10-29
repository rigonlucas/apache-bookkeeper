# PoC: Apache BookKeeper + Apache Pulsar + Python Producer

Esta Prova de Conceito (PoC) demonstra a integração entre **Apache Pulsar**, **Apache BookKeeper** e uma aplicação em **Python** para envio, persistência e leitura de eventos em **Ledgers** do BookKeeper.

---

## 🏗️ Como o Apache Pulsar e o Apache BookKeeper Trabalham Juntos

Diferente do Kafka (onde processamento e armazenamento residem nos mesmos brokers), o Apache Pulsar possui uma **arquitetura desacoplada em duas camadas**:

```
 ┌──────────────────────────────────────────────────────────────┐
 │                Camada de Aplicação (Python)                   │
 │       producer.py (Produtor)      consumer.py (Consumidor)   │
 └───────────────────────────────┬──────────────────────────────┘
                                 │ Protocolo Binário (6650)
                                 ▼
 ┌──────────────────────────────────────────────────────────────┐
 │                Camada de Computação (Pulsar)                 │
 │      Brokers stateless (Roteamento, Caching, Auth, ACLs)     │
 │               ManagedLedger (Abstração de Log)               │
 └───────────────────────────────┬──────────────────────────────┘
                                 │ BookKeeper Client Protocol
                                 ▼
 ┌──────────────────────────────────────────────────────────────┐
 │             Camada de Armazenamento (BookKeeper)             │
 │   Bookies (Nós de Armazenamento com Journal e Ledger Storage)│
 │                                                              │
 │   ┌──────────────────────┐        ┌──────────────────────┐   │
 │   │  Ledger #11 (CLOSED) │        │   Ledger #13 (OPEN)  │   │
 │   │  Entry 0, 1, 2... 9  │        │  Entry 0, 1, 2... 9  │   │
 │   └──────────────────────┘        └──────────────────────┘   │
 └──────────────────────────────────────────────────────────────┘
```

1. **Brokers Pulsar (Camada de Serviço)**: São *stateless*. Eles recebem os eventos via socket, executam validações e orquestram a gravação.
2. **Bookies BookKeeper (Camada de Armazenamento)**: São *stateful*. Armazenam os dados físicos em estruturas chamadas **Ledgers**.
3. **Ledgers**: São segmentos imutáveis, sequenciais e *append-only*. Um tópico no Pulsar é composto por uma sequência contínua de Ledgers.
4. **Coordenadas `LedgerId : EntryId`**: Quando a aplicação Python publica um evento, o Pulsar retorna um `MessageId` que mapeia diretamente para o **ID do Ledger** e o **ID da Entrada (Entry)** no BookKeeper.

---

## 📁 Estrutura do Projeto

```
.
├── docker-compose.yml     # Cluster Pulsar + BookKeeper + Pulsar Manager UI
├── requirements.txt       # Dependências Python (pulsar-client, requests, rich)
├── run_demo.sh            # Script automatizado de demonstração ponta a ponta
├── producer.py            # Envia lote de eventos JSON e exibe Ledger ID e Entry ID
├── producer_stream.py     # Envia eventos em fluxo contínuo para ver métricas ao vivo no Manager
├── peek_messages.py       # Espia e exibe os eventos/JSON gravados sem alterar os cursores
├── inspect_ledger.py      # Inspeciona os Ledgers, Bookies e Cursors via Pulsar REST API
├── consumer.py            # Consome as entradas gravadas no Ledger e faz Ack
└── README.md              # Documentação detalhada
```

---

## 🖥️ Acesso à Interface Gráfica (Apache Pulsar Manager)

O **Apache Pulsar Manager** já está rodando e disponível no seu navegador:

👉 **URL de Acesso:** [http://localhost:9527](http://localhost:9527)

### Primeiro Acesso:
1. Ao abrir a página pela primeira vez, crie seu usuário e senha de administrador (ex: usuário `admin` e senha `admin`).
2. Faça login com o usuário criado.
3. Clique em **Add Environment**:
   - **Environment Name:** `poc`
   - **Service URL:** `http://pulsar:8080`
4. Pronto! Você poderá ver visualmente seus **Tenants**, **Namespaces**, **Tópicos**, **Subscrições** e os nós **BookKeeper Bookies**!


---

## 🚀 Como Executar

### Opção 1: Demonstração Automatizada (1 Comando)

Execute o script de demonstração que provisiona o cluster, prepara o ambiente virtual e executa todo o fluxo:

```bash
./run_demo.sh
```

---

### Opção 2: Passo a Passo Manual

#### 1. Iniciar o Pulsar e o BookKeeper via Docker Compose

```bash
docker compose up -d
```

Verifique se o broker e os bookies estão saudáveis:
```bash
docker exec pulsar-bookkeeper bin/pulsar-admin brokers healthcheck
```

#### 2. Configurar o Ambiente Python

```bash
python3 -m venv venv
./venv/bin/pip install -r requirements.txt
```

#### 3. Enviar Eventos para o Ledger (`producer.py`)

Execute o produtor em Python:

```bash
./venv/bin/python producer.py
```

O produtor envia 10 eventos financeiros para o tópico `persistent://public/default/financial-events`.
Observe a saída:

```
Eventos Enviados ao Apache BookKeeper
┌─────┬─────────────┬──────────────────────┬─────────────────────┬───────────┬────────────────────┐
│ Seq │ Event ID    │ BookKeeper Ledger ID │ BookKeeper Entry ID │ Amount    │ Status             │
├─────┼─────────────┼──────────────────────┼─────────────────────┼───────────┼────────────────────┤
│ 1   │ 9cb7e1a0... │ 13                   │ 0                   │ R$ 15.50  │ Gravado no Ledger  │
│ 2   │ c2001407... │ 13                   │ 1                   │ R$ 31.00  │ Gravado no Ledger  │
│ ... │ ...         │ 13                   │ ...                 │ ...       │ ...                │
│ 10  │ a7c8e982... │ 13                   │ 9                   │ R$ 155.00 │ Gravado no Ledger  │
└─────┴─────────────┴──────────────────────┴─────────────────────┴───────────┴────────────────────┘
```

#### 4. Inspecionar o Ledger no BookKeeper (`inspect_ledger.py`)

Execute o script inspetor:

```bash
./venv/bin/python inspect_ledger.py
```

Ele exibe:
- O **Active Ledger ID** no BookKeeper;
- O endereço do nó Bookie ativo;
- A quantidade de entradas e bytes persistidos em disco;
- Os **cursores** das subscrições indicando exatamente qual `Ledger:Entry` já foi lido e confirmado (`MarkDelete`).

#### 5. Consumir e Confirmar Mensagens do Ledger (`consumer.py`)

Execute o consumidor:

```bash
./venv/bin/python consumer.py
```

O consumidor lê desde a primeira entrada (`InitialPosition.Earliest`) do Ledger e confirma o recebimento.

---

## 🔍 Comandos Avançados de Inspeção do BookKeeper

Você também pode usar a CLI administrativa diretamente no container Docker:

### 1. Listar nós Bookies ativos
```bash
docker exec pulsar-bookkeeper bin/pulsar-admin bookies list-bookies
```

### 2. Consultar estatísticas internas do Managed Ledger
```bash
docker exec pulsar-bookkeeper bin/pulsar-admin topics stats-internal persistent://public/default/financial-events
```

### 3. Forçar rotação de Ledger (Ledger Rollover)
Para fechar o Ledger atual e forçar a criação de um novo Ledger no BookKeeper:
```bash
docker exec pulsar-bookkeeper bin/pulsar-admin topics unload persistent://public/default/financial-events
```
Ao enviar novas mensagens com o `producer.py`, você verá um novo **Ledger ID** sendo alocado!

---

## 🛑 Parando o Ambiente

Para encerrar os containers e liberar recursos:

```bash
docker compose down
```
