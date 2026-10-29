#!/usr/bin/env python3
"""
Producer de eventos para Apache Pulsar + BookKeeper
Envia eventos formatados em JSON e exibe o Ledger ID e Entry ID gerados no Apache BookKeeper.
"""

import json
import time
import uuid
from datetime import datetime, timezone
import pulsar
from rich.console import Console
from rich.table import Table

console = Console()

PULSAR_SERVICE_URL = "pulsar://localhost:6650"
# Tópicos persistentes gravam diretamente no Apache BookKeeper
TOPIC_NAME = "persistent://public/default/financial-events"


def create_sample_event(i: int) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "sequence": i + 1,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "type": "PAYMENT_TRANSACTION",
        "customer_id": f"CUST-{1000 + (i % 10)}",
        "amount": round(15.50 * (i + 1), 2),
        "currency": "BRL",
        "status": "PROCESSED",
        "metadata": {
            "source": "checkout-service",
            "version": "1.0",
            "environment": "poc"
        }
    }


def main():
    console.print("\n[bold cyan]====================================================[/bold cyan]")
    console.print("[bold yellow]🚀 Iniciando Producer Pulsar -> Apache BookKeeper[/bold yellow]")
    console.print(f"[dim]Conectando em {PULSAR_SERVICE_URL}...[/dim]")
    console.print(f"[dim]Tópico Persistente: {TOPIC_NAME}[/dim]")
    console.print("[bold cyan]====================================================[/bold cyan]\n")

    client = pulsar.Client(PULSAR_SERVICE_URL)

    try:
        # Desabilitamos batching para termos relação 1:1 direta com Entries do Ledger na demonstração
        producer = client.create_producer(
            TOPIC_NAME,
            producer_name="payment-service-producer",
            batching_enabled=False,
            block_if_queue_full=True
        )

        table = Table(title="[bold green]Eventos Enviados ao Apache BookKeeper[/bold green]")
        table.add_column("Seq", justify="right", style="cyan", no_wrap=True)
        table.add_column("Event ID", style="magenta")
        table.add_column("BookKeeper Ledger ID", justify="center", style="bold yellow")
        table.add_column("BookKeeper Entry ID", justify="center", style="bold red")
        table.add_column("Amount", justify="right", style="green")
        table.add_column("Status", justify="center", style="blue")

        num_events = 10
        console.print(f"[bold]Publicando {num_events} eventos no BookKeeper Ledger...[/bold]\n")

        for i in range(num_events):
            event = create_sample_event(i)
            payload_bytes = json.dumps(event).encode("utf-8")

            # Envia a mensagem de forma síncrona para obter o MessageId do BookKeeper
            msg_id = producer.send(
                payload_bytes,
                event_timestamp=int(time.time() * 1000),
                properties={"event_type": event["type"], "customer_id": event["customer_id"]}
            )

            # O MessageId do Pulsar contém explicitamente o Ledger ID e Entry ID do BookKeeper!
            bk_ledger_id = msg_id.ledger_id()
            bk_entry_id = msg_id.entry_id()

            table.add_row(
                str(event["sequence"]),
                event["event_id"][:8] + "...",
                str(bk_ledger_id),
                str(bk_entry_id),
                f"R$ {event['amount']:.2f}",
                "Gravado no Ledger"
            )

            time.sleep(0.2)

        console.print(table)
        console.print("\n[bold green]✓ Todos os eventos foram persistidos com sucesso nos Bookies do BookKeeper![/bold green]\n")

    finally:
        client.close()


if __name__ == "__main__":
    main()
