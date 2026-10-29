#!/usr/bin/env python3
"""
Consumer de eventos para Apache Pulsar + BookKeeper
Consome eventos do tópico e exibe as coordenadas do BookKeeper Ledger (Ledger ID : Entry ID).
"""

import json
import pulsar
from rich.console import Console
from rich.table import Table

console = Console()

PULSAR_SERVICE_URL = "pulsar://localhost:6650"
TOPIC_NAME = "persistent://public/default/financial-events"
SUBSCRIPTION_NAME = "audit-ledger-sync-sub"


def main():
    console.print("\n[bold cyan]====================================================[/bold cyan]")
    console.print("[bold green]🎧 Iniciando Consumer Pulsar (Leitura do BookKeeper)[/bold green]")
    console.print(f"[dim]Conectando em {PULSAR_SERVICE_URL}...[/dim]")
    console.print(f"[dim]Tópico: {TOPIC_NAME}[/dim]")
    console.print(f"[dim]Subscription: {SUBSCRIPTION_NAME}[/dim]")
    console.print("[bold cyan]====================================================[/bold cyan]\n")

    client = pulsar.Client(PULSAR_SERVICE_URL)

    try:
        # InitialPosition.Earliest lê desde a primeira entrada gravada no Ledger
        consumer = client.subscribe(
            topic=TOPIC_NAME,
            subscription_name=SUBSCRIPTION_NAME,
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest
        )

        table = Table(title="[bold green]Eventos Recuperados do Apache BookKeeper Ledger[/bold green]")
        table.add_column("BookKeeper Ledger ID", justify="center", style="bold yellow")
        table.add_column("BookKeeper Entry ID", justify="center", style="bold red")
        table.add_column("Event Seq", justify="right", style="cyan")
        table.add_column("Event Type", style="magenta")
        table.add_column("Amount", justify="right", style="green")
        table.add_column("Customer ID", style="blue")

        console.print("[yellow]Aguardando mensagens... (Timeout de 5s após inatividade)[/yellow]\n")

        msg_count = 0
        while True:
            try:
                # Timeout de 5000 ms se nenhuma mensagem chegar
                msg = consumer.receive(timeout_millis=5000)
            except Exception:
                break

            msg_id = msg.message_id()
            ledger_id = msg_id.ledger_id()
            entry_id = msg_id.entry_id()

            data = json.loads(msg.data().decode("utf-8"))
            msg_count += 1

            table.add_row(
                str(ledger_id),
                str(entry_id),
                str(data.get("sequence", "-")),
                str(data.get("type", "-")),
                f"R$ {data.get('amount', 0):.2f}",
                str(data.get("customer_id", "-"))
            )

            # Confirma o recebimento (Acknowledge)
            consumer.acknowledge(msg)

        if msg_count > 0:
            console.print(table)
            console.print(f"\n[bold green]✓ {msg_count} mensagens consumidas e reconhecidas com sucesso![/bold green]\n")
        else:
            console.print("[yellow]Nenhuma mensagem encontrada no tópico.[/yellow]\n")

    finally:
        client.close()


if __name__ == "__main__":
    main()
