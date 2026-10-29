#!/usr/bin/env python3
"""
Stream Producer de eventos contínuos para Apache Pulsar + Apache BookKeeper
Envia eventos em fluxo contínuo para visualização em tempo real no Apache Pulsar Manager.
Pressione Ctrl+C a qualquer momento para encerrar com segurança.
"""

import argparse
import json
import time
import uuid
from datetime import datetime, timezone
import pulsar
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

console = Console()

PULSAR_SERVICE_URL = "pulsar://localhost:6650"
TOPIC_NAME = "persistent://public/default/financial-events"


def create_event(seq: int) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "sequence": seq,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "type": "PAYMENT_TRANSACTION",
        "customer_id": f"CUST-{(seq % 100) + 1000}",
        "amount": round(10.0 + (seq * 1.5) % 500, 2),
        "currency": "BRL",
        "status": "APPROVED",
        "channel": "mobile-app"
    }


def main():
    parser = argparse.ArgumentParser(description="Envio contínuo de eventos para o Apache BookKeeper")
    parser.add_argument(
        "--rate",
        type=float,
        default=2.0,
        help="Quantidade de eventos por segundo (padrão: 2.0)"
    )
    args = parser.parse_args()

    sleep_interval = 1.0 / max(0.1, args.rate)

    intro_text = (
        f"[bold]Tópico:[/bold] {TOPIC_NAME}\n"
        f"[bold]Taxa de Envio:[/bold] [yellow]{args.rate:.1f} eventos/segundo[/yellow]\n"
        f"[bold]Dashboard Web:[/bold] [cyan]http://localhost:9527[/cyan]\n"
        f"[dim]Pressione [bold red]Ctrl + C[/bold red] para parar o fluxo.[/dim]"
    )
    console.print(Panel(intro_text, title="[bold green]🚀 Stream Contínuo de Eventos (Pulsar -> BookKeeper)[/bold green]"))

    client = pulsar.Client(PULSAR_SERVICE_URL)

    try:
        producer = client.create_producer(
            TOPIC_NAME,
            producer_name="streaming-generator",
            batching_enabled=False,
            block_if_queue_full=True
        )

        seq = 1
        total_bytes = 0
        start_time = time.time()

        table = Table(title="[bold yellow]Fluxo em Tempo Real[/bold yellow]")
        table.add_column("Seq", justify="right", style="cyan")
        table.add_column("BookKeeper Ledger", justify="center", style="bold yellow")
        table.add_column("BookKeeper Entry", justify="center", style="bold red")
        table.add_column("Amount", justify="right", style="green")
        table.add_column("Customer", style="magenta")
        table.add_column("Horário (UTC)", style="dim")

        console.print("[dim]Enviando eventos... Abra o navegador em [bold]http://localhost:9527[/bold] para ver os gráficos![/dim]\n")

        while True:
            event = create_event(seq)
            payload_bytes = json.dumps(event).encode("utf-8")
            total_bytes += len(payload_bytes)

            msg_id = producer.send(
                payload_bytes,
                event_timestamp=int(time.time() * 1000),
                properties={"type": event["type"], "customer_id": event["customer_id"]}
            )

            current_time = datetime.now(timezone.utc).strftime("%H:%M:%S")

            console.print(
                f"[bold cyan]#{seq:04d}[/bold cyan] │ "
                f"Ledger: [bold yellow]#{msg_id.ledger_id()}[/bold yellow] │ "
                f"Entry: [bold red]#{msg_id.entry_id():<4d}[/bold red] │ "
                f"Valor: [green]R$ {event['amount']:>6.2f}[/green] │ "
                f"Cliente: [magenta]{event['customer_id']}[/magenta] │ "
                f"Hora: [dim]{current_time}[/dim]"
            )

            seq += 1
            time.sleep(sleep_interval)

    except KeyboardInterrupt:
        elapsed = time.time() - start_time
        events_sent = seq - 1
        actual_rate = events_sent / max(elapsed, 0.001)

        summary_text = (
            f"[bold]Total de Eventos Enviados:[/bold] [bold green]{events_sent}[/bold green]\n"
            f"[bold]Bytes Gravados no BookKeeper:[/bold] {total_bytes} bytes (~{total_bytes / 1024:.2f} KB)\n"
            f"[bold]Tempo de Execução:[/bold] {elapsed:.1f} segundos\n"
            f"[bold]Taxa Efetiva:[/bold] {actual_rate:.2f} msg/s"
        )
        console.print("\n")
        console.print(Panel(summary_text, title="[bold yellow]⏹ Fluxo Encerrado pelo Usuário[/bold yellow]"))

    finally:
        client.close()
        console.print("[dim]Conexão com Apache Pulsar encerrada com sucesso.[/dim]\n")


if __name__ == "__main__":
    main()
