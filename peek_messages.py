#!/usr/bin/env python3
"""
Visualizador de Eventos (Message Peeker) do Apache Pulsar / BookKeeper
Lê os eventos gravados nos Ledgers do BookKeeper sem alterar os ponteiros
das subscrições dos consumidores (usa a abstração Pulsar Reader).
"""

import argparse
import json
import pulsar
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax

console = Console()

PULSAR_SERVICE_URL = "pulsar://localhost:6650"
TOPIC_NAME = "persistent://public/default/financial-events"


def main():
    parser = argparse.ArgumentParser(description="Espiar eventos gravados no Apache BookKeeper")
    parser.add_argument(
        "--count",
        type=int,
        default=5,
        help="Quantidade de mensagens para visualizar (padrão: 5)"
    )
    args = parser.parse_args()

    console.print("\n[bold cyan]====================================================[/bold cyan]")
    console.print(f"[bold yellow]👀 Visualizando últimos {args.count} eventos do BookKeeper[/bold yellow]")
    console.print(f"[dim]Tópico: {TOPIC_NAME}[/dim]")
    console.print("[bold cyan]====================================================[/bold cyan]\n")

    client = pulsar.Client(PULSAR_SERVICE_URL)

    try:
        # O Reader lê diretamente dos Ledgers sem comprometer os cursores das Subscrições
        reader = client.create_reader(
            TOPIC_NAME,
            start_message_id=pulsar.MessageId.earliest
        )

        messages = []
        # Percorre as mensagens até coletar as mais recentes
        while True:
            try:
                msg = reader.read_next(timeout_millis=1500)
                messages.append(msg)
            except Exception:
                break

        if not messages:
            console.print("[yellow]Nenhuma mensagem encontrada no Ledger deste tópico.[/yellow]\n")
            return

        # Pega as últimas N mensagens solicitadas
        display_msgs = messages[-args.count:]

        console.print(f"[bold green]Exibindo {len(display_msgs)} de {len(messages)} eventos armazenados no BookKeeper:[/bold green]\n")

        for idx, msg in enumerate(display_msgs):
            msg_id = msg.message_id()
            ledger_id = msg_id.ledger_id()
            entry_id = msg_id.entry_id()
            props = msg.properties()

            raw_data = msg.data().decode("utf-8")
            try:
                # Formata o JSON com indentação se for válido
                parsed = json.loads(raw_data)
                formatted_json = json.dumps(parsed, indent=2, ensure_ascii=False)
            except Exception:
                formatted_json = raw_data

            header_info = (
                f"[bold yellow]BookKeeper Ledger ID:[/bold yellow] #{ledger_id}  │  "
                f"[bold red]Entry ID:[/bold red] #{entry_id}  │  "
                f"[bold cyan]Publicado em:[/bold cyan] {msg.publish_timestamp()}"
            )

            syntax = Syntax(formatted_json, "json", theme="monokai", line_numbers=True)
            console.print(Panel(syntax, title=header_info, subtitle=f"Propriedades: {props}"))
            console.print()

    finally:
        client.close()


if __name__ == "__main__":
    main()
