#!/usr/bin/env python3
"""
Inspetor de Ledgers do Apache BookKeeper no Pulsar
Consulta a API interna do Pulsar Broker para demonstrar como os eventos
estão armazenados nos Ledgers do BookKeeper e como os cursores apontam para Ledger:Entry.
"""

import requests
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()

PULSAR_ADMIN_URL = "http://localhost:8080/admin/v2"
TOPIC_REST_NAME = "persistent/public/default/financial-events"


def get_json(url: str):
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        console.print(f"[bold red]Erro ao consultar {url}: {e}[/bold red]")
        return None


def main():
    console.print("\n[bold cyan]====================================================[/bold cyan]")
    console.print("[bold yellow]🔍 Inspeção de Ledgers do Apache BookKeeper no Pulsar[/bold yellow]")
    console.print("[dim]Consultando Managed Ledger, Bookies e Cursors...[/dim]")
    console.print("[bold cyan]====================================================[/bold cyan]\n")

    # 1. Informações dos Bookies de Armazenamento
    bookies_info = get_json(f"{PULSAR_ADMIN_URL}/bookies/all")
    bookie_nodes = []
    if bookies_info and "bookies" in bookies_info:
        bookie_nodes = [b.get("bookieId") for b in bookies_info["bookies"]]

    # 2. Informações de baixo nível do ManagedLedger (BookKeeper)
    stats = get_json(f"{PULSAR_ADMIN_URL}/{TOPIC_REST_NAME}/internalStats")
    if not stats:
        console.print("[red]Verifique se o container pulsar-bookkeeper está rodando na porta 8080.[/red]\n")
        return

    last_confirmed = stats.get("lastConfirmedEntry", "N/A")
    state = stats.get("state", "N/A")
    total_entries = stats.get("entriesAddedCounter", 0)
    current_ledger_entries = stats.get("currentLedgerEntries", 0)
    current_ledger_size = stats.get("currentLedgerSize", 0)
    total_size = stats.get("totalSize", 0)
    ledgers = stats.get("ledgers", [])
    cursors = stats.get("cursors", {})

    # Se last_confirmed tiver formato ledger:entry, extrair o active ledger id
    active_ledger_id = "N/A"
    if ":" in last_confirmed:
        active_ledger_id = last_confirmed.split(":")[0]
    elif ledgers:
        active_ledger_id = str(ledgers[-1].get("ledgerId"))

    summary_text = (
        f"[bold]Tópico Pulsar:[/bold] persistent://public/default/financial-events\n"
        f"[bold]Nós de Armazenamento BookKeeper (Bookies):[/bold] [cyan]{', '.join(bookie_nodes) or '1 Bookie ativo'}[/cyan]\n"
        f"[bold]Estado do Ledger:[/bold] [bold green]{state}[/bold green]\n"
        f"[bold]Active BookKeeper Ledger ID:[/bold] [bold yellow]#{active_ledger_id}[/bold yellow]\n"
        f"[bold]Última Entrada Confirmada (Ledger:Entry):[/bold] [bold red]{last_confirmed}[/bold red]\n"
        f"[bold]Entradas no Ledger Ativo:[/bold] {current_ledger_entries}\n"
        f"[bold]Tamanho do Ledger Ativo em Disco:[/bold] {current_ledger_size} bytes\n"
        f"[bold]Total Acumulado no BookKeeper:[/bold] {total_entries} entradas ({total_size} bytes)"
    )

    console.print(Panel(summary_text, title="[bold cyan]Resumo de Armazenamento Apache BookKeeper[/bold cyan]"))

    # Tabela com todos os Ledgers do Managed Ledger
    table = Table(title="[bold green]Segmentos de Armazenamento Físico (BookKeeper Ledgers)[/bold green]")
    table.add_column("Ledger ID", justify="center", style="bold yellow")
    table.add_column("Entradas (Entries)", justify="right", style="cyan")
    table.add_column("Tamanho em Disco", justify="right", style="magenta")
    table.add_column("Offloaded (Tiered Storage)", justify="center", style="blue")
    table.add_column("Status", justify="center", style="green")

    for l in ledgers:
        lid = str(l.get("ledgerId"))
        is_active = (lid == active_ledger_id)
        entries = current_ledger_entries if is_active else l.get("entries", 0)
        size = current_ledger_size if is_active else l.get("size", 0)
        offloaded = "Sim" if l.get("offloaded") else "Não (Bookie local)"
        status = "[bold yellow]ATIVO (OPEN)[/bold yellow]" if is_active else "[dim]FECHADO (CLOSED)[/dim]"
        table.add_row(f"#{lid}", str(entries), f"{size} bytes", offloaded, status)

    console.print(table)

    # Cursores de leitura (como os consumidores rastreiam entradas no BookKeeper)
    if cursors:
        cursor_table = Table(title="[bold cyan]Posicionamento dos Consumidores nos Ledgers (Cursors)[/bold cyan]")
        cursor_table.add_column("Subscription / Cursor", style="magenta")
        cursor_table.add_column("MarkDelete (Último Ack Ledger:Entry)", justify="center", style="green")
        cursor_table.add_column("ReadPosition (Próximo a Ler Ledger:Entry)", justify="center", style="yellow")
        cursor_table.add_column("Mensagens Pendentes", justify="right", style="cyan")

        for name, c in cursors.items():
            mark_del = c.get("markDeletePosition", "N/A")
            read_pos = c.get("readPosition", "N/A")
            pending = c.get("numberOfEntriesSinceFirstNotAckedMessage", 0)
            cursor_table.add_row(name, mark_del, read_pos, str(pending))

        console.print("\n")
        console.print(cursor_table)

    console.print("\n[bold green]✓ Como funciona o Apache BookKeeper com Apache Pulsar:[/bold green]")
    console.print("  1. No Pulsar, os tópicos persistentes gravam seus eventos em [bold yellow]Ledgers[/bold yellow] gerenciados pelo Apache BookKeeper.")
    console.print("  2. Cada evento vira uma [bold red]Entry[/bold red] imutável e append-only dentro do Ledger ativo.")
    console.print("  3. Quando um Ledger atinge limite de tempo ou tamanho, ele é fechado (CLOSED) e um novo Ledger é aberto de forma transparente.")
    console.print("  4. Os dados são replicados entre os [bold cyan]Bookies[/bold cyan] de acordo com a política de Ensemble / Quorum configurada.\n")


if __name__ == "__main__":
    main()
