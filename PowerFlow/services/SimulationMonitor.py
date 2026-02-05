import os
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import box # Importe o box para bordas espessas

class SimulationMonitor:
    def __init__(self, num_workers: int):
        self.console = Console()
        self.num_workers = num_workers
        
    def RenderBar(self, percentage: float, width: int = 40) -> str:
        """Gera uma barra de progresso ultra-espessa usando blocos sólidos"""
        if percentage > 100: percentage = 100
        bar_width = int(width * percentage / 100)
        
        # Caractere de bloco cheio (Totalmente preenchido)
        # O uso de cores diferentes (ex: green) aumenta a percepção de espessura
        bar = "[bold green]" + "█" * bar_width + "[/]"
        
        if bar_width < width:
            bar += "[dim]" + "░" * (width - bar_width) + "[/]" # Fundo texturizado
        return bar
    
    def AtualizarTela(self, progress_dict, total_arquivos, nomes_feeders, pontos_simulaveis):
        snapshot = dict(progress_dict)
        concluidos = sum(1 for p in snapshot.values() if p >= pontos_simulaveis)
        
        os.system('cls' if os.name == 'nt' else 'clear')
        
        # 1. Tabela com bordas HEAVY (mais espessas)
        table = Table(show_header=True, 
                      header_style='bold magenta',
                      box=box.HEAVY, # Define bordas externas e internas grossas
                      expand=True, 
                      width=95)
        
        table.add_column("Feeder", style="bold white", width=25)
        table.add_column("Status / Progress Bar", width=50)
        table.add_column("Perc.", justify="right", style="bold yellow", width=12)
        
        percent_geral = (concluidos / total_arquivos) * 100 if total_arquivos > 0 else 0
        
        # Linha Geral com destaque
        table.add_row(
            f"TOTAL PROGRESS",
            self.RenderBar(percent_geral, width=48),
            f"{percent_geral:>5.1f}%"
        )
        table.add_section()
        
        ativos = 0
        for i, progresso in snapshot.items():
            if 0 < progresso < pontos_simulaveis:
                percent = (progresso / pontos_simulaveis) * 100
                table.add_row(
                    nomes_feeders[i],
                    self.RenderBar(percent, width=48),
                    f"{percent:>5.1f}%"
                )
                ativos += 1
                
        for _ in range(max(0, self.num_workers - ativos)):
            table.add_row("[dim italic]Waiting Process...[/]", "", "")
            
        # 2. Painel com título em destaque e borda colorida
        self.console.print(
            Panel(
                table, 
                title="[bold reverse white]  POWER FLOW SIMULATOR  [/]", 
                #subtitle="[bold]Mato Grosso Project[/]",
                border_style="bright_blue",
                padding=(1, 2)
            )
        )
        
        return concluidos