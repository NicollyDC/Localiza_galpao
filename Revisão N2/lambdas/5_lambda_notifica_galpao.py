import json
from pathlib import Path  # 1. Importa a biblioteca Pathlib

# 2. Define o diretório base do projeto
BASE_DIR = Path(__file__).resolve().parent.parent


def carregar_json(nome_arquivo_completo):
    """Função genérica para carregar um arquivo JSON."""
    try:
        with open(nome_arquivo_completo, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"ERRO: Arquivo de simulação '{nome_arquivo_completo}' não encontrado.")
        return None
    except json.JSONDecodeError:
        print(f"ERRO: O arquivo '{nome_arquivo_completo}' não é um JSON válido.")
        return None

def lambda_handler(event_file_path):
    """
    Função principal que simula o handler da Lambda 2.
    """
    print("\n--- INICIANDO LAMBDA 2: NOTIFICA GALPÃO ---")
    
    # 1. Carregar dados da "Fila 2" (SQS)
    mensagem_despacho = carregar_json(event_file_path)
    
    if not mensagem_despacho:
        print("Erro: Mensagem de despacho não encontrada ou inválida.")
        return

    try:
        produto = mensagem_despacho["id_produto"]
        galpao = mensagem_despacho["galpao_destino"]
        coords = mensagem_despacho["coordenadas_destino"]
        
        # 2. Simular a notificação
        print("... NOTIFICAÇÃO ENVIADA ...")
        print(f"  > Para: API do {galpao}")
        print(f"  > Mensagem: 'Preparar produto {produto} para coleta.'")
        print(f"  > Destino do cliente (Ref): {coords}")
        print("... NOTIFICAÇÃO CONCLUÍDA ...")

    except KeyError as e:
        print(f"Erro: A mensagem de despacho está incompleta. Faltando chave: {e}")
        
    print("--- FINALIZANDO LAMBDA 2 ---")


# --- Ponto de Entrada da Simulação ---
if __name__ == "__main__":
    
    # 3. Usa o BASE_DIR para montar o caminho
    ARQUIVO_FILA_2 = BASE_DIR / "4_simula_fila_despacho.json" 

    # Executa a função principal da Lambda
    lambda_handler(ARQUIVO_FILA_2)