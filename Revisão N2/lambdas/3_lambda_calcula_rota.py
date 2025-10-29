import json
import math
from pathlib import Path  # 1. Importe a biblioteca Pathlib

# 2. Defina o diretório base do projeto (a pasta "Revisão N2")

BASE_DIR = Path(__file__).resolve().parent.parent


# --- Funções Auxiliares (Lógica de Negócio) ---

def carregar_json(nome_arquivo_completo):
    """Função para carregar um arquivo JSON."""
    try:
        with open(nome_arquivo_completo, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"ERRO: Arquivo de simulação '{nome_arquivo_completo}' não encontrado.")
        return None
    except json.JSONDecodeError:
        print(f"ERRO: O arquivo '{nome_arquivo_completo}' não é um JSON válido.")
        return None

def salvar_json(dados, nome_arquivo_completo):
    """Função para salvar dados em um arquivo JSON."""
    try:
        with open(nome_arquivo_completo, 'w', encoding='utf-8') as f:
            json.dump(dados, f, indent=2)
        print(f"Sucesso: Mensagem de despacho salva em '{nome_arquivo_completo}'")
    except IOError as e:
        print(f"ERRO ao salvar arquivo '{nome_arquivo_completo}': {e}")

def encontrar_galpao_mais_proximo(cliente_coords, galpoes_db):
    """Encontra o galpão mais próximo comparando as distâncias quadradas."""
    
    def calcula_distancia_quadrada(ponto_a, ponto_b):
        return (ponto_a[0] - ponto_b[0])**2 + (ponto_a[1] - ponto_b[1])**2

    min_distancia_quadrada = float('inf')
    melhor_galpao_id = None

    if not galpoes_db:
        return None, None, None

    for galpao_id, galpao_coords in galpoes_db.items():
        dist_q = calcula_distancia_quadrada(cliente_coords, galpao_coords)
        if dist_q < min_distancia_quadrada:
            min_distancia_quadrada = dist_q
            melhor_galpao_id = galpao_id

    if melhor_galpao_id is None:
        return None, None, None

    distancia_real = math.sqrt(min_distancia_quadrada)
    coordenadas_vencedor = galpoes_db[melhor_galpao_id]
    
    return melhor_galpao_id, coordenadas_vencedor, distancia_real

# --- Função Principal (Handler da Lambda) ---

def lambda_handler(event_file_path, db_file_path, output_file_path):
    """
    Função principal que simula o handler da Lambda 1.
    Recebe os CAMINHOS COMPLETOS dos arquivos.
    """
    print("--- INICIANDO LAMBDA 1: CALCULA ROTA ---")
    
    # 1. Carregar dados da "Fila 1" (SQS)
    evento_sqs = carregar_json(event_file_path)
    if not evento_sqs or "Records" not in evento_sqs or not evento_sqs["Records"]:
        print("Erro: Evento SQS simulado está vazio ou em formato inválido.")
        return

    try:
        mensagem = evento_sqs["Records"][0]["body"]
        cliente_coords = tuple(mensagem["cliente_coords"]) 
        id_produto = mensagem["id_produto"]
        print(f"Processando Pedido: Produto '{id_produto}' para cliente em {cliente_coords}")
    except (KeyError, TypeError, IndexError) as e:
        print(f"Erro ao processar mensagem SQS: {e}")
        return

    # 2. Carregar dados do "Banco de Dados" (DynamoDB)
    galpoes = carregar_json(db_file_path)
    if not galpoes:
        print("Erro: Banco de dados de galpões não pôde ser carregado.")
        return

    # 3. Executar a lógica de cálculo
    (id_vencedor, coords_vencedor, dist_vencedor) = encontrar_galpao_mais_proximo(cliente_coords, galpoes)

    if not id_vencedor:
        print("Não foi possível encontrar um galpão de destino.")
        return

    print(f"Cálculo Concluído: Melhor galpão é '{id_vencedor}' a {dist_vencedor:.2f} unidades.")

    # 4. Enviar mensagem para a "Fila 2" (gravar arquivo de saída)
    mensagem_despacho = {
        "id_produto": id_produto,
        "galpao_destino": id_vencedor,
        "coordenadas_destino": coords_vencedor,
        "distancia_calculada": dist_vencedor
    }
    
    salvar_json(mensagem_despacho, output_file_path)
    print("--- FINALIZANDO LAMBDA 1 ---")


# --- Ponto de Entrada da Simulação ---
if __name__ == "__main__":
    
    # 3. Usa o BASE_DIR para montar os caminhos corretos
    #    (O operador / é usado pelo pathlib para juntar caminhos)
    ARQUIVO_FILA_1 = BASE_DIR / "1_simula_fila_entrada.json"
    ARQUIVO_DB = BASE_DIR / "2_simula_db_galpoes.json"
    ARQUIVO_FILA_2 = BASE_DIR / "4_simula_fila_despacho.json" # Arquivo de SAÍDA

    # Executa a função principal da Lambda
    lambda_handler(ARQUIVO_FILA_1, ARQUIVO_DB, ARQUIVO_FILA_2)