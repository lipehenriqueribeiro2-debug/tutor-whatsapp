import os
import requests
from litellm import completion
from twilio.rest import Client
from prefect import flow, task
from prefect.variables import Variable

# ==========================================
# UTILITÁRIOS GERAIS (LAZY LOADING)
# ==========================================
def carregar_configuracoes():
    """Busca o JSON no cofre do Prefect apenas no momento da execução."""
    config = Variable.get("tutor_config")
    
    # Injeta as chaves na variável de ambiente para o LiteLLM
    os.environ["OPENROUTER_API_KEY"] = config["apis"]["openrouter"]["api_key"]
    os.environ["GEMINI_API_KEY"] = config["apis"]["gemini"]["api_key"]
    return config

@task(retries=2, retry_delay_seconds=10)
def enviar_mensagem_whatsapp(mensagem: str):
    config = carregar_configuracoes()
    twilio_cfg = config["apis"]["twilio"]
    client = Client(twilio_cfg["account_sid"], twilio_cfg["auth_token"])
    telefone_destino = config["usuario"]["numero_whatsapp"]
    numero_sandbox = twilio_cfg["numero_sandbox"]

    try:
        tamanho_max = 1500
        pedacos = [mensagem[i:i+tamanho_max] for i in range(0, len(mensagem), tamanho_max)]
        for pedaco in pedacos:
            client.messages.create(from_=numero_sandbox, body=pedaco, to=telefone_destino)
            print(f"✉️ [Prefect] Alerta/Relatório enviado.")
    except Exception as e:
        print(f"Erro no Twilio via Prefect: {e}")

# ==========================================
# FLUXO 3: RELATÓRIO SEMANAL (BUSINESS INTELLIGENCE)
# ==========================================
@task(retries=1)
def gerar_relatorio_bi():
    config = carregar_configuracoes()
    prompt = """
    Você é o assistente de inteligência de negócios e engenharia do usuário.
    Gere um relatório de planejamento semanal focado em produtividade.
    
    Estruture a resposta em 3 partes curtas e diretas para leitura no WhatsApp:
    1. 🎯 Foco em Engenharia (Sugira um tópico prático de revisão, ex: cálculos de motores, circuitos, ou normativas do setor elétrico).
    2. 📈 Planejamento Estratégico (Use o formato 5W2H para definir um mini-objetivo para o projeto de locação de máquinas/geradores na região Nordeste).
    3. 💻 Dados & Automação (Um desafio rápido de modelagem em Power BI, DAX ou Python).
    
    Seja executivo, analítico e use emojis com moderação. Formate em Markdown.
    """
    
    resposta = completion(
        model=config["agentes_ia"]["claude"], 
        messages=[{"role": "user", "content": prompt}],
        extra_headers={"HTTP-Referer": "http://localhost:8000", "X-Title": "Tutor BI"}
    )
    return resposta.choices[0].message.content

@flow(name="Relatorio-Semanal-BI")
def fluxo_relatorio_semanal():
    print("Iniciando geração do Relatório Semanal...")
    relatorio = gerar_relatorio_bi()
    enviar_mensagem_whatsapp(f"📊 *SEU RELATÓRIO ESTRATÉGICO DA SEMANA* 📊\n\n{relatorio}")
    print("Relatório Semanal finalizado com sucesso.")

# ==========================================
# FLUXO 4: MONITOR DE INFRAESTRUTURA (DEVOPS)
# ==========================================
@task
def checar_limites_openrouter():
    config = carregar_configuracoes()
    api_key = config["apis"]["openrouter"]["api_key"]
    headers = {"Authorization": f"Bearer {api_key}"}
    
    try:
        response = requests.get("https://openrouter.ai/api/v1/auth/key", headers=headers)
        if response.status_code == 200:
            dados = response.json().get("data", {})
            limite = dados.get("limit")
            uso = dados.get("usage", 0)
            
            if limite is None:
                return {"alerta": False, "msg": f"🟢 OpenRouter: Consumo atual de ${uso:.4f}."}
                
            saldo_restante = limite - uso
            if saldo_restante < 1.0: 
                return {"alerta": True, "msg": f"🔴 *ALERTA DE INFRAESTRUTURA*\n\nSaldo do OpenRouter está crítico!\nRestante: ${saldo_restante:.4f}\nUso: ${uso:.4f}"}
            else:
                return {"alerta": False, "msg": f"🟢 OpenRouter OK. Saldo restante: ${saldo_restante:.4f}"}
        else:
            return {"alerta": True, "msg": f"🔴 Erro ao consultar OpenRouter: Status {response.status_code}"}
    except Exception as e:
         return {"alerta": True, "msg": f"🔴 Falha de conexão com a API de verificação: {e}"}

@flow(name="Monitor-Infraestrutura")
def fluxo_monitoramento_infra():
    print("Iniciando varredura de infraestrutura...")
    status_api = checar_limites_openrouter()
    
    if status_api["alerta"]:
        enviar_mensagem_whatsapp(status_api["msg"])
        print("Alerta de limite disparado!")
    else:
        print(status_api["msg"])
        

# ==========================================
# FLUXO 5: LEMBRETE DIÁRIO DE ESTUDOS
# ==========================================
@flow(name="Lembrete-Estudo-Diario")
def fluxo_estudo_diario():
    print("Iniciando envio do lembrete de estudos...")
    enviar_mensagem_whatsapp(
        "📚 *HORA DO ESTUDO!*\n\n"
        "Os relatórios da UFBA e as documentações de Engenharia Elétrica te esperam. "
        "Foco total agora, desligue as distrações e bora pra cima! ⚡"
    )
    print("Lembrete de estudos enviado com sucesso.")