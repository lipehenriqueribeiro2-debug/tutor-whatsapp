import os
from prefect import flow, task
from litellm import completion
from twilio.rest import Client
from prefect.variables import Variable

CONFIG = Variable.get("tutor_config")
os.environ["OPENROUTER_API_KEY"] = CONFIG["apis"]["openrouter"]["api_key"]
os.environ["GEMINI_API_KEY"] = CONFIG["apis"]["gemini"]["api_key"]

@task(retries=2, retry_delay_seconds=5)
def gerar_conteudo_ia(tema_do_dia: str) -> str:
    modelo_escolhido = CONFIG["agentes_ia"]["deepseek"] # Ajustado para o especialista
    
    print(f"🧠 Consultando IA ({modelo_escolhido}) sobre: {tema_do_dia}...")
    prompt = f"""Você é meu tutor pessoal de engenharia e tecnologia via WhatsApp.
    O tema de estudo de hoje é: {tema_do_dia}.
    
    Por favor, gere uma mensagem curta e amigável contendo:
    1. Um resumo muito breve do conceito (1 parágrafo).
    2. Uma sugestão de termo para pesquisar no YouTube.
    3. Uma questão conceitual rápida para eu responder.
    
    Formate usando negrito (*) e itálico (_) do WhatsApp."""
    
    resposta = completion(model=modelo_escolhido, messages=[{"role": "user", "content": prompt}])
    return resposta.choices[0].message.content

@task
def enviar_mensagem_whatsapp(mensagem: str, telefone_destino: str):
    print("Enviando mensagem para o WhatsApp via Twilio...")
    twilio_cfg = CONFIG["apis"]["twilio"]
    client = Client(twilio_cfg["account_sid"], twilio_cfg["auth_token"])
    
    try:
        message = client.messages.create(
            from_=twilio_cfg["numero_sandbox"],
            body=mensagem,
            to=telefone_destino
        )
        print(f"✅ Mensagem enviada com sucesso! Twilio SID: {message.sid}")
    except Exception as e:
        print(f"❌ Erro ao enviar: {e}")

@flow(name="Tutor Diário", log_prints=True)
def fluxo_de_estudos(tema: str = "Engenharia de Dados"):
    telefone = CONFIG["usuario"]["numero_whatsapp"]
    conteudo = gerar_conteudo_ia(tema)
    enviar_mensagem_whatsapp(conteudo, telefone)

if __name__ == "__main__":
    pass