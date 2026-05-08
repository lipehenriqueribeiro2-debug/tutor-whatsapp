import os
import json
import re
import requests
import base64
import io
import pypdf
import docx
from PIL import Image
from fastapi import FastAPI, Request
from litellm import completion
from twilio.rest import Client
from prefect.variables import Variable
from prefect.client.orchestration import get_client
from prefect.client.schemas.schedules import CronSchedule
from prefect.client.schemas.actions import DeploymentUpdate

app = FastAPI()

# ==========================================
# 1. IA REGENTE (LLM-AS-A-JUDGE)
# ==========================================
def consultar_ia_regente(mensagem_usuario: str, especialistas: dict) -> dict:
    print("🧠 Regente analisando a solicitação...")
    modelo_regente = especialistas["padrao"]
    
    prompt_regente = f"""Você é a IA Regente de um estudante de Engenharia.
    Sua missão é fazer a triagem da seguinte mensagem: "{mensagem_usuario}"
    
    Tome as seguintes decisões:
    1. TIPO_SOLICITACAO: É uma "CONVERSA_SIMPLES", uma "QUESTAO_COMPLEXA", ou um comando para "MUDAR_HORARIO"?
    2. ESPECIALISTA: Se for COMPLEXA, quem deve resolver?
       - "DEEPSEEK": Para cálculos, engenharia, física, programação, análise de dados.
       - "CLAUDE": Para textos densos, estratégias de negócios, worldbuilding/RPG.
       - "PADRAO": Se for simples ou MUDAR_HORARIO.
    3. CRON_EXTRAIDO: Se for MUDAR_HORARIO, extraia para cron diário (ex: "0 19 * * *"). Senão, deixe "".
    4. RESPOSTA_DIRETA: Se for CONVERSA_SIMPLES ou MUDAR_HORARIO, escreva a resposta final para o usuário aqui. Se for QUESTAO_COMPLEXA, deixe em branco.
    
    Responda ESTRITAMENTE neste formato JSON:
    {{
        "tipo_solicitacao": "SIMPLES|COMPLEXO|MUDAR_HORARIO",
        "especialista": "PADRAO|DEEPSEEK|CLAUDE",
        "cron_extraido": "",
        "resposta_direta": "Sua resposta aqui"
    }}"""

    resposta = completion(
        model=modelo_regente, 
        messages=[{"role": "user", "content": prompt_regente}],
        extra_headers={"HTTP-Referer": "http://localhost:8000", "X-Title": "Tutor IA Regente"}
    )
    texto_bruto = resposta.choices[0].message.content
    
    match = re.search(r'\{.*\}', texto_bruto, re.DOTALL)
    if match:
        return json.loads(match.group(0))
    else:
        raise ValueError("A Regente falhou ao estruturar a resposta em JSON.")

# ==========================================
# 2. CONEXÕES EXTERNAS (TWILIO E PREFECT)
# ==========================================
def enviar_resposta_twilio(mensagem: str, telefone_destino: str, config: dict):
    twilio_cfg = config["apis"]["twilio"]
    client = Client(twilio_cfg["account_sid"], twilio_cfg["auth_token"])
    numero_sandbox = twilio_cfg["numero_sandbox"]

    try:
        tamanho_max = 1500
        pedacos = [mensagem[i:i+tamanho_max] for i in range(0, len(mensagem), tamanho_max)]
        for pedaco in pedacos:
            client.messages.create(from_=numero_sandbox, body=pedaco, to=telefone_destino)
            print(f"✉️ Mensagem enviada ({len(pedaco)} caracteres)")
    except Exception as e:
        print(f"Erro no Twilio: {e}")

async def alterar_horario_prefect(nova_expressao_cron: str):
    try:
        async with get_client() as client:
            deployments = await client.read_deployments()
            deployment_alvo = next((d for d in deployments if d.name == "agendamento-estudo-diario"), None)

            if not deployment_alvo:
                print("Deployment não encontrado no Prefect.")
                return False

            # Cria a nova agenda
            nova_agenda = CronSchedule(cron=nova_expressao_cron, timezone="America/Sao_Paulo")
            
            # PREFECT 3.X: Empacota a agenda dentro do objeto DeploymentUpdate
            atualizacao = DeploymentUpdate(schedules=[{"schedule": nova_agenda, "active": True}])
            
            # Envia o pacote de atualização fechado para o servidor
            await client.update_deployment(deployment_id=deployment_alvo.id, deployment=atualizacao)
            
            return True
    except Exception as e:
        print(f"Erro no Prefect: {e}")
        return False

# ==========================================
# 3. ROTA PRINCIPAL DO WEBHOOK
# ==========================================
@app.post("/webhook")
async def receive_message(request: Request):
    # LAZY LOADING ASSÍNCRONO: O app só carrega o cofre quando chega mensagem!
    config = await Variable.get("tutor_config")
    
    # Injeta as chaves na memória
    os.environ["OPENROUTER_API_KEY"] = config["apis"]["openrouter"]["api_key"]
    os.environ["GEMINI_API_KEY"] = config["apis"]["gemini"]["api_key"]
    
    especialistas = config["agentes_ia"]
    
    form_data = await request.form()
    remetente = form_data.get("From")
    texto = form_data.get("Body", "").strip()
    num_media = int(form_data.get("NumMedia", "0"))
    
    if not remetente:
        return {"status": "error"}

    if num_media > 0:
        url_arquivo = form_data.get("MediaUrl0")
        tipo_arquivo = form_data.get("MediaContentType0").lower()
        
        twilio_cfg = config["apis"]["twilio"]
        res = requests.get(url_arquivo, auth=(twilio_cfg["account_sid"], twilio_cfg["auth_token"]))
        prompt_ia = texto if texto else "Analise e explique este conteúdo em detalhes."

        try:
            if "image" in tipo_arquivo:
                enviar_resposta_twilio("📸 Analisando imagem com Gemini...", remetente, config)
                img = Image.open(io.BytesIO(res.content)).convert("RGB")
                buffer = io.BytesIO()
                img.save(buffer, format="JPEG")
                img_b64 = base64.b64encode(buffer.getvalue()).decode("utf-8")
                
                res_ia = completion(
                    model=especialistas["gemini"],
                    messages=[{
                        "role": "user",
                        "content": [
                            {"type": "text", "text": prompt_ia},
                            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}}
                        ]
                    }],
                    extra_headers={"HTTP-Referer": "http://localhost:8000", "X-Title": "Tutor Visao"}
                )
                enviar_resposta_twilio(res_ia.choices[0].message.content, remetente, config)

            elif "pdf" in tipo_arquivo:
                enviar_resposta_twilio("📄 Lendo documento PDF...", remetente, config)
                leitor = pypdf.PdfReader(io.BytesIO(res.content))
                texto_extraido = "".join([pagina.extract_text() + "\n" for pagina in leitor.pages])
                
                prompt_completo = f"Documento fornecido:\n{texto_extraido[:8000]}\n\nPedido do usuário: {prompt_ia}"
                res_ia = completion(model=especialistas["deepseek"], messages=[{"role": "user", "content": prompt_completo}])
                enviar_resposta_twilio(res_ia.choices[0].message.content, remetente, config)

            elif "word" in tipo_arquivo or "docx" in tipo_arquivo:
                enviar_resposta_twilio("📝 Lendo documento Word...", remetente, config)
                doc = docx.Document(io.BytesIO(res.content))
                texto_extraido = "\n".join([paragrafo.text for paragrafo in doc.paragraphs])
                
                prompt_completo = f"Documento fornecido:\n{texto_extraido[:8000]}\n\nPedido do usuário: {prompt_ia}"
                res_ia = completion(model=especialistas["deepseek"], messages=[{"role": "user", "content": prompt_completo}])
                enviar_resposta_twilio(res_ia.choices[0].message.content, remetente, config)
                
            else:
                enviar_resposta_twilio("⚠️ Formato de arquivo não suportado no momento.", remetente, config)

        except Exception as e:
            enviar_resposta_twilio(f"⚠️ Erro ao processar o arquivo: {e}", remetente, config)
            
        return {"status": "ok"}

    if texto:
        try:
            ordens = consultar_ia_regente(texto, especialistas)
            tipo = ordens.get("tipo_solicitacao", "SIMPLES")
            
            if tipo == "MUDAR_HORARIO":
                cron = ordens.get("cron_extraido")
                sucesso = await alterar_horario_prefect(cron)
                msg = f"🕰️ {ordens.get('resposta_direta')}" if sucesso else "⚠️ Erro ao atualizar a rotina no Prefect."
                enviar_resposta_twilio(msg, remetente, config)
                
            elif tipo in ["SIMPLES", "CONVERSA_SIMPLES"]:
                enviar_resposta_twilio(ordens.get("resposta_direta", "Ok!"), remetente, config)
                
            elif tipo in ["COMPLEXO", "QUESTAO_COMPLEXA"]:
                nome_especialista = ordens.get("especialista", "DEEPSEEK").lower()
                modelo_alvo = especialistas.get(nome_especialista, especialistas["deepseek"])
                
                enviar_resposta_twilio(f"🔄 Encaminhando para especialista ({nome_especialista.upper()})...", remetente, config)
                
                res_especialista = completion(
                    model=modelo_alvo, 
                    messages=[{"role": "user", "content": texto}],
                    extra_headers={"HTTP-Referer": "http://localhost:8000", "X-Title": "Tutor Especialista"}
                )
                enviar_resposta_twilio(res_especialista.choices[0].message.content, remetente, config)

        except Exception as e:
            print(f"Erro no Agentic Workflow: {e}")
            res_ia = completion(model=especialistas["padrao"], messages=[{"role": "user", "content": texto}])
            enviar_resposta_twilio(res_ia.choices[0].message.content, remetente, config)

    return {"status": "ok"}