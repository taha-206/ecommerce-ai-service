import sys
import os

# This file is the original Chainlit app, preserved for its logic.
# It will be imported by the new FastAPI server.

# Mevcut klasörü Python'un dosya arama listesine ekle
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import nest_asyncio
nest_asyncio.apply()

import chainlit as cl
from dotenv import load_dotenv

# Env yüklemesi
load_dotenv(override=True)

# This function will now be callable from our FastAPI endpoint
def get_graph_for_api():
    from agents import create_graph
    return create_graph()

@cl.on_chat_start
async def start():
    try:
        from agents import create_graph
        graph = create_graph()
        cl.user_session.set("graph", graph)
        await cl.Message(content="Selam! E-ticaret veritabanın hakkında ne bilmek istersin?").send()
    except Exception as e:
        await cl.Message(content=f"Başlatma hatası: {str(e)}").send()

@cl.on_message
async def main(message: cl.Message):
    graph = cl.user_session.get("graph")
    inputs = {"question": message.content}
    try:
        result = await graph.ainvoke(inputs)
        final_answer = result.get("final_answer", "Cevap üretilemedi.")
        await cl.Message(content=final_answer).send()
    except Exception as e:
        await cl.Message(content=f"Hata oluştu: {str(e)}").send()
