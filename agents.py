import os
import google.generativeai as genai
from dotenv import load_dotenv
from typing import TypedDict
from langgraph.graph import StateGraph, END
from database import get_db
import jwt
import ast

from langchain_groq import ChatGroq
import os

# Env yüklemesi
load_dotenv(override=True)

# JWT Secret - Backend ile aynı olmalı
JWT_SECRET = "default_super_secret_key_that_is_long_enough_for_hs256"

class AgentState(TypedDict):
    question: str
    token: str
    sql_query: str
    db_result: str
    final_answer: str
    error: str

def get_user_info_from_token(token: str):
    if not token:
        return None, None
    
    # Bearer prefix'ini temizle
    if token.startswith("Bearer "):
        token = token.split(" ")[1]
    
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        email = payload.get("sub")
        role = payload.get("role")
        
        if not email:
            return None, role
            
        # DB'den email'e göre user_id bul
        db = get_db()
        if db:
            # SQLDatabase.run string döner, örn: "[(1,)]"
            result_str = db.run(f"SELECT id FROM users WHERE email = '{email}'")
            try:
                data = ast.literal_eval(result_str)
                if data and len(data) > 0 and len(data[0]) > 0:
                    return data[0][0], role
            except Exception as e:
                print(f"User ID parse hatası: {e}")
    except Exception as e:
        print(f"Token decode hatası: {e}")
    
    return None, None

class GeminiGibiCevap:
    def __init__(self, text):
        self.text = text

class GroqAdapter:
    def __init__(self):
        self.llm = ChatGroq(
            groq_api_key="YOUR_GROQ_API_KEY", 
            model_name='llama-3.3-70b-versatile',
            temperature=0.3
        )
        
    def generate_content(self, prompt):
        try:
            print("\n--- GROQ'A GIDEN MESAJ ---")
            print(prompt)
            
            cevap = self.llm.invoke(prompt)
            
            print("--- GROQ'TAN GELEN CEVAP ---")
            print(cevap.content)
            
            return GeminiGibiCevap(cevap.content)
            
        except Exception as e:
            # Hatayı terminale basıyoruz ki asıl sorunu görelim
            print("\n!!! İŞTE HATA BURADA PATLADI !!!")
            print(f"Hata Detayı: {str(e)}")
            
            # API çökmesin diye sahte bir dönüş yapıyoruz
            return GeminiGibiCevap("Sistem Hatası: Lütfen terminali kontrol et.")

# Model tanımlaması sınıfın İÇİNDE DEĞİL, hemen ALTINDA olmalı:
model = GroqAdapter()

# --- ANA GRAF FONKSİYONU BURADAN BAŞLIYOR ---

def create_graph():

    # sql_writer, create_graph'ın içinde olduğu için 1 Tab içeriden başlamalı

    def sql_writer(state: AgentState):
        """
        SİSTEM TALİMATI: Sen BALHAN Technology'nin enerjik ve zeki dijital asistanısın. 
        Kafandan veri uydurma. Sorgu yazmadan önce her zaman veritabanına bak.
        """
        db = get_db()
        if db is None:
            return {"error": "Veritabanı bağlantısı kurulamadı.", "sql_query": ""}
        
        # Kullanıcı kısıtlaması ekle
        user_id, role = get_user_info_from_token(state.get("token"))
        
        if not user_id:
            return {"error": "Selam! Sohbet edebilmemiz için önce giriş yapman gerekiyor. Seni bekliyorum!", "sql_query": ""}

        try:
            # Dinamik şema bilgisi alımı
            schema = db.get_table_info()
            
            # Dinamik Sistem Mesajı (Rol Bazlı)
            # Dinamik Sistem Mesajı (Rol Bazlı)
            if role == 'ADMIN':
                role_instruction = (
                    "YETKİ: Sen bir ADMİN asistanısın. Tam yetkin var.\n"
                    "Tüm mağaza verilerini, toplam ciroyu, tüm kullanıcıların siparişlerini ve detaylı analizleri görebilirsin.\n"
                    "Sorgularında herhangi bir kısıtlama yapmana gerek yok, genel verileri çekebilirsin."
                )
            elif role == 'STORE_OWNER':
                role_instruction = (
                    f"YETKİ: Sen bir SATICI (STORE_OWNER) asistanısın. Kısıtlı yetkin var.\n"
                    f"ÖNEMLİ: Sadece kendi mağazana/satıcılığına (ID: {user_id}) ait ürünlere ve bu ürünlerin satışlarına erişebilirsin.\n"
                    f"KATI KURAL 1: Eğer kullanıcı 'sitede en çok satan satıcı', 'diğer satıcıların cirosu', 'başka bir kullanıcının sepeti' gibi başkasına ait veriler sorarsa, "
                    f"KESİNLİKLE SQL yazma! Bunun yerine SADECE şu sorguyu döndür: SELECT 'YETKİSİZ_ERİŞİM' AS yetki_durumu;\n"
                    f"KATI KURAL 2: Kendi satışlarını, cirosunu veya ürünlerini listelerken sorgularına KESİNLİKLE 'products.seller_id = {user_id}' filtresini eklemek ZORUNDASIN. Dikkat et: 'orders' tablosundaki 'user_id' müşteridir, kendi satışlarını bulmak için 'products' tablosundaki satıcı ID'ni kullanmalısın."
                )
            else:
                role_instruction = (
                    f"YETKİ: Sen bir MÜŞTERİ (STANDART KULLANICI) asistanısın. Kısıtlı yetkin var.\n"
                    f"ÖNEMLİ: Sadece ve sadece giriş yapmış olan kendi ID'ne (ID: {user_id}) ait verilere erişebilirsin.\n"
                    f"KATI KURAL 1: Eğer kullanıcı sorusunda açıkça başka bir kullanıcının ID'sini (örneğin 14. kullanıcı), başka birinin siparişini veya başkasına ait herhangi bir bilgiyi sorarsa, "
                    f"KESİNLİKLE veritabanını tarama ve SQL YAZMA! Bunun yerine SADECE şu sorguyu döndür: SELECT 'YETKİSİZ_ERİŞİM' AS yetki_durumu;\n"
                    f"KATI KURAL 2: Eğer kendi verilerini soruyorsa (örn: son siparişim ne?), yazacağın SQL sorgusunun WHERE kısmında KESİNLİKLE 'user_id = {user_id}' filtresini kullanmak ZORUNDASIN. WHERE şartına asla başka bir rakam yazma."
                )

            prompt = (
                f"SİSTEM: BALHAN Akıllı Veritabanı Asistanısın. Samimi, enerjik ve doğrudan konuya giren birisin.\n"
                f"{role_instruction}\n\n"
                f"GÖREV: Kullanıcının sorusunu yanıtlamak için PostgreSQL sorgusu yaz.\n"
                f"Sadece SQL kodunu dön, açıklama veya markdown (```) ekleme.\n\n"
                f"MEVCUT VERİTABANI ŞEMASI:\n{schema}\n\n"
                f"ÖNEMLİ VERİTABANI İLİŞKİLERİ (BUNLARA KESİNLİKLE UY):\n"
                f"1. Satıcılar 'users' tablosundadır ve role_type = 'STORE_OWNER' şartını sağlarlar.\n"
                f"2. 'orders' tablosundaki 'user_id' kolonu SATICIYI DEĞİL, siparişi veren MÜŞTERİYİ temsil eder.\n"
                f"3. Bir satıcının ürünlerini/satışlarını bulmak için 'users' tablosunu 'products' tablosuna bağlamalısın.\n"
                f"4. Toplam satış hesabı için doğru mantık: users (satıcı) -> products (ürünler) -> order_items (sipariş detayları) olmalıdır.\n\n"
                f"KULLANICI SORUSU: {state['question']}"
            )
            
            response = model.generate_content(prompt)
            query = response.text.strip().replace("```sql", "").replace("```", "").replace("sql", "").strip()
            return {"sql_query": query, "error": ""}
        except Exception as e:
            return {"error": f"SQL Üretim Hatası: {str(e)}", "sql_query": ""}

    def sql_executor(state: AgentState):
        db = get_db()
        query = state.get("sql_query", "").strip()
        
        if db is None:
            return {"db_result": "Veritabanı bağlantısı kurulamadı."}
        
        if state.get("error"):
            return {"db_result": f"{state['error']}"}

        # SQL kontrolü: Sadece SELECT sorgularına izin ver ve düz metinleri ayıkla
        if not query.upper().startswith("SELECT"):
            print(f"--- ⚠️  NOT A SQL QUERY ---\n{query}\n-----------------------")
            # Eğer sorgu değilse, bu muhtemelen bir ret veya açıklama mesajıdır.
            # Bunu doğrudan db_result olarak ata, böylece answer_generator bunu kullanabilir.
            return {"db_result": query}

        try:
            print(f"\n--- 🔍 EXECUTING SQL ---\n{query}\n-----------------------")
            result = db.run(query)
            print(f"--- 📝 DB RESULT ---\n{result}\n--------------------")
            return {"db_result": str(result)}
        except Exception as e:
            print(f"--- ❌ SQL ERROR ---\n{str(e)}\n--------------------")
            return {"error": f"Sorgu çalıştırılırken bir sorun oldu.", "db_result": ""}

    def answer_generator(state: AgentState):
        try:
            db_context = state.get("db_result", "Veri bulunamadı.")
            error_context = state.get("error", "")
            
            prompt = (
                f"ROLÜN: Sen BALHAN Teknoloji'nin enerjik, samimi ve yardımsever dijital asistanısın. "
                f"Müşterilerle bir arkadaş gibi ama saygılı konuşursun.\n\n"
                f"YASAKLI KALIPLAR: 'İsteğinizi anladım', 'Müşteri gizliliği prensipleri gereği', "
                f"'Size nasıl yardımcı olabilirim?', 'Anladığım kadarıyla' gibi robotik cümleleri ASLA kullanma.\n\n"
                f"CEVAP FORMATI:\n"
                f"- Destan yazma, kısa ve vurucu ol.\n"
                f"- Madde imleri (bullet points) ve **kalın yazılar** kullan.\n"
                f"- Ürün önerirken tek bir cümleyle neden kullanıcının tarzına uyduğunu söyle.\n\n"
                f"KATI KURALLAR:\n"
                f"- SADECE 'Veritabanı Verisi' bölümündeki bilgileri kullan.\n"
                f"- Eğer 'Veritabanı Verisi' BOŞSA, kesinlikle bilgi uydurma ve 'Şu an sistemde satış yapan bir satıcı bulunmuyor' gibi dürüst bir cevap ver.\n"
                f"- Asla ama asla dış dünyadaki başka markalardan (Teknosa, Vatan, Hepsiburada vb.) bahsetme. SADECE BALHAN Teknoloji'yi temsil ediyorsun.\n\n"
                f"VERİ BAĞLAMI:\n"
                f"Kullanıcı Sorusu: {state['question']}\n"
                f"Veritabanı Verisi: {db_context}\n"
                f"Hata (Varsa): {error_context}\n\n"
                f"TALİMAT: Yukarıdaki kurallara göre Türkçe cevap ver."
            )
            
            response = model.generate_content(prompt)
            return {"final_answer": response.text}
        except Exception as e:
            print(f"🚨 GERÇEK HATA BURADA: {str(e)}") # BÖYLEYDİ, BUNU EKLE!
            return {"final_answer": "Ups! Cevap verirken bir şeyler ters gitti. Tekrar dener misin?"}

    # Graf Yapılandırması (Zorunlu Akış: Writer -> Executor -> Generator)
    workflow = StateGraph(AgentState)
    
    workflow.add_node("sql_writer", sql_writer)
    workflow.add_node("sql_executor", sql_executor)
    workflow.add_node("answer_generator", answer_generator)
    
    workflow.set_entry_point("sql_writer") # Her soru önce SQL yazıcıya gider
    
    workflow.add_edge("sql_writer", "sql_executor")
    workflow.add_edge("sql_executor", "answer_generator")
    workflow.add_edge("answer_generator", END)
    
    return workflow.compile()
