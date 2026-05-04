import sys
import os
from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import requests

# Add current directory to path to allow imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Load environment variables
load_dotenv(override=True)

# Import the graph creation logic from the original chainlit app
from chainlit_app import get_graph_for_api

# Initialize FastAPI app
app = FastAPI()

# --- CORS Configuration ---
# This allows the Angular frontend (running on localhost:4200)
# to communicate with this FastAPI server.
origins = [
    "http://localhost:4200",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["Authorization", "Content-Type"],
)

# --- AI Graph Initialization ---
# Initialize the graph once when the server starts
try:
    graph = get_graph_for_api()
    print("AI graph initialized successfully.")
except Exception as e:
    print(f"FATAL: Could not initialize AI graph. Error: {e}")
    graph = None

# --- API Endpoint Definition ---
@app.post("/chat")
async def handle_chat(
    request: Request,
    authorization: str | None = Header(None)
):
    if graph is None:
        raise HTTPException(status_code=500, detail="AI service is not available.")

    payload = await request.json()
    user_message = payload.get("message")

    if not user_message:
        raise HTTPException(status_code=400, detail="Message cannot be empty.")

    # Here you would add logic to pass the token to the graph if needed
    # For now, we ensure the token is received and can be used.
    print(f"Received message: '{user_message}'")
    print(f"Authorization Header: {'Present' if authorization else 'Missing'}")

    # Pass the user's question and the auth token to the graph
    inputs = {
        "question": user_message,
        "token": authorization  # Pass the token to the graph state
    }
    
    try:
        # Asynchronously invoke the graph with the inputs
        result = await graph.ainvoke(inputs)
        final_answer = result.get("final_answer", "Sorry, I could not generate a response.")
        
        return {"reply": final_answer}
    except Exception as e:
        print(f"Error during graph invocation: {e}")
        raise HTTPException(status_code=500, detail=f"An error occurred while processing your request: {e}")

@app.get("/")
def read_root():
    return {"status": "AI Service is running"}

# To run this server:
# 1. Make sure you have installed all packages from requirements.txt (pip install -r requirements.txt)
# 2. Run the command in your terminal: uvicorn main:app --reload --port 8000
