# E-commerce AI Service

This is the AI service component of the E-commerce application, providing intelligent chat and data analysis capabilities.

## Tech Stack

- **Framework**: FastAPI / Chainlit
- **Language**: Python 3.12
- **AI Framework**: LangChain / LangGraph
- **Models**: Groq (Llama), Google Gemini
- **Database**: SQLite / PostgreSQL
- **Data Processing**: Pandas, NumPy

## Features

- **Intelligent Chatbot**: Interactive AI assistant built with Chainlit.
- **Natural Language to SQL**: Converts user queries into database queries using LangGraph.
- **ETL Pipelines**: Automated data import and processing from various datasets.
- **Role-Based Access Control**: Secure data access based on user roles (Admin, Store Owner, Customer).
- **Multi-Model Support**: Integration with multiple LLM providers.

## Getting Started

### Prerequisites

- Python 3.12
- Virtual Environment (recommended)

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/taha-206/ecommerce-ai-service.git
   ```
2. Navigate to the project directory:
   ```bash
   cd ecommerce-ai-service
   ```
3. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
4. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### Configuration

Create a `.env` file in the root directory:
```env
GOOGLE_API_KEY=YOUR_GEMINI_API_KEY
GROQ_API_KEY=YOUR_GROQ_API_KEY
JWT_SECRET=your_shared_jwt_secret
```

### Running the Service

To start the Chainlit application:
```bash
chainlit run chainlit_app.py
```

To run the main API:
```bash
python main.py
```
