# llm_chat.py
from __future__ import annotations
import os
from typing import List, Optional, Dict, Any, Protocol
from dotenv import load_dotenv

load_dotenv()

# ---------- Backend Interface ----------

class ChatBackend(Protocol):
    def start(self,
              system_instruction: Optional[str] = None,
              history: Optional[List[Dict[str, Any]]] = None) -> None: ...
    def send(self, message: str) -> str: ...
    def history(self) -> List[Dict[str, Any]]: ...
    def reset(self) -> None: ...

# ---------- Gemini backend (google-generativeai) ----------

class GeminiBackend(ChatBackend):
    """
    Requires: pip install google-generativeai
    Env: GEMINI_API_KEY, (optional) GEMINI_MODEL
    """
    def __init__(self,
                 api_key: Optional[str] = None,
                 model_name: Optional[str] = None,
                 generation_config: Optional[Dict[str, Any]] = None,
                 safety_settings: Optional[Dict[str, Any]] = None):
        api_key = api_key or os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise ValueError("Missing GEMINI_API_KEY.")
        import google.generativeai as genai
        genai.configure(api_key=api_key)

        self._genai = genai
        self._model_name = model_name or os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
        self._generation_config = generation_config
        self._safety_settings = safety_settings

        self._model = self._genai.GenerativeModel(
            model_name=self._model_name,
            generation_config=self._generation_config,
            safety_settings=self._safety_settings,
        )
        self._chat = None  # set in start()

    def start(self,
              system_instruction: Optional[str] = None,
              history: Optional[List[Dict[str, Any]]] = None) -> None:
        if system_instruction is not None:
            self._model = self._genai.GenerativeModel(
                model_name=self._model_name,
                system_instruction=system_instruction,
                generation_config=self._generation_config,
                safety_settings=self._safety_settings,
            )
        self._chat = self._model.start_chat(history=history or [])

    def send(self, message: str) -> str:
        if self._chat is None:
            self.start()
        resp = self._chat.send_message(message)
        return getattr(resp, "text", "") or ""

    def history(self) -> List[Dict[str, Any]]:
        return [] if self._chat is None else self._chat.history

    def reset(self) -> None:
        self._chat = None

# ---------- Future stubs (OpenAI/Anthropic) ----------
# You can implement these later without changing the rest of your app.

# ---------- Azure OpenAI backend (azure-openai) ----------

class AzureOpenAIBackend(ChatBackend):
    """
    Requires: pip install openai>=1.0.0
    Env:
        AZURE_OPENAI_API_KEY       - API key for Azure OpenAI
        AZURE_OPENAI_ENDPOINT      - Your Azure endpoint (e.g. https://myresource.openai.azure.com/)
        AZURE_OPENAI_DEPLOYMENT    - Deployment name (e.g. "testdelaycategory")
        AZURE_OPENAI_API_VERSION   - Optional, defaults to "2024-12-01-preview"
    """

    def __init__(self,
                 api_key: Optional[str] = None,
                 endpoint: Optional[str] = None,
                 deployment: Optional[str] = None,
                 api_version: Optional[str] = None,
                 generation_config: Optional[Dict[str, Any]] = None):
        import os
        from openai import AzureOpenAI

        # --- Get configuration from environment or args ---
        self._api_key = api_key or os.getenv("AZURE_OPENAI_API_KEY") or os.getenv("AZURE_GPT_35_TURBO_API_KEY")
        self._endpoint = endpoint or os.getenv("AZURE_OPENAI_ENDPOINT")
        self._deployment = deployment or os.getenv("AZURE_OPENAI_DEPLOYMENT", "testdelaycategory")
        self._api_version = api_version or os.getenv("AZURE_OPENAI_API_VERSION", "2024-12-01-preview")

        if not self._api_key or not self._endpoint:
            raise ValueError("Missing required Azure OpenAI configuration: API key or endpoint.")


        self._client = AzureOpenAI(
        api_version=self._api_version,
        azure_endpoint=self._endpoint,
        api_key=self._api_key
        )


        # --- Other settings ---
        self._generation_config = generation_config or {
            "max_tokens": 500,
            "temperature": 0.7,
            "top_p": 0.3
        }
        self._chat_history: List[Dict[str, Any]] = []
        self._system_instruction: Optional[str] = None

    # ---------- Session Management ----------

    def start(self,
              system_instruction: Optional[str] = None,
              history: Optional[List[Dict[str, Any]]] = None) -> None:
        """Initialize or reset a chat session with optional system instruction and history."""
        self._system_instruction = system_instruction
        self._chat_history = []

        if system_instruction:
            self._chat_history.append({"role": "system", "content": system_instruction})

        if history:
            self._chat_history.extend(history)

    def send(self, message: str) -> str:
        """Send a user message to Azure OpenAI and get the model's response."""
        if not self._chat_history:
            self.start()

        # Append user message
        self._chat_history.append({"role": "user", "content": message})

        # Generate response
        response = self._client.chat.completions.create(
            model=self._deployment,
            messages=self._chat_history,
            **self._generation_config
        )

        reply = response.choices[0].message.content
        # Append assistant response to history
        self._chat_history.append({"role": "assistant", "content": reply})
        return reply

    def history(self) -> List[Dict[str, Any]]:
        """Return the full chat history."""
        return self._chat_history

    def reset(self) -> None:
        """Clear chat history and reset context."""
        self._chat_history = []
        self._system_instruction = None


class AnthropicBackend(ChatBackend):
    def __init__(self, **kwargs): raise NotImplementedError

# ---------- Factory ----------

def make_backend(provider: Optional[str] = None,
                 *,
                 deployment: Optional[str] = None,
                 endpoint: Optional[str] = None,
                 api_key: Optional[str] = None,
                 **kwargs) -> ChatBackend:
    """
    provider: 'gemini' | 'openai' | 'anthropic' (defaults to 'gemini')
    model_name: overrides provider default
    Also reads env: LLM_PROVIDER, LLM_MODEL
    """
    p = (provider or os.getenv("LLM_PROVIDER") or "gemini").lower()
    deployment = deployment or os.getenv("AZURE_OPENAI_DEPLOYMENT", "testdelaycategory")
    endpoint = endpoint or os.getenv("AZURE_OPENAI_ENDPOINT")
    api_key = api_key or os.getenv("AZURE_OPENAI_API_KEY") or os.getenv("AZURE_GPT_35_TURBO_API_KEY")

    if p == "gemini":
        return GeminiBackend(model_name=os.getenv("GEMINI_MODEL", "gemini-2.5-flash"), **kwargs)

    if p in ("openai", "azure", "azureopenai"):
        # Use the AzureOpenAIBackend we defined
        return AzureOpenAIBackend(
            api_key=api_key,
            endpoint=endpoint,
            deployment=deployment,
            **kwargs
        )

    if p == "anthropic":
        return AnthropicBackend(model_name=os.getenv("ANTHROPIC_MODEL", "claude-3-5-sonnet-latest"), **kwargs)

    raise ValueError(f"Unsupported provider: {p}")

# ---------- Thin app-facing wrapper ----------

class Chat:
    def __init__(self, backend: ChatBackend):
        self._b = backend
    def start(self, system_instruction: Optional[str] = None,
              history: Optional[List[Dict[str, Any]]] = None) -> None:
        self._b.start(system_instruction, history)
    def ask(self, message: str) -> str:
        return self._b.send(message)
    def history(self) -> List[Dict[str, Any]]:
        return self._b.history()
    def reset(self) -> None:
        self._b.reset()


if __name__ == "__main__":
    chat = Chat(make_backend(provider="gemini"))
    chat.start()
    print(chat.ask("Hello, how are you?"))
    print(chat.history())
    chat.reset()
    print(chat.history())