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

class OpenAIBackend(ChatBackend):
    def __init__(self, **kwargs): raise NotImplementedError

class AnthropicBackend(ChatBackend):
    def __init__(self, **kwargs): raise NotImplementedError

# ---------- Factory ----------

def make_backend(provider: Optional[str] = None,
                 *,
                 model_name: Optional[str] = None,
                 **kwargs) -> ChatBackend:
    """
    provider: 'gemini' | 'openai' | 'anthropic' (defaults to 'gemini')
    model_name: overrides provider default
    Also reads env: LLM_PROVIDER, LLM_MODEL
    """
    p = (provider or os.getenv("LLM_PROVIDER") or "gemini").lower()
    m = model_name or os.getenv("LLM_MODEL")

    if p == "gemini":
        return GeminiBackend(model_name=m, **kwargs)
    if p == "openai":
        return OpenAIBackend(model_name=m or "gpt-4o-mini", **kwargs)
    if p == "anthropic":
        return AnthropicBackend(model_name=m or "claude-3-5-sonnet-latest", **kwargs)
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