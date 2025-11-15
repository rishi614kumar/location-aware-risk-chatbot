"""
Centralized prompt and system context definitions for LLM interactions.
"""
from config.settings import DATASET_DESCRIPTIONS
# --- Prompts for app.py ---

def get_first_message() -> str:
    """
    Returns the initial message prompt for the LLM chat in app.py.
    Edit the template below to change the initial message.
    """
    return (
        '''Hi there! I can help you analyze environmental and compliance risks for any NYC address, street span, or neighborhood.'''
    )

def get_system_prompt() -> str:
    """
    Returns the system prompt for the LLM chat in app.py.
    Edit the template below to change the system context.
    """
    return (
        f'''You are a helpful assistant for NYC risk and compliance questions.
        Respond conversationally and concisely.
        When you receive a META-PROMPT following this format $(META-PROMPT: <text>) you will consider that text as an instruction for how to respond to the user.
        your first message is {get_first_message()}
        '''.strip()
    )

def get_conversational_meta_prompt(user_text: str) -> str:
    """
    Returns the conversational meta-prompt for the LLM response in app.py.
    Edit the template below to change how the LLM responds to user input.
    """
    return (
        f'''$(META-PROMPT: respond conversationally to the user.
        do not actually answer any queries that the user has,later stages will address the query.
        you just need to acknowledge the user's input and let them know you are processing it.
        query: {user_text})'''
    )

def get_followup_prompt(result, risk_summary) -> str:
    """
    Returns a prompt for the LLM to generate a context-aware follow-up question.
    Pass in the parsed results and the risk summary.
    """
    cats = ', '.join(result.get('categories', []))
    datasets = ', '.join(result.get('dataset_names', []))
    addresses = ', '.join(a.get('raw', '') for a in result.get('address', []))
    return (
        f'''$(META-PROMPT: Based on the parsed categories ({cats}), datasets ({datasets}), addresses ({addresses}), and the following risk summary:\n{risk_summary}\n\nSuggest a specific, context-aware follow-up question for the user. Only ask about relevant details, trends, or comparisons that would help the user get more insight. Only ask the user things that can be answered from the information available to you. Keep in mind that this is at the end of a conversation and should flow naturally from all the given information.)'''
    )

def get_loading_datasets_prompt(handler) -> str:
    """
    Returns a string describing what datasets are being loaded, based on the handler object.
    """
    if not hasattr(handler, 'names') or not handler.names:
        names = ["No datasets to load."]
    else:
        names = handler.names
    return (
        f'''$(META-PROMPT: Based on the following datasets being loaded: {', '.join(names)}, inform the user that the data is being fetched and processed. This may take a moment.)'''
    )

def get_conversational_fallback_prompt() -> str:
    """
    Returns a fallback conversational prompt when no data analysis is needed.
    """
    return (
        "$(META-PROMPT: Continue the conversation with the user. No data analysis is needed. Respond helpfully and conversationally.)"
    )

def get_decision_prompt(user_text, chat_history):
    """
    Prompt for LLM to decide between conversational mode and data querying mode.
    """
    return (
        f'''$(META-PROMPT: Given the user's query: '{user_text}' and the chat history: '{chat_history}', decide if you should enter conversational mode (just answer the user's question) or data querying mode (parse new addresses, query new datasets, and analyze risk). 
        If the user is just following up on previous information or asking general questions that you can already answer, choose conversational mode. 
        If the user is asking for specific risk information about a location, datasets, or categories that you cannot already answer based on the chat history, choose data querying mode.
        If the user is asking to see the data, choose data querying mode.
        The datasets you have access to are geo-referenced datasets related to NYC risk and compliance and are as follows: {DATASET_DESCRIPTIONS}
        Respond with either 'conversational' or 'data_query'.)'''
    )

def get_risk_summary_decision_prompt(user_text, chat_history, parsed_result):
    """
    Prompt for LLM to decide if a risk summary is needed based on query, history, and parsed results.
    """
    return (
        f'''$(META-PROMPT: Given the user's query: '{user_text}', chat history: '{chat_history}', and parsed results: '{parsed_result}', decide if a risk summary should be generated. 
        If the user is asking for specific risk information that is not already covered in the chat history or parsed results, choose 'risk_summary_needed'.
        If the user is asking for general information or follow-ups that do not require additional risk analysis, choose 'risk_summary_not_needed'.
        Respond with either 'risk_summary_needed' or 'risk_summary_not_needed'.)'''
    )

def get_conversational_answer_prompt(user_text, chat_history=None):
    """
    Returns a prompt for the LLM to directly answer the user's question in conversational mode.
    Optionally includes chat history for context.
    """
    history_part = f"\nChat history: {chat_history}" if chat_history else ""
    return (
        f"$(META-PROMPT: Answer the user's question directly and helpfully. Be accurate and conversational, answer based on their query and the chat history, if applicable.\nUser query: {user_text}{history_part})"
    )

def get_show_data_decision_prompt(user_text, chat_history, parsed_result):
    """
    Prompt for LLM to decide if the user wants to see the actual data preview.
    Respond with either 'show_data' or 'hide_data'.
    """
    return (
        f"$(META-PROMPT: Given the user's query: '{user_text}', chat history: '{chat_history}', and parsed results: '{parsed_result}', decide if the user wants to see the actual data preview. If the user is asking to see the data, show it. Otherwise, hide it. Respond with either 'show_data' or 'hide_data'.)"
    )

def get_reuse_parsed_decision_prompt(user_text, chat_history, last_parsed_result):
    """
    Prompt for LLM to decide if the last parsed datasets/addresses should be reused or reparsed.
    Respond with either 'reuse' or 'reparse'.
    """
    return (
        f"$(META-PROMPT: Given the user's query: '{user_text}', chat history: '{chat_history}', and the last parsed result: '{last_parsed_result}', decide if you should reuse the last parsed datasets and addresses, or reparse from the new query. Respond with either 'reuse' or 'reparse'.)"
    )

