import json

PROMPT_JSON = "/app/scheduler/opendart/TB_FINANCIAL_VARIABLE/prompt/prompts.json"

with open(PROMPT_JSON, encoding="utf-8") as f:
    prompt_dict = json.load(f)

def get_prompt_by_account(account_name: str) -> str:
    """계정명에 해당하는 프롬프트 반환 (없으면 None)"""
    return prompt_dict.get(account_name)