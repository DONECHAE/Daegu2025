import openai
import json
import re
import time
import os
from uuid import uuid4
from typing import Optional, Dict

from setting.inject import provision_inject_orm
from setting.database_orm import SessionLocal
from infrastructure.queryFactory.base_orm import BaseQueryFactory
from scheduler.opendart.TB_FINANCIAL_VARIABLE.prompt.prompt_loader import get_prompt_by_account
from db.public.models import TB_FINANCIAL_VARIABLE, TB_DISCLOSURE_INFORMATION
from Logger import logger , request_context
from error.email.email_logger import attach_error_email_handler

"""
    * TB_FINANCIAL_VARIABLE LLM 추출 스케줄러 
        - 스케줄러 주기: TB_FINANCIAL_STATEMENTS 와 동일

"""
class SchedulerServiceTBFinancialVariableLLM:
    def __init__(self, model_default: str = "gpt-5"):
        cfg = provision_inject_orm()
        self.api_key = getattr(cfg, "OPENAI_API_KEY", None) or os.getenv("OPENAI_API_KEY")
        self.model_default = model_default
        if self.api_key:
            self.client = openai.OpenAI(api_key=self.api_key)
        else:
            # 환경변수에 설정돼 있으면 SDK가 자동 인식, 없으면 이후 호출 시 에러 로그 남김
            self.client = openai.OpenAI()
        attach_error_email_handler(logger, service_name="WEB:FINANCIAL_VARIABLE_LLM 스케줄러")
        try:
            with open(
                "/app/scheduler/opendart/TB_FINANCIAL_VARIABLE/prompt/LLM_Keywords.json",
                "r",
                encoding="utf-8"
            ) as f:
                self.account_keywords = json.load(f)
        except Exception as e:
            logger.warn(f"[LLM] 키워드 맵 로드 실패: {e} → 빈 맵 사용")
            self.account_keywords = {}
        self._loan_cache = {}

    def _clean_html_text(self, html_text: str) -> str:
        return re.sub(r"\s+", " ", html_text).strip()

    def _extract_snippet_near_keywords(self, text: str, account_name: str, window: int = 1000) -> str:
        keywords = self.account_keywords.get(account_name, [account_name])
        logger.debug(f"[LLM] 키워드 매핑({account_name}): {keywords}")

        ranges = []
        for kw in keywords:
            for match in re.finditer(re.escape(kw), text):
                idx = match.start()
                start = max(0, idx - window)
                end = min(len(text), idx + len(kw) + window)
                ranges.append((start, end))
        if not ranges:
            return ""

        ranges.sort()
        merged = []
        for s, e in ranges:
            if not merged or merged[-1][1] < s:
                merged.append((s, e))
            else:
                merged[-1] = (merged[-1][0], max(merged[-1][1], e))
        return " ".join(text[s:e] for s, e in merged)

    def _build_chat_kwargs_v5(self, messages: list) -> dict:
        return {
            "model": self.model_default,
            "messages": messages,
            "max_completion_tokens": 100,
            "reasoning_effort": "minimal",
        }

    def _build_chat_kwargs_v41(self, messages: list) -> dict:
        return {
            "model": self.model_default,
            "messages": messages,
            "max_tokens": 100,
            "temperature": 0.0,
        }

    def _build_chat_kwargs(self, messages: list) -> dict:
        m = str(self.model_default)
        if m.startswith("gpt-5"):
            return self._build_chat_kwargs_v5(messages)
        if m.startswith("gpt-4.1"):
            return self._build_chat_kwargs_v41(messages)
        return self._build_chat_kwargs_v41(messages)

    def _call_llm(self, comment_text: str, prompt: str, retry: int = 0) -> str:
        try:
            default_system_prompt = (
                "당신은 재무제표 주석의 숫자 데이터를 정확하게 추출하는 정보 추출 전문가입니다. "
                "HTML 형태의 재무제표 주석에서 특정 항목의 '당기말' 또는 '당기/당분기/당반기' 금액만 숫자로 추출하세요. "
                "'전기' 금액은 제외합니다. 금액은 항상 '원' 단위로 환산(천원→×1,000 / 백만원→×100,000)하고, "
                "음수를 의미하는 ((value)) 표기는 반드시 '-' 부호로 반영하세요. "
                "출력에는 텍스트/단위를 포함하지 말고, 쉼표는 허용됩니다. 하나의 숫자만 반환, 없으면 0을 반환하세요."
            )
            loan_system_prompt = (
                "당신은 기업의 총차입금 항목을 정확하게 추출하는 금융 정보 추출 전문가입니다. "
                "HTML 형태의 재무제표 주석에서 특정 항목의 '당기말/당기/당분기/당반기' 금액만 숫자로 추출하세요(전기 제외). "
                "모든 금액은 '원' 단위로 환산하세요(천원×1,000 / 백만원×100,000). "
                "항목: 단기차입금, 장기차입금, 유동성장기차입금, 사채(유동/비유동), 금융리스부채(유동/비유동). "
                "아래 JSON 포맷을 정확히 지켜 응답하세요(키/순서 동일, 값은 정수):\n"
                "{\n"
                " '단기차입금': 0,\n"
                " '장기차입금': 0,\n"
                " '유동성장기차입금': 0,\n"
                " '사채_유동': 0,\n"
                " '사채_비유동': 0,\n"
                " '금융리스부채_유동': 0,\n"
                " '금융리스부채_비유동': 0,\n"
                " '금융리스부채_합계': 0\n"
                "}\n"
            )
            system_prompt = loan_system_prompt if "총차입금" in prompt else default_system_prompt
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"아래는 기업의 재무제표 주석입니다:\n\n{comment_text}\n\n{prompt}"},
            ]
            kwargs = self._build_chat_kwargs(messages)
            resp = self.client.chat.completions.create(**kwargs)
            return resp.choices[0].message.content.strip()

        except openai.RateLimitError as e:
            retry_after = int(getattr(getattr(e, "response", None), "headers", {}).get("Retry-After", "10"))
            logger.warn(f"[LLM] RateLimit → {retry_after}s 대기 후 재시도({retry+1})")
            time.sleep(retry_after)
            return self._call_llm(comment_text, prompt, retry + 1)

        except Exception as e:
            if retry < 3:
                logger.error(f"[LLM] 예외({e}) → 30s 대기 후 재시도({retry+1})")
                time.sleep(30)
                return self._call_llm(comment_text, prompt, retry + 1)
            return f"[Error] {str(e)}"

    def _parse_numeric(self, value: Optional[str]) -> Optional[int]:
        if not value or not any(c.isdigit() for c in value):
            return None
        if value.strip() == "0":
            return 0
        if not re.fullmatch(r"-?[\d,]+(\.\d+)?", value.strip()):
            return None
        clean = re.sub(r"[^0-9\-,.]", "", value)
        try:
            return int(float(clean.replace(",", "")))
        except Exception:
            return None

    def _try_extract_single_number(self, text: str, prompt: str) -> Optional[int]:
        value = self._call_llm(text, prompt)
        return self._parse_numeric(value)

    def _try_recover_json_string(self, value: str) -> str:
        try:
            json.loads(value)
            return value
        except json.JSONDecodeError:
            value = re.sub(r"(\d)_(\d)", r"\1\2", value).strip()
            if not value.endswith("}"):
                last_comma = value.rfind(",")
                if last_comma != -1:
                    value = value[:last_comma] + "\n}"
                else:
                    value += "}"
            return value

    def _enforce_fixed_json_format(self, raw_value: str) -> str:
        try:
            parsed = json.loads(raw_value)
        except Exception:
            return raw_value
        template_keys = [
            "단기차입금", "장기차입금", "유동성장기차입금",
            "사채_유동", "사채_비유동", "금융리스부채_유동", "금융리스부채_비유동", "금융리스부채_합계"
        ]
        fixed = {}
        for k in template_keys:
            raw = parsed.get(k, 0)
            try:
                cleaned = str(raw).replace(",", "").replace("_", "")
                fixed[k] = int(float(cleaned))
            except Exception:
                fixed[k] = 0
        return json.dumps(fixed, ensure_ascii=False, indent=2)
    
    def _enforce_loan_json_format(self, raw_value: str) -> str:
        """
        대여금 JSON(단기대여금/장기대여금) 보정:
        - 누락 키를 0으로 채움
        - 콤마/밑줄 제거 후 정수화
        - 두 키만 유지
        """
        try:
            parsed = json.loads(raw_value)
        except Exception:
            # JSON 파싱 불가 시, 안전한 기본값 반환
            return json.dumps({"단기대여금": 0, "장기대여금": 0}, ensure_ascii=False, indent=2)

        keys = ["단기대여금", "장기대여금"]
        fixed = {}
        for k in keys:
            raw = parsed.get(k, 0)
            try:
                cleaned = str(raw).replace(",", "").replace("_", "")
                fixed[k] = int(float(cleaned))
            except Exception:
                fixed[k] = 0
        return json.dumps(fixed, ensure_ascii=False, indent=2)

    def _calculate_total_loan(self, cleaned_html: str) -> Dict[str, object]:
        categories = ["총차입금", "리스부채"]
        merged: dict = {}
        for account in categories:
            prompt_retry = get_prompt_by_account(account)
            snippet_retry = self._extract_snippet_near_keywords(cleaned_html, account)
            chunk = snippet_retry if snippet_retry.strip() else cleaned_html
            value = self._call_llm(chunk, prompt_retry)
            if not value or not value.strip().startswith("{"):
                continue
            try:
                value = self._try_recover_json_string(value)
                value = self._enforce_fixed_json_format(value)
                parsed = json.loads(value)
                for k, v in parsed.items():
                    merged[k] = merged.get(k, 0) + int(v)
            except Exception:
                continue

        if merged.get("금융리스부채_합계", 0) == 0:
            total = (
                merged.get("단기차입금", 0)
                + merged.get("장기차입금", 0)
                + merged.get("유동성장기차입금", 0)
                + merged.get("사채_유동", 0)
                + merged.get("사채_비유동", 0)
                + merged.get("금융리스부채_유동", 0)
                + merged.get("금융리스부채_비유동", 0)
            )
        else:
            total = (
                merged.get("단기차입금", 0)
                + merged.get("장기차입금", 0)
                + merged.get("유동성장기차입금", 0)
                + merged.get("사채_유동", 0)
                + merged.get("사채_비유동", 0)
                + merged.get("금융리스부채_합계", 0)
            )
        logger.info(f"[LLM] 총차입금+리스부채 합산: {merged} → 합계: {total}")
        return {"ACCOUNT_AMOUNT": int(total), "IS_COMPLETE": True, "RAW_VALUE": json.dumps(merged, ensure_ascii=False)}
    
    def _extract_loan_receivable(self, cleaned_html: str, rcept_no: str) -> Dict[str, int]:
        """
        대여금(단기/장기) JSON 1회 추출 캐시:
        - 동일 RCEPT_NO에서 '단기대여금'과 '장기대여금'을 각각 요청하더라도 LLM 호출은 1번만 수행
        - 프롬프트는 반드시 get_prompt_by_account("대여금")을 사용
        - 반환: {"단기대여금": int, "장기대여금": int}
        """
        # 1) 캐시 확인
        if rcept_no in self._loan_cache:
            return self._loan_cache[rcept_no]

        # 2) 프롬프트: '대여금' 이름으로 강제 사용 (없으면 안전한 기본 프롬프트)
        receivable_prompt = get_prompt_by_account("대여금") or (
            "아래 HTML 주석에서 '대여금' 관련 표/문단을 분석해 "
            "'당기말' 또는 '당기' 또는 '당분기' '당반기' 금액만 추출하세요. "
            "단위가 '천원', '백만원' 등으로 표시되어 있으면 반드시 원(₩) 단위로 환산하세요. "
            "반드시 다음 JSON 형식 그대로만 출력하세요 (키와 순서, 들여쓰기 유지):\n"
            "{\n"
            "  \"단기대여금\": 0,\n"
            "  \"장기대여금\": 0\n"
            "}\n"
            "값이 없으면 0으로 두세요. 음수는 -기호를 붙이세요. "
            "숫자 외 텍스트/단위는 넣지 마세요."
        )

        # 3) 스니펫 추출
        snippet = self._extract_snippet_near_keywords(cleaned_html, "대여금")
        chunk = snippet if snippet and snippet.strip() else cleaned_html

        # 4) LLM 호출 및 JSON 보정
        value = self._call_llm(chunk, receivable_prompt)
        try:
            if not value or not value.strip():
                parsed = {"단기대여금": 0, "장기대여금": 0}
            else:
                value = self._try_recover_json_string(value)
                value = self._enforce_loan_json_format(value)
                parsed = json.loads(value)
        except Exception:
            parsed = {"단기대여금": 0, "장기대여금": 0}

        # 5) 정수화 및 캐시 저장
        short_amt = int(parsed.get("단기대여금", 0) or 0)
        long_amt  = int(parsed.get("장기대여금", 0) or 0)
        fixed = {"단기대여금": short_amt, "장기대여금": long_amt}

        self._loan_cache[rcept_no] = fixed
        logger.info(f"[LLM] 대여금 JSON 추출(캐시 저장): {fixed} (RCEPT_NO={rcept_no})")
        return fixed

    def _extract_value_with_flags(self, comment_html: str, account_name: str, rcept_no: str) -> Dict[str, object]:
        cleaned_html = self._clean_html_text(comment_html)
        if account_name in {"단기대여금", "장기대여금"}:
            parsed_recv = self._extract_loan_receivable(cleaned_html, rcept_no)
            amt = int(parsed_recv.get(account_name, 0) or 0)
            return {
                "ACCOUNT_AMOUNT": amt,
                "IS_COMPLETE": True,
                "RAW_VALUE": json.dumps(parsed_recv, ensure_ascii=False)
            }

        # 일반 케이스 프롬프트 조회 (대여금은 위에서 처리됨)
        prompt = get_prompt_by_account(account_name)
        if not prompt and account_name not in {"대여금"}:
            return {"ACCOUNT_AMOUNT": None, "IS_COMPLETE": False, "RAW_VALUE": None}

        # 2) 스니펫 추출 (없으면 전체 HTML)
        snippet_text = self._extract_snippet_near_keywords(cleaned_html, account_name)
        chunk_text = snippet_text if snippet_text.strip() else cleaned_html
        if not snippet_text.strip():
            logger.warn(f"[LLM] 스니펫 없음 ({account_name})")

        # 3) 총차입금(단일): 단일 숫자 시도 후 실패 시 합산 로직
        if account_name == "총차입금(단일)":
            val = self._try_extract_single_number(chunk_text, prompt)
            if val is not None:
                logger.info(f"[LLM] 총차입금(단일) 1차 추출: {val}")
                return {"ACCOUNT_AMOUNT": val, "IS_COMPLETE": True, "RAW_VALUE": f"{val}"}
            return self._calculate_total_loan(cleaned_html)

        # 4) 일반 항목
        for _ in range(2):
            value = self._call_llm(chunk_text, prompt)
            parsed = self._parse_numeric(value)
            if parsed is not None:
                # 0도 유효값으로 간주
                return {"ACCOUNT_AMOUNT": parsed, "IS_COMPLETE": True, "RAW_VALUE": str(value)}
        return {"ACCOUNT_AMOUNT": 0, "IS_COMPLETE": True, "RAW_VALUE": "0"}

    def run(self, throttle_sec: float = 5.0) -> int:
        request_context.request_id = str(uuid4())
        logger.info("[LLM] TB_FINANCIAL_VARIABLE 추출 스케줄러 시작")

        processed = 0
        with SessionLocal() as conn:
            factory_var = BaseQueryFactory(conn=conn, model=TB_FINANCIAL_VARIABLE)
            factory_dis = BaseQueryFactory(conn=conn, model=TB_DISCLOSURE_INFORMATION)

            # 후보 조회: IS_LLM=True & IS_COMPLETE=False & (ACCOUNT_AMOUNT 비어있음)
            candidates = factory_var.find_all(IS_LLM=True, IS_COMPLETE=False)
            # candidates = [r for r in candidates if (not r.ACCOUNT_AMOUNT or str(r.ACCOUNT_AMOUNT).strip() == "")]

            if not candidates:
                logger.info("[LLM] 처리할 항목 없음")
                return 0

            logger.info(f"[LLM] 후보 {len(candidates)}건 처리 시작")

            for row in candidates:
                try:
                    disclosure = factory_dis.find_one(RCEPT_NO=row.RCEPT_NO)
                    if not disclosure or not disclosure.OFS_COMMENT:
                        logger.warn(f"[LLM] 주석 없음: RCEPT_NO={row.RCEPT_NO}")
                        continue

                    model_name = "gpt-4.1" if row.ACCOUNT_NM in {"총차입금(단일)", "개발비", "이자비용","단기대여금","장기대여금"} else self.model_default
                    self.model_default = model_name  

                    result = self._extract_value_with_flags(
                        comment_html=disclosure.OFS_COMMENT,
                        account_name=row.ACCOUNT_NM,
                        rcept_no=row.RCEPT_NO
                    )
                    logger.info(f"[LLM:{model_name}] RCEPT_NO={row.RCEPT_NO} → 결과: {result}")

                    update_data = {"IS_COMPLETE": bool(result.get("IS_COMPLETE", False))}
                    if result.get("IS_COMPLETE", False):
                        update_data["ACCOUNT_AMOUNT"] = result.get("ACCOUNT_AMOUNT")

                    factory_var.update(row, **update_data)
                    processed += 1

                    time.sleep(throttle_sec)

                except Exception as e:
                    logger.error(f"[LLM] 처리 실패: RCEPT_NO={row.RCEPT_NO} / {e}", exc_info=True)

        logger.info(f"[LLM] 스케줄러 종료 (처리: {processed}건)")
        return processed
