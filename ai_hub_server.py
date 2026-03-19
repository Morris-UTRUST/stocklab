import json
import os
import re
import subprocess
import uuid
from datetime import date
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from requests.exceptions import Timeout, RequestException

import requests

HOST = "127.0.0.1"
PORT = int(os.getenv("AI_HUB_PORT", "8787"))
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://127.0.0.1:11434/api/generate")
OLLAMA_MODEL = os.getenv("AI_HUB_MODEL", "qwen3:8b")
OLLAMA_TIMEOUT = int(os.getenv("AI_HUB_TIMEOUT_SECONDS", "120"))
OLLAMA_RETRIES = int(os.getenv("AI_HUB_RETRIES", "2"))
OPENCLAW_REPORT_PROVIDER = os.getenv("AI_HUB_REPORT_PROVIDER", "openclaw")

# Cloud API 設定
CLOUD_API_BASE = os.getenv("AI_HUB_CLOUD_API_BASE", "https://openrouter.ai/api/v1")
CLOUD_API_KEY = os.getenv("AI_HUB_CLOUD_API_KEY", "")
CLOUD_REPORT_MODEL = os.getenv("AI_HUB_CLOUD_MODEL", "openai-codex/gpt-5.4")
CLOUD_TIMEOUT = int(os.getenv("AI_HUB_CLOUD_TIMEOUT_SEC", "90"))
OPENCLAW_CLI = os.getenv("AI_HUB_OPENCLAW_CLI", "/opt/homebrew/bin/openclaw")
OPENCLAW_AGENT_ID = os.getenv("AI_HUB_OPENCLAW_AGENT_ID", "sub1")
OPENCLAW_FALLBACK_AGENT_ID = os.getenv("AI_HUB_OPENCLAW_FALLBACK_AGENT_ID", "main")
OPENCLAW_TIMEOUT = int(os.getenv("AI_HUB_OPENCLAW_TIMEOUT_SECONDS", "90"))
OPENCLAW_ACCOUNT = os.getenv("AI_HUB_OPENCLAW_ACCOUNT", "")
OPENCLAW_SESSION_PREFIX = os.getenv("AI_HUB_OPENCLAW_SESSION_PREFIX", "stocklab-report")
SKILL_VERSION = "quant-report-skill-v1.0"
HUB_VERSION = "xiaofang-ai-hub-v0.4"


def build_prompt(payload: dict) -> str:
    stock_id = payload.get("stock_id", "未知")
    stock_name = payload.get("stock_name", "未知名稱")
    as_of_date = payload.get("as_of_date", str(date.today()))

    return f"""
你是「小芳量化投資研究引擎」，請嚴格依照 quant-report-skill-v1.0 產出報告。

# 報告目標
- 量化核心 + 結論導向
- 先給可執行建議，再給證據鏈
- 基本面僅作校正，不可覆蓋量化主結論

# 輸入資料
{json.dumps(payload, ensure_ascii=False, indent=2)}

# 必須遵守
1. 不可虛構任何不存在的數據
2. 建議動作需與輸入 action 大方向一致
3. 一定要給「短線/中線/長線」可操作價位與理由
4. 一定要給「失效條件」
5. 文字要專業但實用，避免空泛形容
6. 僅可引用以下 market intel 欄位：`exchange_disclosures[].date/title/summary/source/source_url`、`industry_news[].published_at/title/summary/source/source_url`、`market_intel_meta.*`
7. 只有在上述欄位真的有資料時才可引用；若陣列為空或欄位缺漏，必須明說「目前無可引用的重大訊息/新聞資料」，不可補寫事件、日期、媒體或引述
8. 若引用資料，請用簡短來源標記，例如 `（重大訊息/MOPS）`、`（新聞/Google News）`，不可冒用不存在的直接引言

# 固定輸出格式（Markdown）
## 0) 一句話結論（先給答案）
- 目前建議：
- 信心等級：
- 核心理由：

## 1) 可執行操作建議（最重要）
### 1.1 短線（1~10交易日）
- 參考買點區：
- 參考賣點區：
- 防守停損：
- 建議動作：

### 1.2 中線（2~8週）
- 參考布局區：
- 參考減碼區：
- 失效條件：
- 建議動作：

### 1.3 長線（2~6月）
- 趨勢持有條件：
- 趨勢失效條件：
- 建議動作：

## 2) 頂底與關鍵位階（支撐/壓力）
- 近20日支撐區：
- 近60日支撐區：
- 近20日壓力區：
- 近60日壓力區：
- 當前相對位置：

## 3) 量化證據鏈（前四步）
### 3.1 趨勢（MA + ADX）
### 3.2 動能（MACD + RSI/KD）
### 3.3 量能（Volume + OBV/MFI）
### 3.4 風險（ATR + Volatility + Drawdown）

## 4) 基本面與事件校正（輔助層）
- 證交所重大訊息摘要：
- 近期產業/公司新聞摘要：
- 校正結論：
說明：本段只能摘要輸入 payload 內可引用欄位；沒有資料就明確寫無資料。

## 5) 風險與失效條件
- 風險等級：
- 主要風險清單：
- 全域失效條件：

## 6) 最終提醒
- 本報告僅供研究與決策輔助，不構成投資建議。

# 報告抬頭
股票：{stock_id} {stock_name}
日期：{as_of_date}
""".strip()


def _is_valid_report_text(s: str) -> bool:
    if not isinstance(s, str):
        return False
    s = s.strip()
    if len(s) < 80:
        return False
    if s.startswith("{") and s.endswith("}"):
        return False
    return bool(re.search(r"[\u4e00-\u9fff]", s))


def _extract_openclaw_text(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""

    # 1) prefer last non-empty line for plain outputs
    lines = [ln.strip() for ln in s.splitlines() if ln.strip()]
    if lines and not (s.startswith("{") or s.startswith("[")):
        return lines[-1]

    # 2) json output from openclaw --json
    try:
        data = json.loads(s)
        if isinstance(data, dict):
            for k in ["text", "message", "content", "output", "response"]:
                v = data.get(k)
                if isinstance(v, str) and v.strip():
                    return v.strip()

            # openclaw json envelope
            result = data.get("result")
            if isinstance(result, dict):
                payloads = result.get("payloads")
                if isinstance(payloads, list) and payloads:
                    for p in payloads:
                        if isinstance(p, dict):
                            txt = p.get("text")
                            if isinstance(txt, str) and txt.strip():
                                return txt.strip()

            # choices style
            choices = data.get("choices")
            if isinstance(choices, list) and choices:
                c0 = choices[0]
                if isinstance(c0, dict):
                    msg = c0.get("message")
                    if isinstance(msg, dict):
                        out = msg.get("content")
                        if isinstance(out, str) and out.strip():
                            return out.strip()
    except Exception:
        pass

    return s




def _cloud_report_text(prompt: str) -> str:
    """直接呼叫雲端 API (OpenRouter)"""
    if not CLOUD_API_KEY:
        raise RuntimeError("cloud_api_key_missing")

    r = requests.post(
        f"{CLOUD_API_BASE}/chat/completions",
        headers={
            "Authorization": f"Bearer {CLOUD_API_KEY}",
            "Content-Type": "application/json",
            "HTTP-Referer": "http://localhost",
            "X-Title": "StockLab-AI-Hub"
        },
        json={
            "model": CLOUD_REPORT_MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.2,
        },
        timeout=CLOUD_TIMEOUT,
    )
    r.raise_for_status()
    data = r.json()
    return (data.get("choices", [{}])[0].get("message", {}).get("content", "") or "").strip()


def _openclaw_report_text(payload: dict, prompt: str) -> str:
    message = (
        "以下為量化分析結構化資料(JSON)。請直接輸出最終投資報告內容，不要輸出 JSON。\n"
        f"{json.dumps(payload, ensure_ascii=False)}\n\n"
        f"補充規格：\n{prompt}"
    )

    last_err = ""
    for agent_id in [OPENCLAW_AGENT_ID, OPENCLAW_FALLBACK_AGENT_ID]:
        for _attempt in range(1, 3):
            sid = f"{OPENCLAW_SESSION_PREFIX}-{uuid.uuid4().hex[:12]}"
            cmd = [
                OPENCLAW_CLI,
                "agent",
                "--agent", agent_id,
                "--session-id", sid,
                "--message", message,
                "--json",
                "--timeout", str(OPENCLAW_TIMEOUT),
            ]
            if OPENCLAW_ACCOUNT:
                cmd.extend(["--account", OPENCLAW_ACCOUNT])

            env = os.environ.copy()
            env["PATH"] = f"/opt/homebrew/bin:/usr/local/bin:{env.get('PATH', '/usr/bin:/bin:/usr/sbin:/sbin')}"
            cp = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=OPENCLAW_TIMEOUT + 20,
                check=False,
                env=env,
            )
            out = _extract_openclaw_text(cp.stdout)
            if _is_valid_report_text(out):
                return out

            err = (cp.stderr or "").strip()[:300]
            if out and not err:
                err = f"invalid_output:{out[:80]}"
            last_err = err or f"returncode={cp.returncode}"

    raise RuntimeError(f"openclaw_report_failed: {last_err}")


class Handler(BaseHTTPRequestHandler):
    def _json(self, code: int, obj: dict):
        data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        try:
            self.wfile.write(data)
        except BrokenPipeError:
            # Client disconnected before receiving response; ignore noisy traceback.
            return

    def _read_json(self):
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length > 0 else b"{}"
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception as e:
            raise ValueError(f"invalid json: {e}")

    def _ollama_text(self, prompt: str, temperature: float = 0.2) -> str:
        body = {
            "model": OLLAMA_MODEL,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": temperature},
        }
        last_err = None
        for attempt in range(1, OLLAMA_RETRIES + 1):
            try:
                r = requests.post(OLLAMA_URL, json=body, timeout=OLLAMA_TIMEOUT)
                r.raise_for_status()
                out = r.json()
                return out.get("response", "").strip()
            except Timeout as e:
                last_err = e
                if attempt < OLLAMA_RETRIES:
                    time.sleep(min(2 * attempt, 5))
                    continue
                raise Timeout(f"ollama timeout after {OLLAMA_RETRIES} attempts ({OLLAMA_TIMEOUT}s each)") from e
            except RequestException as e:
                last_err = e
                if attempt < OLLAMA_RETRIES:
                    time.sleep(min(2 * attempt, 5))
                    continue
                raise
        raise RuntimeError(f"ollama request failed: {last_err}")

    def do_GET(self):
        if self.path == "/health":
            self._json(200, {"ok": True, "engine": "xiaofang-ai-hub", "version": HUB_VERSION, "model": OLLAMA_MODEL})
            return
        self._json(404, {"error": "not found"})

    def do_POST(self):
        if self.path not in ["/report/investment", "/v1/chat/completions"]:
            self._json(404, {"error": "not found"})
            return

        try:
            payload = self._read_json()
        except Exception as e:
            self._json(400, {"error": str(e)})
            return

        try:
            if self.path == "/report/investment":
                prompt = build_prompt(payload)
                route = "ollama_local"
                model = OLLAMA_MODEL

                # 路由順序: Cloud API > OpenClaw > Ollama
                if CLOUD_API_KEY:
                    try:
                        report = _cloud_report_text(prompt)
                        route = "cloud"
                        model = CLOUD_REPORT_MODEL
                    except Exception as cloud_err:
                        print(f"[WARN] Cloud failed: {cloud_err}, trying OpenClaw...")
                        try:
                            report = _openclaw_report_text(payload, prompt)
                            route = "openclaw_sub1_main"
                            model = f"{OPENCLAW_AGENT_ID}->{OPENCLAW_FALLBACK_AGENT_ID}"
                        except Exception as oc_err:
                            print(f"[WARN] OpenClaw failed: {oc_err}, falling back to Ollama...")
                            report = self._ollama_text(prompt, temperature=0.2)
                            route = "ollama_local"
                            model = OLLAMA_MODEL
                elif OPENCLAW_REPORT_PROVIDER == "openclaw":
                    try:
                        report = _openclaw_report_text(payload, prompt)
                        route = "openclaw_sub1_main"
                        model = f"{OPENCLAW_AGENT_ID}->{OPENCLAW_FALLBACK_AGENT_ID}"
                    except Exception as oc_err:
                        print(f"[WARN] OpenClaw failed: {oc_err}, falling back to Ollama...")
                        report = self._ollama_text(prompt, temperature=0.2)
                        route = "ollama_local"
                        model = OLLAMA_MODEL
                else:
                    report = self._ollama_text(prompt, temperature=0.2)
                    route = "ollama_local"
                    model = OLLAMA_MODEL

                if not report:
                    report = "### AI 報告產生失敗\n模型未回傳內容。"
                self._json(200, {
                    "ok": True,
                    "engine": "xiaofang-ai-hub",
                    "model": model,
                    "route": route,
                    "version": SKILL_VERSION,
                    "report_markdown": report,
                })
                return

            # OpenAI-compatible endpoint for all systems (中樞模式)
            messages = payload.get("messages", [])
            if not isinstance(messages, list) or not messages:
                self._json(400, {"error": "messages is required"})
                return
            text_parts = []
            for m in messages:
                role = m.get("role", "user")
                content = m.get("content", "")
                text_parts.append(f"[{role}]\n{content}")
            prompt = "\n\n".join(text_parts)
            temperature = float(payload.get("temperature", 0.2) or 0.2)
            answer = self._ollama_text(prompt, temperature=temperature)

            self._json(200, {
                "id": "chatcmpl-xiaofang",
                "object": "chat.completion",
                "created": int(time.time()),
                "model": payload.get("model") or OLLAMA_MODEL,
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": answer},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
                "engine": "xiaofang-ai-hub",
                "version": HUB_VERSION,
            })
        except Exception as e:
            self._json(500, {"ok": False, "error": str(e)})


def main():
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"AI Hub listening on http://{HOST}:{PORT} using model={OLLAMA_MODEL}")
    server.serve_forever()


if __name__ == "__main__":
    main()
