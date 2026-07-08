# WeCom Stream Plugin — Fix Log

This document tracks each fix applied to the wecom_stream adapter plugin,
following TDD methodology: write failing test → implement fix → verify pass.

## Test Infrastructure

- Test framework: pytest + pytest-asyncio
- Test count: 70 (baseline) + incremental tests per fix
- Run: `cd ~/.hermes/plugins/wecom_stream && ./run_tests.sh`

---

## Issue #1 [Critical]: deferred finish 无重试机制

**Symptom**: 企业微信中最后一条回复消息无法长按弹出菜单（复制、转发、收藏等）。

**Root Cause**: `_deferred_finish()` 在发送 `finish=True` 时没有任何重试机制。
如果 `reply_stream()` 因网络抖动、WeCom 服务端限流等原因失败，只 log warning，
stream 永远停留在 `finish=False` 状态。企业微信客户端不为 `finish=False` 的
消息渲染长按交互菜单。

**Fix**: 
- 添加 `_DEFERRED_FINISH_RETRIES = 3` 和 `_DEFERRED_FINISH_RETRY_DELAY = 1.0` 配置
- `_deferred_finish()` 重写为 for 循环，最多重试 3 次（指数退避 1s→2s→4s）
- 每次重试前检查 `session.finished`，避免不必要的重试
- 所有重试均失败后，仍然标记 `session.finished = True`（防止永久 stale state）

**Tests added**:
- `test_deferred_finish_retries_on_failure` — 验证前 2 次失败后第 3 次成功
- `test_deferred_finish_exhausts_retries_and_marks_finished` — 验证重试耗尽后标记 finished

---

## Issue #2 [Critical]: 进程关闭时未完成的 stream 永远不会关闭

**Symptom**: Gateway 重启或关闭时，正在 streaming 的消息气泡永久停留在 `finish=False`。

**Root Cause**: `disconnect()` 直接清空 `_stream_sessions`，没有先发送 `finish=True`。
任何进行中的 stream 在 WebSocket 断开时就永远无法再被关闭。

**Fix**:
- 新增 `_flush_pending_finishes()` 方法
- 在 `disconnect()` 中、清理 sessions 之前调用
- 遍历所有 `finished=False` 的 session，立即发送 `finish=True`
- 即使发送失败也标记 `finished=True`（防止 stale state）

**Tests added**:
- `test_disconnect_flushes_pending_finish` — 单 session flush
- `test_disconnect_flushes_multiple_sessions` — 多 session flush

---

## Issue #4 [High]: send_typing 中遗留 debug traceback 日志

**Symptom**: 每次调用 `send_typing()` 都会 `import traceback` 并用 `logger.info(format_stack())` 打印完整调用栈。

**Fix**:
- 删除 `import traceback` + `logger.info(format_stack())` debug 代码
- 将 3 个 INFO 级别日志降为 DEBUG（DEDUP SKIP, CREATED session）

---

## Issue #5 [High]: 长文本无拆分，直接截断到 4000 字符

**Symptom**: 超过 4000 字符的回复被 `[:MAX_MESSAGE_LENGTH]` 静默截断，用户丢失内容。

**Fix**:
- 新增 `split_text()` 函数（受 QwenPaw 启发），支持：
  - 按换行符拆分，保留格式
  - 单行超长时硬切分
  - 代码块（code fence）跨块保持渲染正确（自动闭合/重开）
- `_proactive_send()` 使用 `split_text()` 分多条 `send_message` 发送
- 每条消息不超过 `MAX_MESSAGE_LENGTH`（4000 字符）

**Tests added**:
- `test_short_text_not_split` — 短文本不拆分
- `test_long_text_split_into_chunks` — 长文本拆分
- `test_split_preserves_newlines` — 换行保留
- `test_split_handles_code_fence` — 代码块跨块
- `test_send_long_text_uses_split` — send_message 多次调用

---

## Issue #6 [High]: edit_message 的 finalize 参数被忽略

**状态**: 已被 Issue #1/#3 的 deferred finish retry fix 解决。

`edit_message(finalize=True)` 的 `finalize` 参数在 deferred finish 策略下
故意不直接发送 `finish=True`（因为 gateway 在 segment break 时也会传
`finalize=True`，直接 finish 会提前关闭气泡）。Issue #1 的 retry fix 确保
了 deferred finish 最终一定会送达，解决了根因。

---

## Issue #11 [Medium]: proactive send 硬编码 markdown 类型

**Symptom**: `_proactive_send` 总是用 `msgtype: "markdown"`，纯文本消息也会
被渲染为 markdown，可能导致显示不一致。

**Fix**: 自动检测内容是否包含 markdown 语法（`**`, `##`, `|`, ` ``` `, `- `, `1. `），
有则用 markdown 类型，否则用 text 类型。

---

## Issue #15 [Low]: Thinking 文案硬编码 emoji

**Fix**: 支持通过 `WECOM_STREAM_PROCESSING_TEXT` 环境变量自定义 Thinking 文案，
默认仍为 `🤔 Thinking...`。

---

## Issue #7 [High]: WebSocket 跨 loop 调用健壮性

**Symptom**: `_capture_ws_loop` 只尝试 `_receive_task` 和 `_heartbeat_task`，SDK 升级改名后直接返回 None。

**Fix**:
- 3 级 fallback 策略：task 属性 → SDK client loop 属性 → running loop
- 检查 `loop.is_closed()` 避免使用已关闭的 loop
- 尝试更多 task 属性（`_reconnect_task`）

---

## Issue #8 [Medium]: monkey-patch SDK 版本检测

**Symptom**: SDK 升级后 patch 可能静默失效。

**Fix**: patch 后立即验证 3 个目标方法是否正确替换，失败则 WARNING 日志。

---

## Issue #9 [Medium]: `_keepalive_typing` generation 竞态

**Symptom**: keepalive 的 `reply_stream` 在途时 `send()` 同时覆写气泡，可能导致竞态。

**Fix**:
- 新增 `_stream_locks: Dict[str, asyncio.Lock]` 按 chat 隔离
- `_keepalive_typing` 在 `reply_stream` 调用前后持锁
- `send()` overwrite 路径在覆写前等待锁释放
- 锁内二次检查 generation，消除窗口期

---

## Issue #10 [Medium]: turn_id 内存状态丢失

**Symptom**: gateway 重启后 `_turn_id` 清零，turn 边界检测失效。

**Fix**:
- 存储 WeCom frame 的 `send_time` 作为 turn 边界的 fallback 信号
- `send_typing` 的 turn 检测同时比较 `_turn_id` 和 `_send_time`
- session 创建时记录 `_send_time`

---

## Issue #12 [Medium]: media 上传与 stream session 无关联

**Symptom**: `send_media` 不关闭活跃 stream，media 消息成为游离气泡。

**Fix**: `send_media()` 开头检查并 finalize 当前 chat 的活跃 stream session（发送 `finish=True`），然后再上传和发送 media。

---

## 未在本轮修复的问题

- **#13 [Low]**: 单文件拆分为模块 — 需要补充 media upload / IPC 的集成测试后再做，风险较高。
