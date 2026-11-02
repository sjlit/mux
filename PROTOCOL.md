# mux Protocol Specification (wire version 3)

- Status: Draft (wire codec implemented in `wire/`)
- Wire version: 3
- Scope: 全新线协议与运行时设计，**不兼容** v1/v2
- Transport: 任意可靠、有序的字节流（TCP / KCP stream / TLS / 自定义）
- Goal: 每批一次调度、一次 syscall、零 payload 拷贝，流控开销趋近于 0

---

## 1. 术语与约定

- 多字节整数一律 **little-endian**。
- `MUST` / `SHOULD` / `MAY` 按 RFC 2119 理解。
- “发送方”指写入该帧的一端；“接收方”指读取该帧的一端。
- “本地发起的流”指本端分配 stream id 并发送 `F_SYN` 的流；“对端发起的流”反之。
- 计数器和窗口使用 uint32；飞行字节数按模 2^32 计算，窗口 MUST < 2^31。

---

## 2. 传输假设

1. 底层连接提供可靠、有序的字节流，不保证消息边界。
2. 一个帧可能被拆成任意多个读块，也可能多个帧合并在一个读块中。
3. 协议不依赖底层 `WriteBuffers` / `writev`；实现可以自行利用 scatter-gather 优化。
4. 连接断开等价于会话终止，所有流以错误结束。

---

## 3. 帧格式

固定 8 字节头，可选 4 字节 credit，之后是 payload：

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|    Version    | Type  | Flags |          Length               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                          Stream ID                            |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                     Credit (optional, 4B)                     |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                     Payload (Length bytes)                    |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

| 字段 | 大小 | 说明 |
|---|---|---|
| Version | 1 | MUST 为 `0x03` |
| Type | 4 bits | 帧类型，见 §5 |
| Flags | 4 bits | 标志位，见 §4 |
| Length | 2 | payload 字节数，**不含** header 和 credit |
| Stream ID | 4 | 流标识；控制帧有特殊含义，见 §5 |
| Credit | 4 | 仅当 `F_CREDIT` 置位时存在；绝对 consumed 计数 |

`byte1 = (Type << 4) | Flags`。

### 4. Flags

| 位 | 名称 | 说明 |
|---|---|---|
| 0 | `F_SYN` | 打开流；仅 PSH 合法 |
| 1 | `F_FIN` | 关闭发送方向（half-close）；仅 PSH 合法 |
| 2 | `F_CREDIT` | header 后跟 4B credit；仅 PSH 合法 |
| 3 | reserved | MUST 为 0，收到非 0 视为 protocol error |

### 5. 帧类型

| Type | 名称 | Stream ID | Length | 说明 |
|---|---|---|---|---|
| 0x0 | `PSH` | 目标流 | 0..65535 | 数据；可带 `F_SYN` / `F_FIN` / `F_CREDIT` |
| 0x1 | `UPD` | 流 id，0 表示 session | 4 | payload = absolute consumed (u32) |
| 0x2 | `RST` | 目标流 | 4 | payload = error code (u32) |
| 0x3 | `PING` | nonce | 0 | 请求对端回 PONG |
| 0x4 | `PONG` | nonce | 0 | 回显 PING 的 nonce |
| 0x5 | `GOAWAY` | last stream id | 4 | payload = error code (u32) |
| 0x6 | `SETTINGS` | 0 | 28 | session 参数，见 §6 |
| 0x7-0xF | reserved | — | — | MUST 视为 protocol error |

### 5.1 PSH

- payload 是流数据。
- `F_SYN`：创建流；payload 可以同时携带首块数据（`SYN+data`）。
- `F_FIN`：该 payload 交付后，发送方向 EOF（`FIN+data`）。
- `F_SYN|F_FIN`：一次性请求流（open + data + EOF）。
- `F_CREDIT`：携带本端在**同一 stream id 的反方向**上已消费的绝对字节数，用于 piggyback 流控更新。
- `Stream ID` MUST NOT 为 0；0 保留给 session 级控制帧（`UPD sid=0`），不是合法流 id。
- Length 为 0 且无 flag：合法 no-op，接收方 MUST 忽略。
- Length 为 0 且有 `F_SYN`/`F_FIN`：合法。
- 收到 payload 超过本端 advertised `max_frame_size`：`FRAME_SIZE_ERROR`。

### 5.2 UPD

- `Stream ID != 0`：更新该流的 absolute consumed。
- `Stream ID == 0`：更新 session 的 absolute consumed。
- payload 固定 4 字节。
- consumed 单调递增；`int32(new - old) <= 0` 的更新 MUST 被忽略（stale）。
- `int32(new - old)` 超过当前窗口视为 `FLOW_CONTROL_ERROR`。

### 5.3 RST

- 立即关闭流双向，丢弃所有未交付数据。
- payload 是 4 字节 error code。
- `Stream ID` MUST NOT 为 0。
- 幂等；收到已关闭流的 RST MUST 忽略。
- RST 不携带 credit；已接收但未消费的字节仍必须计入 session consumed 并回补 session credit。

### 5.4 PING / PONG

- `Stream ID` 字段作为 32-bit nonce；Length MUST 为 0。
- 收到 PING MUST 回一个相同 nonce 的 PONG。
- nonce 建议用单调计数器；允许回绕。
- PONG 用于 RTT 测量；任何合法帧都算 liveness。

### 5.5 GOAWAY

- `Stream ID` = 本端已处理的最高对端流 id（`last_stream_id`）。
- payload = error code。
- 发送后本端 MUST NOT 再发起新流；收到后本端 MUST NOT 再发起新流。
- 存量流可以继续，直到双方 FIN/RST；实现可以在超时后 abort。
- 对端可以对 `last_stream_id` 之上的本地流返回 `ErrRefusedStream`。

### 5.6 SETTINGS

- MUST 是会话的**第一个帧**；每个方向只发一次。
- `Stream ID` MUST 为 0；Length MUST 为 28。
- payload 布局：

| Offset | Size | 字段 | 说明 |
|---|---|---|---|
| 0 | 2 | version | MUST 为 3 |
| 2 | 2 | flags | 保留，MUST 为 0 |
| 4 | 4 | max_frame_size | PSH payload 上限；1..65535 |
| 8 | 4 | initial_stream_window | 每流初始窗口；1..2^31-1 |
| 12 | 4 | initial_session_window | session 初始窗口；1..2^31-1 |
| 16 | 4 | keepalive_interval_ms | 0 = 禁用 |
| 20 | 4 | keepalive_timeout_ms | 必须 >= interval（若启用） |
| 24 | 4 | max_concurrent_streams | 对端可同时发起的流数；0 = 不允许新流，0xFFFFFFFF = 不限 |

- 收到 SETTINGS 前，双方使用 §13 的默认值；SETTINGS 只允许**增大**窗口，减小为 protocol error。
- SETTINGS 的 `max_frame_size` 只约束 PSH payload；控制帧长度固定，不受此限制。
- 重复 SETTINGS：protocol error（v3 不支持动态更新）。

---

## 6. 会话生命周期

```
        创建 session
             │
             ▼
  发送 SETTINGS（第一个帧）
             │
             ▼
         OPEN  ◄──────────────┐
        /    \                │
   GOAWAY     IO error        │
      │          │            │
      ▼          ▼            │
  DRAINING    ABORTED ────────┘
      │
  所有流关闭 / 超时
      ▼
   CLOSED
```

规则：

1. 双方创建 session 后 MUST 立即发送 SETTINGS，作为本方向第一个帧。
2. 发送 SETTINGS 后即可发送数据；**不需要等待对端 SETTINGS**，使用默认窗口。
3. 收到第一个非 SETTINGS 帧 → protocol error，关闭连接。
4. 收到版本 != 3 的第一个字节 → 立即关闭，不做 v1/v2 回退。
5. `GOAWAY` 后停止发起新流；存量流继续。
6. `Session.Close(ctx)`：发送 GOAWAY，等待存量流结束或 ctx 超时，然后关闭 conn。
7. `Session.Abort()`：立即关闭 conn，所有流返回 `ErrSessionClosed`。
8. 下面任一情况立即 abort：
   - 底层读/写错误；
   - protocol error / flow-control error；
   - keepalive timeout；
   - 未知流丢弃预算耗尽。

---

## 7. 流生命周期

### 7.1 Stream ID

- client 使用奇数：1, 3, 5, ...
- server 使用偶数：2, 4, 6, ...
- stream id 0 保留给 session 级控制（`UPD sid=0`），MUST NOT 用作流 id。
- 每次 +2；MUST NOT 复用。
- 对端发起的流 id 必须符合对端 parity，否则 protocol error。
- 对端发起的 id 必须严格递增；`F_SYN` 的 id <= 已接受的最高对端 id → protocol error。
- id 耗尽：发送 `GOAWAY(last_stream_id, NO_ERROR)`，本端 `OpenStream` 返回 `ErrStreamIDExhausted`。

### 7.2 状态机

| 状态 | 含义 |
|---|---|
| `OPEN` | 双向可读写 |
| `HALF_LOCAL` | 本地已发 FIN，仍可读 |
| `HALF_REMOTE` | 已收 FIN，仍可写 |
| `CLOSING` | 双向 FIN 已交换，等待应用读完剩余数据 |
| `RESET` | RST 已发或已收 |
| `CLOSED` | 资源已回收 |

转移：

| 事件 | 当前状态 | 新状态 | 动作 |
|---|---|---|---|
| 本地发 `F_SYN` | — | `OPEN` | 分配 id |
| 收 `F_SYN` | — | `OPEN` | 创建流，入 accept 队列 |
| 本地 `CloseWrite` / FIN | `OPEN`/`HALF_REMOTE` | `HALF_LOCAL`/`CLOSING` | 发 FIN |
| 收 `F_FIN` | `OPEN`/`HALF_LOCAL` | `HALF_REMOTE`/`CLOSING` | 数据交付后 EOF |
| 双方 FIN 且数据读完 | `CLOSING` | `CLOSED` | 回收 |
| 本地 `Close` / RST | 任意 | `RESET` | 发 RST，回收 |
| 收 `RST` | 任意 | `RESET` | 丢弃数据，回收 |
| 收 FIN 后继续收数据 | `HALF_REMOTE`/`CLOSING` | — | protocol error / RST |

### 7.3 打开与接受

- `F_SYN` 创建流后立即进入 accept 队列；payload 可以先缓存。
- `AcceptStream` 从队列取出流；取出即视为“已接受”。
- 若 RST 在流被 Accept 之前到达：从 accept 队列移除，**不交付**给应用，直接回收。
- 若流已被 Accept：`Read`/`Write` 返回 `StreamError{Code}`。
- FIN 不把流从 accept 队列移除；应用会看到带 EOF 的流。
- accept 队列长度有上限；满时对新 `F_SYN` 回 `RST(REFUSED_STREAM)`，丢弃 payload 并回补 credit。

### 7.4 关闭语义

- `CloseWrite()`：发送 FIN（可带最后一块数据），之后本地写返回 `ErrClosedPipe`，仍可读。
- `Close()`：
  - 若本地未发 FIN 且没有未读数据：发 FIN；
  - 若存在未读数据、或对端未 FIN、或流已异常：发 `RST(CANCEL)`；
  - 本地资源立即释放，不等待对端。
- `Reset(err)`：直接发 RST，丢弃双向数据。
- `CloseRead()`：v3 建议等价于 `RST(CANCEL)`（不引入额外的 `STOP_SENDING` 类型）；如果后续需要真正的半关闭读，再新增帧类型。

### 7.5 错误

- RST / GOAWAY 的 error code 映射到 `StreamError` / `SessionError`。
- `errors.Is(err, ErrStreamReset)`、`ErrRefusedStream`、`ErrFlowControl`、`ErrProtocol` 等。
- 应用关闭流不应导致整个 session 关闭；只有协议/IO/流控错误才关闭 session。

---

## 8. 流量控制

### 8.1 两级窗口

| 级别 | 作用域 | 初始值（默认） | 更新帧 |
|---|---|---|---|
| stream | 每个方向、每条流 | 1 MiB | `UPD sid=S` / `F_CREDIT` |
| session | 每个方向、整个会话 | 8 MiB | `UPD sid=0` |

- 发送方维护 `sent` 计数器；接收方维护 `consumed` 计数器（绝对值，u32）。
- 可用窗口 = `window - int32(sent - consumed)`；必须 >= 0。
- 接收方通过 UPD / piggyback 告知 `consumed`；发送方更新 `peerConsumed`。
- 计数器按模 2^32 运算；窗口 MUST <= 2^31-1。

### 8.2 更新规则

- 流窗口：`consumed - lastSentConsumed >= window/2` 时发送 `UPD sid=S`。
- session 窗口：`consumed - lastSentSessionConsumed >= sessionWindow/2` 时发送 `UPD sid=0`。
- 应用停止读取时，接收方不更新窗口，发送方自然阻塞（背压）。
- 接收方关闭流时，丢弃的已接收字节仍计入 session consumed，并立即回补 session credit。
- 丢弃未知/已关闭流的数据时，同样计入 session consumed 并回补，避免发送方永久阻塞。

### 8.3 Piggyback

- PSH 可以置 `F_CREDIT`，在 header 后携带 4 字节 absolute consumed。
- 该 credit 只针对**同一 stream id 的反方向**；不用于 session。
- 推荐在 `consumed - lastSentConsumed >= window/4` 时顺带携带；独立 UPD 作为兜底。
- 对于纯单向流（下载），接收方没有反向数据可 piggyback，只能发 UPD；因此窗口要大、阈值要大。

### 8.4 初始值与握手

- SETTINGS 之前使用默认窗口（1 MiB / 8 MiB）。
- SETTINGS 只能增大窗口；不能减小。
- 发送方在收到 SETTINGS 之前 MUST NOT 超过默认窗口发送。
- 接收方 MUST 能缓冲最多 advertised 窗口的数据；实际内存可以懒分配。
- 超过窗口发送 → `FLOW_CONTROL_ERROR`，关闭 session。

### 8.5 丢弃与预算

- 未知流 / 已关闭流 / RST 之后的 PSH：
  - 必须读取并丢弃 payload；
  - 计入 session consumed；
  - 立即回补 session credit；
  - 计入 unknown-stream budget（默认 1 MiB 或 8 × session window，取较大者）。
- 超过预算 → `GOAWAY(ENHANCE_YOUR_CALM)` 并关闭。
- 对端对已 RST 的流继续发数据，同样走上述逻辑；发送方应尽快停止。

---

## 9. Keepalive 与 RTT

- 每个方向独立配置 `keepalive_interval_ms` / `keepalive_timeout_ms`。
- 超过 interval 未发送 PING，发送一个 PING（nonce 单调递增）。
- 收到 PING MUST 立即回 PONG。
- 任何合法帧都刷新 liveness 计时；timeout 内没有任何帧 → abort。
- PONG 用于计算 RTT；实现可以据此调整批量大小 / 窗口（可选）。
- PING/PONG 属于控制帧，优先于数据发送。

---

## 10. 顺序与公平性（规范性）

1. 同一流、同一方向的数据 MUST 按写入顺序发送；FIN MUST 在该方向所有数据之后。
2. 多流之间 SHOULD 轮转（round-robin），单条流不得长期独占连接。
3. 控制帧（SETTINGS/UPD/RST/PING/PONG/GOAWAY）SHOULD 优先于数据，但连续控制帧不得超过每批 16 帧，避免饿死数据。
4. 批量写只允许合并**已经排队**的帧，MUST NOT 为了凑批而延迟发送。
5. 单批字节数 SHOULD 有上限（建议 128 KiB，可配置；控制帧延迟敏感场景可降到 64 KiB），以限制控制帧延迟。

---

## 11. 错误码

| Code | 名称 | 场景 |
|---|---|---|
| 0x00 | NO_ERROR | 正常关闭 / GOAWAY |
| 0x01 | PROTOCOL_ERROR | 非法帧、状态机违规、重复 SYN |
| 0x02 | FLOW_CONTROL_ERROR | 超窗口、credit 回退 |
| 0x03 | STREAM_CLOSED | 已关闭流收到数据 |
| 0x04 | STREAM_ID_EXHAUSTED | stream id 用完 |
| 0x05 | FRAME_SIZE_ERROR | payload 超过 max_frame_size |
| 0x06 | INTERNAL_ERROR | 实现内部错误 |
| 0x07 | CANCEL | 应用主动取消 |
| 0x08 | REFUSED_STREAM | accept 队列满 / 并发流超限 |
| 0x09 | ENHANCE_YOUR_CALM | 未知流丢弃预算耗尽 |

---

## 12. 默认值与限制

| 项 | 默认值 | 范围 |
|---|---|---|
| version | 3 | 固定 |
| max_frame_size | 32768（协议默认/最小值；v3 运行时默认 65535） | 1..65535 |
| initial_stream_window | 1 MiB | 1..2^31-1 |
| initial_session_window | 8 MiB | 1..2^31-1 |
| keepalive_interval | 10s | 0 或 >0 |
| keepalive_timeout | 30s | >= interval |
| max_concurrent_streams | 0xFFFFFFFF | 0..0xFFFFFFFF |
| unknown_stream_budget | max(1 MiB, 8 × session_window) | >0 |
| accept backlog | 1024 | >0 |
| 单批上限 | 64 KiB | 1..1 MiB |
| 单批最大帧数 | 64 | 1..256 |

---

## 13. 安全与 DoS 防护

1. `max_frame_size`、窗口、SETTINGS 数值必须做范围校验，防止整数溢出。
2. 未知流丢弃必须有预算，且预算耗尽后关闭 session。
3. accept 队列有界；满时 RST(REFUSED_STREAM)，不能阻塞 recvLoop。
4. 并发流数量受 `max_concurrent_streams` 限制；超限 RST(REFUSED_STREAM)。
5. 所有长度/flag 组合在解析阶段校验，非法输入不得导致 panic 或越界。
6. 对端可以发送大量零长度 PING/SYN 制造 CPU 负载；实现应限制每批控制帧数量，并可对 PING 频率做本地限速。
7. 流控计数器使用模运算，禁止负 inflight；任何负值都视为协议错误。
8. 不在日志中打印 payload；错误信息只包含类型、sid、长度、错误码。

---

## 14. 版本与扩展

- 第一个字节是 wire version；v3 不兼容 v1/v2，收到其他版本直接关闭。
- Type 0x7-0xF、Flags bit3 保留；收到非 0 视为 protocol error。
- 未来扩展优先使用新的 Type，而不是修改现有帧布局。
- API 版本与 wire version 独立；本项目模块路径为 `github.com/sjlit/mux`，按 v0.x 发布，wire version 固定为 3。

---

## 15. 测试向量

### 15.1 SETTINGS（默认值）

```
03 60 1C 00 00 00 00 00
03 00 00 00
00 80 00 00
00 00 10 00
00 00 80 00
10 27 00 00
30 75 00 00
FF FF FF FF
```

含义：SETTINGS，max_frame=32768，stream_window=1 MiB，session_window=8 MiB，keepalive=10s，timeout=30s，max_streams=unlimited。

### 15.2 PSH + SYN，sid 1，payload "hello"

```
03 01 05 00 01 00 00 00 68 65 6C 6C 6F
```

### 15.3 PSH + FIN + CREDIT，sid 1，credit=65536，payload "hi"

```
03 06 02 00 01 00 00 00 00 00 01 00 68 69
```

### 15.4 UPD session，consumed=1 MiB

```
03 10 04 00 00 00 00 00 00 00 10 00
```

### 15.5 RST，sid 3，code=CANCEL(7)

```
03 20 04 00 03 00 00 00 07 00 00 00
```

### 15.6 PING / PONG，nonce=42

```
03 30 00 00 2A 00 00 00
03 40 00 00 2A 00 00 00
```

### 15.7 GOAWAY，last_stream_id=99，code=NO_ERROR

```
03 50 04 00 63 00 00 00 00 00 00 00
```

---

## 16. Codec 实现约定

建议放在独立包（例如 `wire` 或 `internal/wire`），不依赖 `Session`/`stream`：

```go
const (
    Version      = 3
    HeaderSize   = 8
    CreditSize   = 4
    SettingsSize = 28
)

type Type uint8

const (
    TypePSH Type = 0
    TypeUPD Type = 1
    TypeRST Type = 2
    TypePING Type = 3
    TypePONG Type = 4
    TypeGOAWAY Type = 5
    TypeSETTINGS Type = 6
)

type Flags uint8

const (
    FlagSYN    Flags = 1 << 0
    FlagFIN    Flags = 1 << 1
    FlagCredit Flags = 1 << 2
)

type Frame struct {
    Type     Type
    Flags    Flags
    StreamID uint32
    Credit   uint32 // valid iff Flags&FlagCredit != 0
    Payload  []byte // sub-slice of input; no copy
}

// DecodeFrame parses one frame from b.
// Returns the number of bytes consumed.
// Must not panic for any input.
func DecodeFrame(b []byte, maxFrameSize uint32) (Frame, int, error)

// AppendFrame encodes f into dst and returns the extended slice.
// Must validate f before encoding.
func AppendFrame(dst []byte, f Frame) ([]byte, error)

// EncodedLen returns the number of bytes AppendFrame would emit for f.
func EncodedLen(f Frame) (int, error)
```

辅助函数：

```go
func NewUPDFrame(streamID, consumed uint32) Frame
func NewRSTFrame(streamID uint32, code ErrorCode) Frame
func NewPINGFrame(nonce uint32) Frame
func NewPONGFrame(nonce uint32) Frame
func NewGOAWAYFrame(lastStreamID uint32, code ErrorCode) Frame
func NewSettingsFrame(s Settings) (Frame, error)

func (f Frame) Consumed() (uint32, error) // UPD only
func (f Frame) Code() (ErrorCode, error)  // RST / GOAWAY only
```

校验规则（`DecodeFrame` 与 `AppendFrame` 共用）：

- `len(b) < HeaderSize` → `ErrShortFrame`
- `Version != 3` → `ErrBadVersion`
- `Type > TypeSETTINGS` → `ErrBadType`
- `Flags & ^(FlagSYN|FlagFIN|FlagCredit) != 0` → `ErrBadFlags`
- `Length > maxFrameSize`（仅 PSH payload）→ `ErrFrameTooLarge`
- 非 PSH 带 `F_SYN`/`F_FIN`/`F_CREDIT` → `ErrBadFlags`
- `TypeUPD`/`RST`/`GOAWAY` 的 Length != 4 → `ErrBadLength`
- `TypeSETTINGS` 的 Length != 28 或 StreamID != 0 → `ErrBadSettings`
- `TypePING`/`PONG` 的 Length != 0 → `ErrBadLength`
- `F_CREDIT` 时 `len(b) < HeaderSize+CreditSize+Length` → `ErrShortFrame`
- 返回的 `Payload` MUST 是 `b` 的子切片，零拷贝。
- 所有错误用 `errors.Is` 可判断的 sentinel。

---

## 17. Fuzz / 属性测试计划

### 17.1 单元测试

- 每种 Type 的正常编解码。
- 每种非法组合（长度、flag、sid、version、reserved）。
- SETTINGS 边界值：最大/最小窗口、0、2^31、2^32-1。
- 流控模运算：回绕、stale update、负 inflight。
- 状态机：重复 SYN、FIN 后数据、RST 幂等、RST-before-accept。

### 17.2 Fuzz

```go
func FuzzDecodeFrame(f *testing.F) {
    // seed with §15 test vectors
    f.Fuzz(func(t *testing.T, data []byte) {
        fr, n, err := DecodeFrame(data, 32768)
        if err == nil {
            if n > len(data) { t.Fatal("consumed > input") }
            if n < HeaderSize { t.Fatal("consumed < header") }
            // invariants...
        }
    })
}
```

- `FuzzDecodeFrame`：任意输入不 panic、不越界、consumed 合法。
- `FuzzDecodeFrames`：把输入随机切成多个 chunk，模拟 TCP 分片/粘包，验证解析结果与一次性解析一致。
- `FuzzSettings`：随机 28 字节，校验窗口/帧长/keepalive 约束。
- `FuzzRoundTrip`：对合法 Frame 执行 `Decode(Append(x)) == x`。
- 可选：与参考实现（简单逐字节解析器）做差分测试。

### 17.3 集成测试

- 随机并发读写、随机 RST、随机 deadline、随机窗口（小到 1 字节）。
- 数据完整性：写入字节流与读出字节流逐字节一致；EOF/RST 语义正确。
- 泄漏：所有流关闭后，ring buffer / frame block / batch 对象计数归零。
- 故障注入：短写、连接错误、部分帧、超时、并发 Close。
- `-race`、32 位构建、soak（大流量 + 多流）。

---

## 18. 设计决策（已确认）

以下决策已确认，作为 v3 的默认行为：

| # | 决策 | 建议 |
|---|---|---|
| 1 | 默认窗口 | stream 1 MiB / session 8 MiB |
| 2 | SETTINGS 是否可重复 | 仅一次，重复视为 protocol error |
| 3 | RST-before-accept | 从 accept 队列移除，不交付应用 |
| 4 | `CloseRead` | 等价 RST(CANCEL)，不新增 STOP_SENDING |
| 5 | PING nonce | 用 Stream ID 字段，Length=0 |
| 6 | 默认 max_frame_size | 32768（可配到 65535） |
| 7 | `Session.Close` | 默认优雅 GOAWAY + 超时；另有 `Abort()` |
| 8 | GOAWAY 语义 | 双向停止新流，存量流继续 |
| 9 | max_concurrent_streams 默认 | 0xFFFFFFFF（不限），实现可配 |
| 10 | credit piggyback | 保留 `F_CREDIT`，默认开启 |

---

## 19. 变更记录

| 日期 | 变更 |
|---|---|
| 2026-11-01 | 初稿：8 字节头、SYN/FIN 合并、RST/GOAWAY/PING、两级 credit、codec/fuzz 约定 |
