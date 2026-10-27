// shim.dll — 通用 Win32 消息桥，注入到目标 Wine 进程（如 TdxW.exe）。
// 控制通道：TCP 127.0.0.1，端口由 C:\windows\temp\tdx_shim_port.txt
// 指定（Python 注入前写入）。
// 协议：请求一行 `cmd arg...\n`；应答 0..N 行 JSON 记录，末行 `ok` 或 `err
// <原因>`。 target = 0x.. 或 class=<类名>；[target] 缺省 = 本进程全部顶层窗口。
//   ping                          {pid}
//   treegen                       {gen}
//   treewatch                     长连接，窗口树变更即推 {gen}（不收尾）
//   enum [target]                 节点记录，只展开一层
//   tree [target]                 节点记录，递归展开
//   find class=<cls> [title=<t>]  命中则一条节点记录
//   rect <target>                 一条节点记录
//   gettext <target>              {text}
//   settext <target> <utf8>       {ret}
//   post <target> <msg> <wp> <lp> {ret}  PostMessageW
//   send <target> <msg> <wp> <lp> {ret}  SendMessageW
//   click <target>                {ret}  BM_CLICK
//   clickat <target> <x> <y>      {client:[x,y]}  屏幕坐标投递左键按下+抬起
//   highlight <target>            控件上画闪烁框
//   detail <target>               一条图层记录，items 为 [l,t,r,b,st,"文字"]：
//                                 列表行/格、表头、树项、菜单项、勾选态
//   mouse <kind> <x> <y> [mk] [delta]  {hwnd,client}  屏幕坐标投递鼠标消息
//                                 kind=move/ldown/lup/ldbl/rdown/rup/rdbl/wheel
//   key <vk>                      {hwnd}  焦点窗口投递按下+抬起
//   char <codepoint>              {hwnd}  焦点窗口投递一个字符
//   noraise [on|off]              {on,patched,blocked}  不抬窗：拦顶层窗口 z
//   序变化（默认开；on 重扫模块） spy on [msg=<id>|all|cmd] / off / clear /
//   dump  默认 msg=0x111(WM_COMMAND)；cmd = WM_COMMAND + 发给非标准控件的
//   WM_USER..0xFFFF 私有消息
//   gdi [on|off]                  {on,patched,layers,items,evicted}  绘制层：钩
//   文字/填色/贴图入口，按窗口记录可见文字与色块（on 首次装钩并重绘全部窗口；
//   patched = 已装入口数，装不上的先各回一行 {skip}；evicted = 图层满时挤掉
//   的内存位图图层数）
//   gdidump                       全部窗口图层各一条图层记录
//   gdiwatch 长连接，窗口绘制层变更即推变了的图层记录（不收尾） help / spy help
//   req on / off / clear / drain  交易请求对象追踪（只记录，不改参数）
//   req attach <hwnd> [offset]     从 MFC 窗口对象接入宿主请求接口；offset
//   缺省自动找 {cmds}
// 节点记录：{depth,hwnd,id,class,title,rect:[l,t,r,b],style,exstyle,vis,en}
// 图层记录：{hwnd,off:[客户区相对窗口左上],items:[[l,t,r,b,v]|[l,t,r,b,v,"文字"]]}
//   客户区坐标，后出的在上；v 在 gdi 为色 0x00BBGGRR，在 detail 为 st 位。
// 长连接（treewatch / gdiwatch）：无变更 1s 推一次心跳 {}。
// 编译：见 shimctl.py（i686-w64-mingw32-gcc 或 clang
// --target=i686-w64-windows-gnu）。

#include <winsock2.h>

#include <windows.h>

#include <commctrl.h>
#include <tlhelp32.h>

#include <assert.h>
#include <limits.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define DEFAULT_PORT 17703
#define SPY_CAP 512
#define REQ_CAP 4096
#define HOOK_CAP 256
#define FILTER_ALL 0xFFFFFFFFu
#define FILTER_CMD 0xFFFFFFFEu
#define CLS_CAP (256 * 3 + 1) // 256 个 wchar 转 UTF-8 的上界
#define TTL_CAP (512 * 3 + 1)
#define HX(h) ((unsigned)(DWORD_PTR)(h))

static HMODULE g_hMod = NULL;
static CRITICAL_SECTION g_cs;
static CRITICAL_SECTION g_req_cs;
static int g_cs_inited = 0;

// ---------- 应答缓冲 / JSON ----------
struct out {
  SOCKET sock;
  int detach; // watch 接走 socket：不收尾、不关
  char *buf;
  size_t cap, len;
};

static void put(struct out *o, const char *s, size_t n) {
  if (o->len + n + 1 > o->cap) {
    o->cap = (o->len + n + 1) * 2;
    o->buf = (char *)realloc(o->buf, o->cap);
    assert(o->buf);
  }
  memcpy(o->buf + o->len, s, n);
  o->len += n;
  o->buf[o->len] = 0;
}

// fmt 只含定长数值与常量串；变长字符串一律走 emit_str。
static void emit(struct out *o, const char *fmt, ...) {
  char tmp[256];
  va_list ap;
  va_start(ap, fmt);
  int n = vsnprintf(tmp, sizeof(tmp), fmt, ap);
  va_end(ap);
  assert(n >= 0 && n < (int)sizeof(tmp));
  put(o, tmp, n);
}

// JSON 字符串：控制符与 " \ 一律 \u00XX，其余字节（UTF-8）原样。
static void emit_str(struct out *o, const char *s) {
  const char *seg = s;
  put(o, "\"", 1);
  for (; *s; s++) {
    unsigned char c = (unsigned char)*s;
    if (c >= 0x20 && c != '"' && c != '\\')
      continue;
    put(o, seg, s - seg);
    emit(o, "\\u%04x", c);
    seg = s + 1;
  }
  put(o, seg, s - seg);
  put(o, "\"", 1);
}

static void w2u(const wchar_t *w, char *u, int cap) {
  int n = WideCharToMultiByte(CP_UTF8, 0, w, -1, u, cap, NULL, NULL);
  assert(n > 0);
}

static void u2w(const char *u, wchar_t *w, int cap) {
  int n = MultiByteToWideChar(CP_UTF8, 0, u, -1, w, cap);
  assert(n > 0);
}

// 图层记录：layer_open 与 layer_close 之间逐个 emit_elem。
static void layer_open(struct out *o, HWND h, POINT off) {
  emit(o, "{\"hwnd\":\"0x%08x\",\"off\":[%ld,%ld],\"items\":[", HX(h),
       (long)off.x, (long)off.y);
}

static void layer_close(struct out *o) { emit(o, "]}\n"); }

// w=NULL 即无文字（色块）。
static void emit_elem(struct out *o, const RECT *r, unsigned v,
                      const wchar_t *w) {
  char u[TTL_CAP];
  if (o->buf[o->len - 1] != '[')
    put(o, ",", 1);
  emit(o, "[%ld,%ld,%ld,%ld,%u", (long)r->left, (long)r->top, (long)r->right,
       (long)r->bottom, v);
  if (w) {
    w2u(w, u, sizeof(u));
    put(o, ",", 1);
    emit_str(o, u);
  }
  put(o, "]", 1);
}

// 客户区左上相对窗口左上；窗口已销毁回 0。
static int client_off(HWND h, POINT *off) {
  RECT wr = {0, 0, 0, 0};
  POINT p = {0, 0};
  int alive = IsWindow(h) && GetWindowRect(h, &wr) && ClientToScreen(h, &p);
  off->x = p.x - wr.left;
  off->y = p.y - wr.top;
  return alive;
}

// ---------- 本进程枚举 ----------
// fn 对每项回一个计数，each_* 回其和。模块不含各版
// shim_*.dll，线程不含调用线程。
typedef int (*module_fn)(HMODULE m, void *ctx);
typedef int (*thread_id_fn)(DWORD tid, void *ctx);
typedef int (*window_fn)(HWND h, void *ctx);

static int each_module(module_fn fn, void *ctx) {
  HANDLE snap = CreateToolhelp32Snapshot(TH32CS_SNAPMODULE, 0);
  assert(snap != INVALID_HANDLE_VALUE);
  MODULEENTRY32 me;
  me.dwSize = sizeof(me);
  int n = 0;
  for (BOOL ok = Module32First(snap, &me); ok; ok = Module32Next(snap, &me))
    if (me.hModule != g_hMod && _strnicmp(me.szModule, "shim_", 5))
      n += fn(me.hModule, ctx);
  CloseHandle(snap);
  return n;
}

static int each_thread(thread_id_fn fn, void *ctx) {
  HANDLE snap = CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0);
  assert(snap != INVALID_HANDLE_VALUE);
  THREADENTRY32 te;
  te.dwSize = sizeof(te);
  int n = 0;
  for (BOOL ok = Thread32First(snap, &te); ok; ok = Thread32Next(snap, &te))
    if (te.th32OwnerProcessID == GetCurrentProcessId() &&
        te.th32ThreadID != GetCurrentThreadId())
      n += fn(te.th32ThreadID, ctx);
  CloseHandle(snap);
  return n;
}

struct each_win {
  window_fn fn;
  void *ctx;
  int n;
};

static BOOL CALLBACK each_window_proc(HWND h, LPARAM lp) {
  struct each_win *e = (struct each_win *)lp;
  DWORD pid = 0;
  GetWindowThreadProcessId(h, &pid);
  if (pid == GetCurrentProcessId())
    e->n += e->fn(h, e->ctx);
  return TRUE;
}

// 顶层窗口。
static int each_window(window_fn fn, void *ctx) {
  struct each_win e = {fn, ctx, 0};
  EnumWindows(each_window_proc, (LPARAM)&e);
  return e.n;
}

// 命令：h 为解析好的 target（ARG_NONE / 缺省 ARG_OPT 时为 NULL），a
// 为其余参数。 回 NULL = ok，否则为 err 原因。
typedef const char *(*cmd_fn)(struct out *o, HWND h, char *a);
enum { ARG_NONE, ARG_OPT, ARG_REQ };
struct cmd {
  const char *name;
  cmd_fn fn;
  int arg;
};

// ---------- 消息 spy ----------
struct spy_evt {
  DWORD msg;
  HWND hwnd;
  WPARAM wp;
  LPARAM lp;
};
static struct spy_evt g_spy[SPY_CAP];
static int g_spy_head = 0, g_spy_count = 0;
static DWORD g_filter = WM_COMMAND; // 默认只抓 WM_COMMAND
static HHOOK g_hooks[HOOK_CAP];
static int g_nhooks = 0;
static UINT g_exec_msg; // listener 启动时注册（见"目标线程内执行"）

// 标准控件的私有消息多是属主查询/驱动控件（列表、提示、滚动条…），不是按钮动作。
static const char *const NOISY[] = {
    "tooltips_class32",
    "SysListView32",
    "SysHeader32",
    "SysTreeView32",
    "SysTabControl32",
    "ToolbarWindow32",
    "ReBarWindow32",
    "msctls_",
    "ComboLBox",
    "ComboBox",
    "Edit",
    "ListBox",
    "Button",
    "Static",
    "ScrollBar",
    "RichEdit",
    "IME",
    "MSCTFIME",
    NULL,
};

static int spy_want(DWORD msg, HWND hwnd) {
  if (g_filter != FILTER_CMD)
    return g_filter == FILTER_ALL || msg == g_filter;
  if (msg == WM_COMMAND)
    return 1;
  if (msg < WM_USER || msg > 0xFFFF || msg == g_exec_msg)
    return 0;
  char cls[64] = {0};
  GetClassNameA(hwnd, cls, sizeof(cls));
  for (int k = 0; NOISY[k]; k++)
    if (!_strnicmp(cls, NOISY[k], strlen(NOISY[k])))
      return 0;
  return 1;
}

static void spy_record(DWORD msg, HWND hwnd, WPARAM wp, LPARAM lp) {
  if (!spy_want(msg, hwnd))
    return;
  EnterCriticalSection(&g_cs);
  g_spy[g_spy_head].msg = msg;
  g_spy[g_spy_head].hwnd = hwnd;
  g_spy[g_spy_head].wp = wp;
  g_spy[g_spy_head].lp = lp;
  g_spy_head = (g_spy_head + 1) % SPY_CAP;
  if (g_spy_count < SPY_CAP)
    g_spy_count++;
  LeaveCriticalSection(&g_cs);
}

static LRESULT CALLBACK spy_cwp(int code, WPARAM wp, LPARAM lp) {
  if (code == HC_ACTION) {
    CWPSTRUCT *c = (CWPSTRUCT *)lp;
    spy_record(c->message, c->hwnd, c->wParam, c->lParam);
  }
  return CallNextHookEx(NULL, code, wp, lp);
}

static LRESULT CALLBACK spy_gm(int code, WPARAM wp, LPARAM lp) {
  if (code == HC_ACTION && wp == PM_REMOVE) {
    MSG *m = (MSG *)lp;
    spy_record(m->message, m->hwnd, m->wParam, m->lParam);
  }
  return CallNextHookEx(NULL, code, wp, lp);
}

static const struct {
  int id;
  HOOKPROC fn;
} HOOKS[] = {{WH_CALLWNDPROC, spy_cwp}, {WH_GETMESSAGE, spy_gm}};

static void spy_unhook(void) {
  for (int i = 0; i < g_nhooks; i++)
    UnhookWindowsHookEx(g_hooks[i]);
  g_nhooks = 0;
}

static int spy_hook_thread(DWORD tid, void *ctx) {
  int n = 0;
  for (size_t k = 0; k < sizeof(HOOKS) / sizeof(*HOOKS) && g_nhooks < HOOK_CAP;
       k++) {
    HHOOK hk = SetWindowsHookExA(HOOKS[k].id, HOOKS[k].fn, g_hMod, tid);
    if (hk) {
      g_hooks[g_nhooks++] = hk;
      n++;
    }
  }
  return n;
}

// 本进程每个线程各挂 HOOKS 全套。
static void spy_hook(void) { each_thread(spy_hook_thread, NULL); }

// ---------- 交易请求追踪 ----------
// AddinStock/AddinCredit 不直接导入 tc.dll；插件从宿主对象取
// session，再创建请求对象。 请求对象虚表在两种插件中一致：+14 初始化，+18
// 功能号，+1c/+24/+2c 写字符串/整数/浮点字段，+64
// 发送。这里只钩这六项，原样转调。
enum req_op {
  REQ_INIT,
  REQ_FUNC,
  REQ_STR,
  REQ_INT,
  REQ_FLOAT,
  REQ_SEND,
};

struct req_evt {
  DWORD tick, tid;
  DWORD self;
  int op, id;
  DWORD a, b, c, d;
  float f;
  char s[512];
};

static struct req_evt g_req[REQ_CAP];
static int g_req_head, g_req_count;
static volatile LONG g_req_on;
static int g_req_hooked;
static DWORD g_req_target[6];

#if defined(__GNUC__) || defined(__clang__)
#define TCALL __attribute__((thiscall))
#else
#define TCALL __thiscall
#endif

typedef DWORD(TCALL *req_init_fn)(void *, DWORD);
typedef DWORD(TCALL *req_func_fn)(void *, DWORD);
typedef DWORD(TCALL *req_str_fn)(void *, DWORD, const char *, DWORD);
typedef DWORD(TCALL *req_int_fn)(void *, DWORD, DWORD);
typedef DWORD(TCALL *req_float_fn)(void *, DWORD, float);
typedef DWORD(TCALL *req_send_fn)(void *, DWORD, DWORD, DWORD, DWORD);

static req_init_fn o_req_init;
static req_func_fn o_req_func;
static req_str_fn o_req_str;
static req_int_fn o_req_int;
static req_float_fn o_req_float;
static req_send_fn o_req_send;

static void req_record(int op, void *self, int id, DWORD a, DWORD b, DWORD c,
                       DWORD d, float f, const char *s) {
  if (!g_req_on)
    return;
  EnterCriticalSection(&g_req_cs);
  struct req_evt *e = &g_req[g_req_head];
  memset(e, 0, sizeof(*e));
  e->tick = GetTickCount();
  e->tid = GetCurrentThreadId();
  e->self = (DWORD)self;
  e->op = op;
  e->id = id;
  e->a = a;
  e->b = b;
  e->c = c;
  e->d = d;
  e->f = f;
  if (s && !IsBadStringPtrA(s, sizeof(e->s))) {
    strncpy(e->s, s, sizeof(e->s) - 1);
    e->s[sizeof(e->s) - 1] = 0;
  }
  g_req_head = (g_req_head + 1) % REQ_CAP;
  if (g_req_count < REQ_CAP)
    g_req_count++;
  LeaveCriticalSection(&g_req_cs);
}

static DWORD TCALL my_req_init(void *self, DWORD clear) {
  req_record(REQ_INIT, self, 0, clear, 0, 0, 0, 0, NULL);
  return o_req_init(self, clear);
}

static DWORD TCALL my_req_func(void *self, DWORD fn) {
  req_record(REQ_FUNC, self, 0, fn, 0, 0, 0, 0, NULL);
  return o_req_func(self, fn);
}

static DWORD TCALL my_req_str(void *self, DWORD id, const char *s, DWORD aux) {
  req_record(REQ_STR, self, (int)id, aux, 0, 0, 0, 0, s);
  return o_req_str(self, id, s, aux);
}

static DWORD TCALL my_req_int(void *self, DWORD id, DWORD v) {
  req_record(REQ_INT, self, (int)id, v, 0, 0, 0, 0, NULL);
  return o_req_int(self, id, v);
}

static DWORD TCALL my_req_float(void *self, DWORD id, float v) {
  req_record(REQ_FLOAT, self, (int)id, 0, 0, 0, 0, v, NULL);
  return o_req_float(self, id, v);
}

static DWORD TCALL my_req_send(void *self, DWORD a, DWORD b, DWORD c, DWORD d) {
  req_record(REQ_SEND, self, 0, a, b, c, d, 0, NULL);
  return o_req_send(self, a, b, c, d);
}

// ---------- 窗口解析 ----------
static HWND resolve_target(const char *s) {
  if (!strncmp(s, "class=", 6))
    return FindWindowA(s + 6, NULL);
  return (HWND)(DWORD_PTR)strtoul(s, NULL, 0);
}

// 首 token 取 target，其余(去掉前导空格)作为剩余参数返回。
static HWND split_target(char *args, char **rest) {
  char a1[128] = {0};
  char *p = args;
  size_t i = 0;
  while (*p && *p != ' ' && i < sizeof(a1) - 1)
    a1[i++] = *p++;
  a1[i] = 0;
  while (*p == ' ')
    p++;
  if (rest)
    *rest = p;
  return resolve_target(a1);
}

// ---------- 标题 ----------
// GetWindowTextW 读别的线程的窗口是同步跨线程 WM_GETTEXT（实测 ~2ms/次），整树
// 两千余节点要数秒；而 InternalGetWindowText 对本进程窗口直接读 WND 内部文本、
// 不发消息，且 SetWindowText/WM_SETTEXT 写的就是它（Wine 9 改标题不发
// NAMECHANGE，故不能靠事件作废缓存）。例外：Edit/ComboBox/RichEdit 的文本在控件
// 自己的缓冲里，内部文本是旧的，只对其中可见的走
// WM_GETTEXT（隐藏的数百个不读）。
static int text_in_control(const char *cls) {
  return !_stricmp(cls, "Edit") || !_stricmp(cls, "ComboBox") ||
         !_strnicmp(cls, "RichEdit", 8);
}

// 标题(UTF-8) 写入 out，cap ≥ TTL_CAP；cls 为 h 的类名。
static void ttl_get(HWND h, const char *cls, char *out, int cap) {
  wchar_t w[512] = {0};
  if (text_in_control(cls) && IsWindowVisible(h))
    GetWindowTextW(h, w, 512);
  else
    InternalGetWindowText(h, w, 512);
  w2u(w, out, cap);
}

// 类名(UTF-8) 写入 out，cap ≥ CLS_CAP。
static void cls_get(HWND h, char *out, int cap) {
  wchar_t w[256] = {0};
  GetClassNameW(h, w, 256);
  w2u(w, out, cap);
}

// ---------- 节点记录 / 遍历 ----------
static void emit_node(struct out *o, HWND h, int depth) {
  char cls[CLS_CAP], ttl[TTL_CAP];
  cls_get(h, cls, sizeof(cls));
  ttl_get(h, cls, ttl, sizeof(ttl));
  RECT r = {0, 0, 0, 0};
  GetWindowRect(h, &r);
  emit(o, "{\"depth\":%d,\"hwnd\":\"0x%08x\",\"id\":%d,\"class\":", depth,
       HX(h), GetDlgCtrlID(h));
  emit_str(o, cls);
  emit(o, ",\"title\":");
  emit_str(o, ttl);
  emit(o,
       ",\"rect\":[%d,%d,%d,%d],\"style\":%u,\"exstyle\":%u,\"vis\":%d,"
       "\"en\":%d}\n",
       (int)r.left, (int)r.top, (int)r.right, (int)r.bottom,
       (unsigned)GetWindowLongW(h, GWL_STYLE),
       (unsigned)GetWindowLongW(h, GWL_EXSTYLE), IsWindowVisible(h) ? 1 : 0,
       IsWindowEnabled(h) ? 1 : 0);
}

// 先序：h 及其子孙，展开到第 limit 层。
static void walk(struct out *o, HWND h, int depth, int limit) {
  emit_node(o, h, depth);
  if (depth < limit)
    for (HWND c = GetWindow(h, GW_CHILD); c; c = GetWindow(c, GW_HWNDNEXT))
      walk(o, c, depth + 1, limit);
}

struct roots {
  struct out *o;
  int limit;
};

static int walk_root(HWND h, void *ctx) {
  struct roots *r = (struct roots *)ctx;
  walk(r->o, h, 0, r->limit);
  return 1;
}

// 以 h 为根；h 为空则以本进程全部顶层窗口为根。
static void walk_roots(struct out *o, HWND h, int limit) {
  struct roots r = {o, limit};
  if (h)
    walk_root(h, &r);
  else
    each_window(walk_root, &r);
}

// ---------- 命令：节点 ----------
static const char *cmd_enum(struct out *o, HWND h, char *a) {
  walk_roots(o, h, 1);
  return NULL;
}

static const char *cmd_tree(struct out *o, HWND h, char *a) {
  walk_roots(o, h, INT_MAX);
  return NULL;
}

// find class=<cls> [title=<ttl>]：title 取到行尾（可含空格）。
static const char *cmd_find(struct out *o, HWND h, char *a) {
  if (strncmp(a, "class=", 6))
    return "expected class=<cls>";
  char *cls = a + 6, *ttl = strstr(cls, " title=");
  if (ttl) {
    *ttl = 0;
    ttl += 7;
  }
  HWND f = FindWindowExA(NULL, NULL, cls, ttl);
  if (f)
    emit_node(o, f, 0);
  return NULL;
}

static const char *cmd_rect(struct out *o, HWND h, char *a) {
  emit_node(o, h, 0);
  return NULL;
}

// ---------- 命令：文本 ----------
static const char *cmd_gettext(struct out *o, HWND h, char *a) {
  int n = GetWindowTextLengthW(h) + 1;
  wchar_t *w = (wchar_t *)calloc(n, sizeof(wchar_t));
  char *u = (char *)malloc(n * 3);
  GetWindowTextW(h, w, n);
  w2u(w, u, n * 3);
  emit(o, "{\"text\":");
  emit_str(o, u);
  emit(o, "}\n");
  free(u);
  free(w);
  return NULL;
}

static const char *cmd_settext(struct out *o, HWND h, char *a) {
  int n = (int)strlen(a) + 1;
  wchar_t *w = (wchar_t *)calloc(n, sizeof(wchar_t));
  u2w(a, w, n);
  BOOL ok = SetWindowTextW(h, w);
  emit(o, "{\"ret\":%d}\n", ok);
  free(w);
  return NULL;
}

// ---------- 命令：消息 ----------
struct wmsg {
  UINT msg;
  WPARAM wp;
  LPARAM lp;
};

// "<msg> <wp> <lp>"，各数可带 0x 前缀。
static struct wmsg parse_wmsg(char *a) {
  struct wmsg m;
  m.msg = strtoul(a, &a, 0);
  m.wp = (WPARAM)strtoul(a, &a, 0);
  m.lp = (LPARAM)strtoul(a, &a, 0);
  return m;
}

static const char *cmd_post(struct out *o, HWND h, char *a) {
  struct wmsg m = parse_wmsg(a);
  if (!m.msg)
    return "msg=0";
  emit(o, "{\"ret\":%ld}\n", (long)PostMessageW(h, m.msg, m.wp, m.lp));
  return NULL;
}

static const char *cmd_send(struct out *o, HWND h, char *a) {
  struct wmsg m = parse_wmsg(a);
  if (!m.msg)
    return "msg=0";
  emit(o, "{\"ret\":%ld}\n", (long)SendMessageW(h, m.msg, m.wp, m.lp));
  return NULL;
}

static const char *cmd_click(struct out *o, HWND h, char *a) {
  emit(o, "{\"ret\":%ld}\n", (long)SendMessageW(h, BM_CLICK, 0, 0));
  return NULL;
}

// "<screenX> <screenY>" → 控件客户区坐标，投递左键按下+抬起。
static const char *cmd_clickat(struct out *o, HWND h, char *a) {
  POINT p;
  p.x = strtol(a, &a, 0);
  p.y = strtol(a, &a, 0);
  ScreenToClient(h, &p);
  LPARAM lp = MAKELPARAM(p.x, p.y);
  PostMessageW(h, WM_MOUSEMOVE, 0, lp);
  PostMessageW(h, WM_LBUTTONDOWN, MK_LBUTTON, lp);
  PostMessageW(h, WM_LBUTTONUP, 0, lp);
  emit(o, "{\"client\":[%ld,%ld]}\n", (long)p.x, (long)p.y);
  return NULL;
}

// 在控件屏幕矩形上用 DSTINVERT 画边框并闪烁，供肉眼确认定位是否正确。
static const char *cmd_highlight(struct out *o, HWND h, char *a) {
  RECT r = {0, 0, 0, 0};
  GetWindowRect(h, &r);
  HDC dc = GetDC(NULL);
  int t = 3, w = r.right - r.left, hgt = r.bottom - r.top, k;
  for (k = 0; k < 6; k++) {
    PatBlt(dc, r.left, r.top, w, t, DSTINVERT);
    PatBlt(dc, r.left, r.bottom - t, w, t, DSTINVERT);
    PatBlt(dc, r.left, r.top, t, hgt, DSTINVERT);
    PatBlt(dc, r.right - t, r.top, t, hgt, DSTINVERT);
    Sleep(150);
  }
  ReleaseDC(NULL, dc);
  return NULL;
}

// ---------- 目标线程内执行 ----------
// 跨线程 SendMessage
// 每次一个往返（~2ms），读一张列表要上百次；故临时给目标线程挂
// WH_CALLWNDPROC，发一条私有消息，在钩子里（即目标线程内）一次读完。
typedef void (*thread_fn)(struct out *o, HWND h);
struct exec {
  thread_fn fn;
  struct out *o;
};

static LRESULT CALLBACK exec_cwp(int code, WPARAM wp, LPARAM lp) {
  CWPSTRUCT *c = (CWPSTRUCT *)lp;
  if (code == HC_ACTION && c->message == g_exec_msg) {
    struct exec *e = (struct exec *)c->lParam;
    e->fn(e->o, c->hwnd);
  }
  return CallNextHookEx(NULL, code, wp, lp);
}

static const char *in_thread(struct out *o, HWND h, thread_fn fn) {
  DWORD pid = 0, tid = GetWindowThreadProcessId(h, &pid);
  if (pid != GetCurrentProcessId())
    return "target not in this process";
  struct exec e = {fn, o};
  HHOOK hk = SetWindowsHookExW(WH_CALLWNDPROC, exec_cwp, g_hMod, tid);
  assert(hk);
  DWORD_PTR r;
  LRESULT ok =
      SendMessageTimeoutW(h, g_exec_msg, 0, (LPARAM)&e, SMTO_NORMAL, 3000, &r);
  UnhookWindowsHookEx(hk);
  return ok ? NULL : "target thread busy (timeout)";
}

// ---------- 命令：detail（控件子项，均在目标线程内读）----------
// 一条图层记录，子项 [l,t,r,b,st,"文字"]（客户区坐标）；st 位：
enum {
  ST_SEL = 1,   // 选中 / 高亮 / 按下
  ST_CHECK = 2, // 勾选
  ST_KIDS = 4,  // 有子项（树）
  ST_OPEN = 8,  // 已展开（树）
  ST_GRAY = 16, // 禁用
  ST_SEP = 32,  // 分隔线（菜单）
  ST_SUB = 64,  // 有子菜单
  ST_ROW = 128, // 列表整行（其后紧跟该行各格）
};
#define DET_CAP 200 // 单控件最多出的行 / 项数
#define TXT_CAP 512 // 子项文本 wchar 上限（转 UTF-8 后 ≤ TTL_CAP）

static void det_button(struct out *o, HWND h) {
  RECT r;
  GetClientRect(h, &r);
  LRESULT s = SendMessageW(h, BM_GETSTATE, 0, 0);
  emit_elem(o, &r,
            (s & BST_PUSHED ? ST_SEL : 0) | (s & BST_CHECKED ? ST_CHECK : 0),
            L"");
}

static void det_listbox(struct out *o, HWND h) {
  RECT c;
  GetClientRect(h, &c);
  DWORD style = GetWindowLongW(h, GWL_STYLE);
  int strs = !(style & (LBS_OWNERDRAWFIXED | LBS_OWNERDRAWVARIABLE)) ||
             (style & LBS_HASSTRINGS);
  int n = (int)SendMessageW(h, LB_GETCOUNT, 0, 0);
  int top = (int)SendMessageW(h, LB_GETTOPINDEX, 0, 0);
  for (int i = top; i < n && i < top + DET_CAP; i++) {
    RECT r;
    SendMessageW(h, LB_GETITEMRECT, i, (LPARAM)&r);
    if (r.top >= c.bottom)
      break;
    wchar_t w[TXT_CAP] = {0};
    if (strs && SendMessageW(h, LB_GETTEXTLEN, i, 0) < TXT_CAP)
      SendMessageW(h, LB_GETTEXT, i, (LPARAM)w);
    emit_elem(o, &r, SendMessageW(h, LB_GETSEL, i, 0) > 0 ? ST_SEL : 0, w);
  }
}

static void det_header(struct out *o, HWND h) {
  int n = (int)SendMessageW(h, HDM_GETITEMCOUNT, 0, 0);
  for (int i = 0; i < n; i++) {
    RECT r;
    SendMessageW(h, HDM_GETITEMRECT, i, (LPARAM)&r);
    wchar_t w[TXT_CAP] = {0};
    HDITEMW it;
    memset(&it, 0, sizeof(it));
    it.mask = HDI_TEXT;
    it.pszText = w;
    it.cchTextMax = TXT_CAP;
    SendMessageW(h, HDM_GETITEMW, i, (LPARAM)&it);
    emit_elem(o, &r, 0, w);
  }
}

// 只出可见行：每行一条 ST_ROW（整行矩形 + 选中态），其后该行各列一条。
static void det_listview(struct out *o, HWND h) {
  RECT c;
  GetClientRect(h, &c);
  HWND hd = (HWND)SendMessageW(h, LVM_GETHEADER, 0, 0);
  int ncol = hd ? (int)SendMessageW(hd, HDM_GETITEMCOUNT, 0, 0) : 0;
  if (ncol < 1)
    ncol = 1; // 非报表视图：只有第 0 列
  int n = (int)SendMessageW(h, LVM_GETITEMCOUNT, 0, 0);
  int top = (int)SendMessageW(h, LVM_GETTOPINDEX, 0, 0);
  for (int i = top; i < n && i < top + DET_CAP; i++) {
    RECT r;
    r.left = LVIR_BOUNDS;
    SendMessageW(h, LVM_GETITEMRECT, i, (LPARAM)&r);
    if (r.top >= c.bottom)
      break;
    UINT s = (UINT)SendMessageW(h, LVM_GETITEMSTATE, i, LVIS_SELECTED);
    emit_elem(o, &r, ST_ROW | (s & LVIS_SELECTED ? ST_SEL : 0), L"");
    for (int j = 0; j < ncol; j++) {
      RECT rc;
      rc.top = j;
      rc.left = LVIR_LABEL;
      SendMessageW(h, LVM_GETSUBITEMRECT, i, (LPARAM)&rc);
      wchar_t w[TXT_CAP] = {0};
      LVITEMW it;
      memset(&it, 0, sizeof(it));
      it.iSubItem = j;
      it.pszText = w;
      it.cchTextMax = TXT_CAP;
      SendMessageW(h, LVM_GETITEMTEXTW, i, (LPARAM)&it);
      emit_elem(o, &rc, 0, w);
    }
  }
}

// 从首个可见项沿"下一个可见项"走到客户区底；rect 为文字矩形。
static void det_tree(struct out *o, HWND h) {
  RECT c;
  GetClientRect(h, &c);
  HTREEITEM t =
      (HTREEITEM)SendMessageW(h, TVM_GETNEXTITEM, TVGN_FIRSTVISIBLE, 0);
  for (int k = 0; t && k < DET_CAP; k++) {
    RECT r;
    *(HTREEITEM *)&r = t;
    if (!SendMessageW(h, TVM_GETITEMRECT, TRUE, (LPARAM)&r) ||
        r.top >= c.bottom)
      break;
    wchar_t w[TXT_CAP] = {0};
    TVITEMW it;
    memset(&it, 0, sizeof(it));
    it.mask = TVIF_TEXT | TVIF_STATE | TVIF_CHILDREN;
    it.hItem = t;
    it.stateMask = TVIS_SELECTED | TVIS_EXPANDED;
    it.pszText = w;
    it.cchTextMax = TXT_CAP;
    SendMessageW(h, TVM_GETITEMW, 0, (LPARAM)&it);
    emit_elem(o, &r,
              (it.state & TVIS_SELECTED ? ST_SEL : 0) |
                  (it.cChildren ? ST_KIDS : 0) |
                  (it.state & TVIS_EXPANDED ? ST_OPEN : 0),
              w);
    t = (HTREEITEM)SendMessageW(h, TVM_GETNEXTITEM, TVGN_NEXTVISIBLE,
                                (LPARAM)t);
  }
}

// 弹出菜单窗口(#32768)：MN_GETHMENU 取菜单，逐项读文本/状态/屏幕矩形。
static void det_menu(struct out *o, HWND h) {
  HMENU m = (HMENU)SendMessageW(h, MN_GETHMENU, 0, 0);
  int n = m ? GetMenuItemCount(m) : 0;
  for (int i = 0; i < n; i++) {
    wchar_t w[TXT_CAP] = {0};
    MENUITEMINFOW mi;
    memset(&mi, 0, sizeof(mi));
    mi.cbSize = sizeof(mi);
    mi.fMask = MIIM_FTYPE | MIIM_STATE | MIIM_STRING | MIIM_SUBMENU;
    mi.dwTypeData = w;
    mi.cch = TXT_CAP;
    GetMenuItemInfoW(m, i, TRUE, &mi);
    RECT r = {0, 0, 0, 0};
    GetMenuItemRect(NULL, m, i, &r);
    MapWindowPoints(NULL, h, (POINT *)&r, 2); // 屏幕 → 客户区
    emit_elem(o, &r,
              (mi.fState & MFS_HILITE ? ST_SEL : 0) |
                  (mi.fState & MFS_CHECKED ? ST_CHECK : 0) |
                  (mi.fState & MFS_DISABLED ? ST_GRAY : 0) |
                  (mi.fType & MFT_SEPARATOR ? ST_SEP : 0) |
                  (mi.hSubMenu ? ST_SUB : 0),
              w);
  }
}

static const struct {
  const char *cls;
  thread_fn fn;
} DETAILS[] = {
    {"Button", det_button},
    {"ListBox", det_listbox},
    {"ComboLBox", det_listbox},
    {"SysHeader32", det_header},
    {"SysListView32", det_listview},
    {"SysTreeView32", det_tree},
    {"#32768", det_menu},
    {NULL, NULL},
};

static const char *cmd_detail(struct out *o, HWND h, char *a) {
  char cls[CLS_CAP];
  POINT off;
  cls_get(h, cls, sizeof(cls));
  for (int k = 0; DETAILS[k].cls; k++) {
    if (_stricmp(cls, DETAILS[k].cls))
      continue;
    if (!client_off(h, &off))
      return "target gone";
    layer_open(o, h, off);
    const char *err = in_thread(o, h, DETAILS[k].fn);
    layer_close(o);
    return err;
  }
  return "no detail for this class";
}

// ---------- 命令：输入（鼠标 / 键盘）----------
// 一律 PostMessage 进目标线程队列：不动真实光标、不抢前台。
// 鼠标目标：有捕获（拖拽中 / 菜单循环）→ 捕获窗口，否则坐标下最深子窗口。
// 菜单循环中 lParam 给屏幕坐标（Wine 的菜单循环按屏幕坐标解读合成鼠标消息）。
static HWND g_last; // 最近一次鼠标目标：键盘找不到焦点窗口时兜底

static const struct {
  const char *name;
  UINT msg;
} MOUSE[] = {
    {"move", WM_MOUSEMOVE},
    {"ldown", WM_LBUTTONDOWN},
    {"lup", WM_LBUTTONUP},
    {"ldbl", WM_LBUTTONDBLCLK},
    {"rdown", WM_RBUTTONDOWN},
    {"rup", WM_RBUTTONUP},
    {"rdbl", WM_RBUTTONDBLCLK},
    {"wheel", WM_MOUSEWHEEL},
    {NULL, 0},
};

static GUITHREADINFO gui_info(HWND h) {
  GUITHREADINFO gi;
  memset(&gi, 0, sizeof(gi));
  gi.cbSize = sizeof(gi);
  BOOL ok = GetGUIThreadInfo(GetWindowThreadProcessId(h, NULL), &gi);
  assert(ok);
  return gi;
}

// mouse <kind> <x> <y> [mk] [delta]：x/y 屏幕坐标，mk 为 MK_* 位，delta
// 为滚轮量（±120/格）。
static const char *cmd_mouse(struct out *o, HWND h, char *a) {
  char kind[16] = {0};
  POINT p = {0, 0};
  unsigned mk = 0;
  int delta = 0;
  if (sscanf(a, "%15s %ld %ld %u %d", kind, &p.x, &p.y, &mk, &delta) < 3)
    return "usage: mouse <kind> <x> <y> [mk] [delta]";
  UINT msg = 0;
  for (int k = 0; MOUSE[k].name && !msg; k++)
    if (!strcmp(kind, MOUSE[k].name))
      msg = MOUSE[k].msg;
  if (!msg)
    return "unknown kind";
  HWND t = WindowFromPoint(p);
  if (!t)
    return "no window at point";
  GUITHREADINFO gi = gui_info(t);
  if (gi.hwndCapture)
    t = gi.hwndCapture;
  int menu = (gi.flags & GUI_INMENUMODE) != 0;
  POINT c = p;
  if (!menu && msg != WM_MOUSEWHEEL) // 滚轮 lParam 本就是屏幕坐标
    ScreenToClient(t, &c);
  if ((msg == WM_LBUTTONDBLCLK || msg == WM_RBUTTONDBLCLK) &&
      !(GetClassLongW(t, GCL_STYLE) & CS_DBLCLKS))
    msg -= 2; // 类不收双击：Win32 投递的是第二个 BUTTONDOWN
  int press = msg == WM_LBUTTONDOWN || msg == WM_LBUTTONDBLCLK ||
              msg == WM_RBUTTONDOWN || msg == WM_RBUTTONDBLCLK;
  LPARAM lp = MAKELPARAM(c.x, c.y);
  if (press) // 先移到位：自绘控件常靠 hover 态定命中
    PostMessageW(t, WM_MOUSEMOVE, mk & (MK_SHIFT | MK_CONTROL), lp);
  PostMessageW(t, msg, msg == WM_MOUSEWHEEL ? MAKEWPARAM(mk, delta) : mk, lp);
  if (!menu)
    g_last = t;
  emit(o, "{\"hwnd\":\"0x%08x\",\"client\":[%ld,%ld]}\n", HX(t), (long)c.x,
       (long)c.y);
  return NULL;
}

// 键盘目标：GUI 线程的焦点窗口 → 活动窗口 → 最近一次鼠标目标。
static HWND key_target(void) {
  if (!g_last || !IsWindow(g_last))
    return NULL;
  GUITHREADINFO gi = gui_info(g_last);
  return gi.hwndFocus ? gi.hwndFocus : gi.hwndActive ? gi.hwndActive : g_last;
}

// 按下 + 抬起；WM_CHAR 由客户端消息泵的 TranslateMessage 照常生成。
static void post_key(HWND t, UINT vk) {
  LPARAM lp = 1 | (LPARAM)MapVirtualKeyW(vk, MAPVK_VK_TO_VSC) << 16;
  if ((vk >= VK_PRIOR && vk <= VK_DOWN) || vk == VK_INSERT || vk == VK_DELETE)
    lp |= KF_EXTENDED << 16;
  PostMessageW(t, WM_KEYDOWN, vk, lp);
  PostMessageW(t, WM_KEYUP, vk, lp | (LPARAM)0xC0000000u); // 前态按下、转抬起
}

static const char *cmd_key(struct out *o, HWND h, char *a) {
  HWND t = key_target();
  if (!t)
    return "no key target (click the client first)";
  post_key(t, strtoul(a, NULL, 0));
  emit(o, "{\"hwnd\":\"0x%08x\"}\n", HX(t));
  return NULL;
}

// 无需修饰键就能打出的字符走 post_key（与真实按键同路径）；
// 其余（大写 / 符号 / 中文）直接投 WM_CHAR——投递的按键不改线程的 Shift 态。
static const char *cmd_char(struct out *o, HWND h, char *a) {
  HWND t = key_target();
  if (!t)
    return "no key target (click the client first)";
  unsigned cp = strtoul(a, NULL, 0);
  if (!cp || cp > 0xFFFF)
    return "codepoint must be in BMP";
  SHORT vs = VkKeyScanW((WCHAR)cp);
  if (vs != -1 && !(vs & 0xFF00))
    post_key(t, vs & 0xFF);
  else
    PostMessageW(t, WM_CHAR, cp, 1);
  emit(o, "{\"hwnd\":\"0x%08x\"}\n", HX(t));
  return NULL;
}

// ---------- 不抬窗（IAT 钩子）----------
// Wine 9 里能把 WM 托管的顶层窗口抬到 X 最前的，只有带 z
// 序变化（或对已可见窗口带 SWP_SHOWWINDOW）的 SetWindowPos：winex11 据此发
// XReconfigureWMWindow(Above)； SetForegroundWindow / SetActiveWindow /
// SetFocus 不动 X 层级。user32 的 SetWindowPos 直接转发到
// win32u!NtUserSetWindowPos，BringWindowToTop / DeferWindowPos / MDI 在 user32
// 内再调 win32u；故把所有已加载模块 IAT 里指向这两个 win32u
// 函数的项换成包装：对已可见的顶层窗口去掉 z 序变化，其余原样放行。
typedef BOOL(WINAPI *swp_fn)(HWND, HWND, int, int, int, int, UINT);
typedef HDWP(WINAPI *dwp_fn)(HDWP, HWND, HWND, int, int, int, int, UINT, UINT,
                             UINT);
static swp_fn g_swp;
static dwp_fn g_dwp;
static volatile LONG g_noraise = 1, g_blocked = 0;
static int g_patched; // 已改写的 IAT 项数（仅 listener 线程写）

static UINT noraise_flags(HWND h, UINT f) {
  if (!g_noraise || ((f & SWP_NOZORDER) && !(f & SWP_SHOWWINDOW)) ||
      !IsWindowVisible(h) || GetAncestor(h, GA_PARENT) != GetDesktopWindow())
    return f;
  InterlockedIncrement(&g_blocked);
  return (f | SWP_NOZORDER) & ~SWP_SHOWWINDOW;
}

static BOOL WINAPI my_swp(HWND h, HWND after, int x, int y, int cx, int cy,
                          UINT f) {
  return g_swp(h, after, x, y, cx, cy, noraise_flags(h, f));
}

static HDWP WINAPI my_dwp(HDWP d, HWND h, HWND after, int x, int y, int cx,
                          int cy, UINT f, UINT u1, UINT u2) {
  return g_dwp(d, h, after, x, y, cx, cy, noraise_flags(h, f), u1, u2);
}

// 模块 m 的导入描述表；无导入回 NULL。
static IMAGE_IMPORT_DESCRIPTOR *imports(HMODULE m) {
  BYTE *b = (BYTE *)m;
  IMAGE_DOS_HEADER *dos = (IMAGE_DOS_HEADER *)b;
  if (dos->e_magic != IMAGE_DOS_SIGNATURE)
    return NULL;
  IMAGE_NT_HEADERS *nt = (IMAGE_NT_HEADERS *)(b + dos->e_lfanew);
  IMAGE_DATA_DIRECTORY d =
      nt->OptionalHeader.DataDirectory[IMAGE_DIRECTORY_ENTRY_IMPORT];
  return d.VirtualAddress ? (IMAGE_IMPORT_DESCRIPTOR *)(b + d.VirtualAddress)
                          : NULL;
}

static void iat_write(IMAGE_THUNK_DATA *t, void *to) {
  DWORD old;
  BOOL ok = VirtualProtect(&t->u1.Function, sizeof(t->u1.Function),
                           PAGE_READWRITE, &old);
  assert(ok);
  t->u1.Function = (DWORD_PTR)to;
  VirtualProtect(&t->u1.Function, sizeof(t->u1.Function), old, &old);
}

// 模块 m 的 IAT 里值为 from 的项改成 to，回改写项数。
static int patch_iat(HMODULE m, void *from, void *to) {
  BYTE *b = (BYTE *)m;
  int n = 0;
  for (IMAGE_IMPORT_DESCRIPTOR *imp = imports(m); imp && imp->Name; imp++)
    for (IMAGE_THUNK_DATA *t = (IMAGE_THUNK_DATA *)(b + imp->FirstThunk);
         t->u1.Function; t++)
      if ((void *)t->u1.Function == from) {
        iat_write(t, to);
        n++;
      }
  return n;
}

static int noraise_mod(HMODULE m, void *ctx) {
  return patch_iat(m, (void *)g_swp, (void *)my_swp) +
         patch_iat(m, (void *)g_dwp, (void *)my_dwp);
}

// 扫本进程全部模块打补丁；已改过的项不再等于原函数，重扫只补新加载的模块。
static int noraise_patch(void) {
  if (!g_swp) {
    HMODULE w = GetModuleHandleA("win32u.dll");
    assert(w); // Wine 9 布局：user32 → win32u
    g_swp = (swp_fn)GetProcAddress(w, "NtUserSetWindowPos");
    g_dwp = (dwp_fn)GetProcAddress(w, "NtUserDeferWindowPosAndBand");
    assert(g_swp && g_dwp);
  }
  return each_module(noraise_mod, NULL);
}

static const char *cmd_noraise(struct out *o, HWND h, char *a) {
  if (!strcmp(a, "on")) {
    g_noraise = 1;
    g_patched += noraise_patch();
  } else if (!strcmp(a, "off"))
    g_noraise = 0;
  else if (*a)
    return "usage: noraise [on|off]";
  emit(o, "{\"on\":%ld,\"patched\":%d,\"blocked\":%ld}\n", (long)g_noraise,
       g_patched, (long)g_blocked);
  return NULL;
}

// ---------- 命令：spy ----------
// on [msg=<id>|all|cmd]：先摘后挂，重复 on 不叠钩子。
static const char *cmd_spy_on(struct out *o, HWND h, char *a) {
  const char *m = strstr(a, "msg=");
  g_filter = m                  ? strtoul(m + 4, NULL, 0)
             : strstr(a, "all") ? FILTER_ALL
             : strstr(a, "cmd") ? FILTER_CMD
                                : WM_COMMAND;
  spy_unhook();
  spy_hook();
  emit(o, "{\"hooks\":%d,\"filter\":\"0x%08x\"}\n", g_nhooks,
       (unsigned)g_filter);
  return NULL;
}

static const char *cmd_spy_off(struct out *o, HWND h, char *a) {
  spy_unhook();
  return NULL;
}

static const char *cmd_spy_clear(struct out *o, HWND h, char *a) {
  EnterCriticalSection(&g_cs);
  g_spy_head = g_spy_count = 0;
  LeaveCriticalSection(&g_cs);
  return NULL;
}

static const char *cmd_spy_dump(struct out *o, HWND h, char *a) {
  char cls[CLS_CAP];
  EnterCriticalSection(&g_cs);
  int start = g_spy_count < SPY_CAP ? 0 : g_spy_head;
  for (int i = 0; i < g_spy_count; i++) {
    struct spy_evt *e = &g_spy[(start + i) % SPY_CAP];
    cls_get(e->hwnd, cls, sizeof(cls));
    emit(o,
         "{\"msg\":\"0x%08x\",\"hwnd\":\"0x%08x\",\"class\":", (unsigned)e->msg,
         HX(e->hwnd));
    emit_str(o, cls);
    emit(o, ",\"wparam\":\"0x%08x\",\"lparam\":\"0x%08x\"}\n", (unsigned)e->wp,
         (unsigned)e->lp);
  }
  LeaveCriticalSection(&g_cs);
  return NULL;
}

// ---------- 推送通道（treewatch / gdiwatch 同构）----------
// 通道 = 变更信号 + pump。长连接线程：连上先 pump 全量，之后每次被唤醒（合并
// coalesce_ms 内连发）pump 增量；无变更 1s 心跳 {}（beat=1，pump 可顺带清理）；
// send 失败即对端已断，退出线程。
#define WATCH_CAP 8

static int send_all(SOCKET c, const char *s, size_t n);

// 写出 last 之后的变更，回本轮快照点；last=-1 即全量。
typedef LONG (*pump_fn)(struct out *o, LONG last, int beat);

struct feed {
  const char *name;
  int coalesce_ms;
  pump_fn pump;
  HANDLE evt[WATCH_CAP]; // 每个长连接一个 auto-reset 事件，护于 g_feed_cs
  volatile LONG pending; // 已唤醒未取走：热路径上不重复 SetEvent
};
static CRITICAL_SECTION g_feed_cs;
static LONG tree_pump(struct out *o, LONG last, int beat);
static LONG gdi_pump(struct out *o, LONG last, int beat);
static struct feed TREE_FEED = {"treewatch", 15, tree_pump};
static struct feed GDI_FEED = {"gdiwatch", 8, gdi_pump};

static void feed_signal(struct feed *f) {
  if (InterlockedExchange(&f->pending, 1))
    return;
  EnterCriticalSection(&g_feed_cs);
  for (int i = 0; i < WATCH_CAP; i++)
    if (f->evt[i])
      SetEvent(f->evt[i]);
  LeaveCriticalSection(&g_feed_cs);
}

// 回占的槽号；满了回 -1。
static int feed_join(struct feed *f, HANDLE ev) {
  int slot = -1;
  EnterCriticalSection(&g_feed_cs);
  for (int i = 0; i < WATCH_CAP && slot < 0; i++)
    if (!f->evt[i]) {
      f->evt[i] = ev;
      slot = i;
    }
  LeaveCriticalSection(&g_feed_cs);
  return slot;
}

static void feed_leave(struct feed *f, int slot) {
  EnterCriticalSection(&g_feed_cs);
  f->evt[slot] = NULL;
  LeaveCriticalSection(&g_feed_cs);
}

struct sub {
  struct feed *f;
  SOCKET c;
};

static DWORD WINAPI feed_thread(LPVOID arg) {
  struct sub s = *(struct sub *)arg;
  free(arg);
  HANDLE ev = CreateEventW(NULL, FALSE, FALSE, NULL);
  int slot = feed_join(s.f, ev);
  struct out o = {s.c, 1, NULL, 0, 0};
  if (slot < 0) {
    emit(&o, "err %s full\n", s.f->name);
    send_all(s.c, o.buf, o.len);
  } else {
    LONG last = -1;
    for (int beat = 0;;) {
      o.len = 0;
      InterlockedExchange(&s.f->pending, 0);
      last = s.f->pump(&o, last, beat);
      if (beat && !o.len)
        put(&o, "{}\n", 3);
      if (o.len && !send_all(s.c, o.buf, o.len))
        break;
      beat = WaitForSingleObject(ev, 1000) != WAIT_OBJECT_0;
      if (!beat)
        Sleep(s.f->coalesce_ms);
    }
    feed_leave(s.f, slot);
  }
  free(o.buf);
  CloseHandle(ev);
  closesocket(s.c);
  return 0;
}

// 长连接：接走 socket 另开线程推送。
static const char *feed_open(struct out *o, struct feed *f) {
  struct sub *s = (struct sub *)malloc(sizeof(*s));
  assert(s);
  s->f = f;
  s->c = o->sock;
  HANDLE t = CreateThread(NULL, 0, feed_thread, s, 0, NULL);
  assert(t);
  CloseHandle(t);
  o->detach = 1;
  return NULL;
}

// 快照：一次全量。
static const char *feed_dump(struct out *o, struct feed *f) {
  f->pump(o, -1, 0);
  return NULL;
}

// ---------- 窗口树变更事件（事件驱动自动刷新用）----------
// 装 WinEvent 钩子监听本进程窗口的 创建/销毁/显示/隐藏/重排/状态/位置/标题/值
// 变更，命中就给版本号 +1 并唤醒 treewatch。Wine 9 的 WM_SETTEXT 不发
// NAMECHANGE，标题变化靠前端轮询补。
static volatile LONG g_tree_gen = 0;

static void CALLBACK win_event_proc(HWINEVENTHOOK hook, DWORD ev, HWND hwnd,
                                    LONG idObj, LONG idChild, DWORD tid,
                                    DWORD tm) {
  (void)hook;
  (void)tid;
  (void)tm;
  int text_ev = ev == EVENT_OBJECT_NAMECHANGE || ev == EVENT_OBJECT_VALUECHANGE;
  if (!hwnd || idChild != CHILDID_SELF)
    return;
  if (idObj != OBJID_WINDOW && !(text_ev && idObj == OBJID_CLIENT))
    return; // 光标/插入符等非窗口对象的 LOCATIONCHANGE 很吵
  if (ev >= EVENT_OBJECT_FOCUS && ev <= EVENT_OBJECT_SELECTIONWITHIN)
    return; // 焦点/选区不改窗口树
  if (ev == EVENT_OBJECT_DESCRIPTIONCHANGE)
    return;
  InterlockedIncrement(&g_tree_gen);
  feed_signal(&TREE_FEED);
}

// OUTOFCONTEXT 回调要本线程有消息泵，故单开一个泵线程。
static DWORD WINAPI evt_thread(LPVOID arg) {
  (void)arg;
  SetWinEventHook(EVENT_OBJECT_CREATE, EVENT_OBJECT_VALUECHANGE, NULL,
                  win_event_proc, GetCurrentProcessId(), 0,
                  WINEVENT_OUTOFCONTEXT);
  MSG m;
  while (GetMessage(&m, NULL, 0, 0) > 0) {
    TranslateMessage(&m);
    DispatchMessage(&m);
  }
  return 0;
}

// 版本号变了才出 {gen}。
static LONG tree_pump(struct out *o, LONG last, int beat) {
  LONG gen = g_tree_gen;
  if (gen != last)
    emit(o, "{\"gen\":%ld}\n", (long)gen);
  return gen;
}

static const char *cmd_treegen(struct out *o, HWND h, char *a) {
  return feed_dump(o, &TREE_FEED);
}

static const char *cmd_treewatch(struct out *o, HWND h, char *a) {
  return feed_open(o, &TREE_FEED);
}

// ---------- GDI 绘制层 ----------
// 自绘控件（MHPToolBar 等）上的按钮只是画出来的字，窗口树里没有。钩 gdi32/
// user32 的绘制入口（改写函数入口，见"入口钩子"），按"后画盖先画"维护每个窗口
// 客户区上还看得见的文字与色块：
//   色块（PatBlt/FillRect/文字不透明底）盖掉中心落入的文字、整个落入的色块；
//   文字盖掉与之重叠的旧文字；贴图（BitBlt/StretchBlt…）把源 DC
//   图层对应区的内容 搬到目标区；滚动平移区内内容；BeginPaint 作废更新区。
// 内存 DC 按所选位图记图层（双缓冲：先画位图再贴到窗口），位图被删即丢。
// 只能钩 API 入口、不能钩 win32u：Wine 9 的 gdi32!ExtTextOutW 先 ScriptShape
// 成字形索引再下发 NtGdiExtTextOutW，那一层已没有原文。
#define LAYER_CAP 512
#define ITEM_CAP 8192        // 单图层上限，满了丢最旧一半
#define TEXT_CAP 256         // 单段文字 wchar 上限
#define ROP_MASK 0x00FFFFFFu // 去掉 CAPTUREBLT / NOMIRRORBITMAP 位
#define C_KEEP 0xFFFFFFFEu   // 不改图层（与目标混合的 rop、空画刷）
#define C_OPAQUE CLR_INVALID // 盖掉但颜色未知（图案画刷、位图）
#define PAINT_MS 200 // BeginPaint 后至多压这么久不推（防 EndPaint 不配对）

struct gitem {
  RECT r;
  COLORREF c;
  WCHAR *t; // NULL = 色块
};
struct layer {
  HANDLE key; // win ? HWND : HBITMAP
  int win;
  DWORD painting; // BeginPaint 时刻，0 = 不在重绘
  LONG seq;
  int n, cap;
  struct gitem *it;
};
static struct layer g_lay[LAYER_CAP];
static int g_nlay;
static int g_lay_hit;             // 上次命中的下标：同一 DC 连画是常态
static CRITICAL_SECTION g_gdi_cs; // 只护图层；持锁期间只做内存运算
static volatile LONG g_gdi_on, g_gdi_seq;
static DWORD g_tls;         // 本线程是否已在某个绘制钩子内
static HGDIOBJ g_stock_bmp; // 新建内存 DC 的默认 1x1 位图（各 DC 共用，不记）
static int g_gdi_patched, g_gdi_hooked, g_gdi_evicted;
static DWORD(WINAPI *o_GdiGetCodePage)(HDC);

static int center_in(const RECT *a, const RECT *r) {
  LONG x = (a->left + a->right) / 2, y = (a->top + a->bottom) / 2;
  return x >= r->left && x < r->right && y >= r->top && y < r->bottom;
}

static int inside(const RECT *a, const RECT *r) {
  return a->left >= r->left && a->right <= r->right && a->top >= r->top &&
         a->bottom <= r->bottom;
}

// 被 r 盖住：文字看中心，色块看是否整个落入。
static int covered(const struct gitem *e, const RECT *r) {
  return e->t ? center_in(&e->r, r) : inside(&e->r, r);
}

static WCHAR *wdup(const WCHAR *s, int n) {
  if (n > TEXT_CAP)
    n = TEXT_CAP;
  WCHAR *t = (WCHAR *)malloc((n + 1) * sizeof(WCHAR));
  assert(t);
  memcpy(t, s, n * sizeof(WCHAR));
  t[n] = 0;
  return t;
}

// 以下 lay_* 均须持 g_gdi_cs。
static void lay_free(struct layer *L) {
  for (int i = 0; i < L->n; i++)
    free(L->it[i].t);
  free(L->it);
  *L = g_lay[--g_nlay];
}

static int lay_is(int i, HANDLE key, int win) {
  return i < g_nlay && g_lay[i].key == key && g_lay[i].win == win;
}

static struct layer *lay_get(HANDLE key, int win, int make) {
  if (lay_is(g_lay_hit, key, win))
    return &g_lay[g_lay_hit];
  for (int i = 0; i < g_nlay; i++)
    if (lay_is(i, key, win))
      return &g_lay[g_lay_hit = i];
  if (!make)
    return NULL;
  if (g_nlay == LAYER_CAP) { // 挤掉最久没动的内存图层
    struct layer *v = NULL;
    for (int i = 0; i < g_nlay; i++)
      if (!g_lay[i].win && (!v || g_lay[i].seq < v->seq))
        v = &g_lay[i];
    if (!v)
      return NULL;
    lay_free(v);
    g_gdi_evicted++;
  }
  struct layer *L = &g_lay[g_nlay++];
  memset(L, 0, sizeof(*L));
  L->key = key;
  L->win = win;
  return L;
}

static void lay_touch(struct layer *L) {
  L->seq = InterlockedIncrement(&g_gdi_seq);
}

static void lay_erase(struct layer *L, const RECT *r) {
  int k = 0;
  for (int i = 0; i < L->n; i++)
    if (covered(&L->it[i], r))
      free(L->it[i].t);
    else
      L->it[k++] = L->it[i];
  L->n = k;
}

// t 归图层所有。
static void lay_push(struct layer *L, const RECT *r, COLORREF c, WCHAR *t) {
  if (L->n == ITEM_CAP) {
    int h = ITEM_CAP / 2;
    for (int i = 0; i < h; i++)
      free(L->it[i].t);
    memmove(L->it, L->it + h, (L->n - h) * sizeof(*L->it));
    L->n -= h;
  }
  if (L->n == L->cap) {
    L->cap = L->cap ? L->cap * 2 : 64;
    L->it = (struct gitem *)realloc(L->it, L->cap * sizeof(*L->it));
    assert(L->it);
  }
  struct gitem *e = &L->it[L->n++];
  e->r = *r;
  e->c = c;
  e->t = t;
}

static void lay_fill(struct layer *L, const RECT *r, COLORREF c) {
  lay_erase(L, r);
  if (c != C_OPAQUE)
    lay_push(L, r, c, NULL);
}

// 新文字盖掉与之重叠（任一方中心落入另一方）的旧文字。
static void lay_text(struct layer *L, const RECT *r, COLORREF c, WCHAR *t) {
  int k = 0;
  for (int i = 0; i < L->n; i++) {
    struct gitem *e = &L->it[i];
    if (e->t && (center_in(&e->r, r) || center_in(r, &e->r)))
      free(e->t);
    else
      L->it[k++] = *e;
  }
  L->n = k;
  lay_push(L, r, c, t);
}

// sc 内的内容平移 (dx,dy)；移到 cl 外的丢掉，色块裁到 cl。
static void lay_scroll(struct layer *L, int dx, int dy, const RECT *sc,
                       const RECT *cl) {
  int k = 0;
  for (int i = 0; i < L->n; i++) {
    struct gitem e = L->it[i];
    if (covered(&e, sc)) {
      OffsetRect(&e.r, dx, dy);
      if (e.t ? !center_in(&e.r, cl) : !IntersectRect(&e.r, &e.r, cl)) {
        free(e.t);
        continue;
      }
    }
    L->it[k++] = e;
  }
  L->n = k;
}

// 记录入口：开着且不在别的绘制钩子里（原函数内部会互调别的入口）。
static int gdi_enter(void) {
  if (!g_gdi_on || TlsGetValue(g_tls))
    return 0;
  TlsSetValue(g_tls, (void *)1);
  return 1;
}

static void gdi_leave(void) { TlsSetValue(g_tls, NULL); }

struct dci {
  HANDLE key;
  int win;
  POINT off; // 设备坐标 + off = 图层坐标
  RECT clip; // 图层坐标下可画区的外接框
};

// 逻辑矩形 → 图层坐标（规整为 l≤r、t≤b）。
static void dc_rect(HDC dc, const struct dci *d, RECT *r) {
  LPtoDP(dc, (POINT *)r, 2);
  if (r->left > r->right) {
    LONG t = r->left;
    r->left = r->right;
    r->right = t;
  }
  if (r->top > r->bottom) {
    LONG t = r->top;
    r->top = r->bottom;
    r->bottom = t;
  }
  OffsetRect(r, d->off.x, d->off.y);
}

// 窗口 DC → 该窗口客户区坐标；内存 DC → 所选位图像素坐标。其余 DC
// 或无可画区回 0。
static int dc_info(HDC dc, struct dci *d) {
  DWORD t = GetObjectType(dc);
  POINT org = {0, 0}, cl = {0, 0};
  if (t == OBJ_MEMDC) {
    d->key = GetCurrentObject(dc, OBJ_BITMAP);
    d->win = 0;
    if (!d->key || d->key == g_stock_bmp)
      return 0;
  } else if (t == OBJ_DC) {
    HWND h = WindowFromDC(dc);
    if (!h || h == GetDesktopWindow())
      return 0;
    d->key = h;
    d->win = 1;
  } else
    return 0;
  // 须先于 GetDCOrgEx：GetClipBox 会刷新脏 DC（窗口刚移动）的可见区原点，
  // GetDCOrgEx 只读现值。
  if (GetClipBox(dc, &d->clip) <= NULLREGION)
    return 0;
  if (d->win) {
    GetDCOrgEx(dc, &org); // 窗口 DC：DC 原点的屏幕坐标
    ClientToScreen((HWND)d->key, &cl);
  }
  d->off.x = org.x - cl.x;
  d->off.y = org.y - cl.y;
  dc_rect(dc, d, &d->clip);
  return 1;
}

static COLORREF brush_color(HBRUSH b) {
  LOGBRUSH lb;
  if (!b || !GetObjectW(b, sizeof(lb), &lb) || lb.lbStyle == BS_NULL)
    return C_KEEP;
  return lb.lbStyle == BS_SOLID ? lb.lbColor & 0xFFFFFF : C_OPAQUE;
}

// 不读源的 rop 的结果色：PATCOPY 取当前画刷，BLACKNESS/WHITENESS
// 定色，其余与目标混合。
static COLORREF rop_color(HDC dc, DWORD rop) {
  rop &= ROP_MASK;
  return rop == PATCOPY ? brush_color((HBRUSH)GetCurrentObject(dc, OBJ_BRUSH))
         : rop == BLACKNESS ? RGB(0, 0, 0)
         : rop == WHITENESS ? RGB(255, 255, 255)
                            : C_KEEP;
}

static void gdi_fill(HDC dc, RECT r, COLORREF c) {
  struct dci d;
  if (c == C_KEEP || !dc_info(dc, &d))
    return;
  dc_rect(dc, &d, &r);
  if (!IntersectRect(&r, &r, &d.clip))
    return;
  EnterCriticalSection(&g_gdi_cs);
  struct layer *L = lay_get(d.key, d.win, 1);
  if (L) {
    lay_fill(L, &r, c);
    lay_touch(L);
  }
  LeaveCriticalSection(&g_gdi_cs);
  if (d.win)
    feed_signal(&GDI_FEED);
}

static int has_ink(const WCHAR *s, int n) {
  for (int i = 0; i < n; i++)
    if (s[i] > L' ' && s[i] != 0x3000)
      return 1;
  return 0;
}

// 逻辑宽：有 dx 就累加前 ndx 个（ETO_PDY 时 dx 为 (x,y) 对），否则量字。
static int text_w(HDC dc, const WCHAR *s, int n, const INT *dx, int ndx,
                  UINT fl) {
  int w = 0;
  if (dx) {
    for (int i = 0; i < ndx; i++)
      w += dx[fl & ETO_PDY ? 2 * i : i];
    return w;
  }
  SIZE sz = {0, 0};
  GetTextExtentPoint32W(dc, s, n, &sz);
  return sz.cx;
}

// (x,y) 为参考点（逻辑坐标，按 TextAlign 解读），w 为逻辑宽；s=NULL
// 表示没有可记的文字（字形索引），只处理不透明底。
static void gdi_text(HDC dc, int x, int y, UINT fl, const RECT *rc,
                     const WCHAR *s, int n, int w) {
  int opaque = (fl & ETO_OPAQUE) && rc;
  int bkop = s && n > 0 && GetBkMode(dc) == OPAQUE; // 字框先刷背景色
  int ink = s && has_ink(s, n);
  struct dci d;
  if (!(opaque || bkop || ink) || !dc_info(dc, &d))
    return;
  RECT r = {0, 0, 0, 0}, o = {0, 0, 0, 0}, q;
  if (bkop || ink) {
    TEXTMETRICW tm;
    GetTextMetricsW(dc, &tm);
    UINT al = GetTextAlign(dc);
    if (al & TA_UPDATECP) {
      POINT p;
      GetCurrentPositionEx(dc, &p);
      x = p.x;
      y = p.y;
    }
    r.left = (al & TA_CENTER) == TA_CENTER ? x - w / 2
             : al & TA_RIGHT               ? x - w
                                           : x;
    r.top = (al & TA_BASELINE) == TA_BASELINE ? y - tm.tmAscent
            : al & TA_BOTTOM                  ? y - tm.tmHeight
                                              : y;
    r.right = r.left + w;
    r.bottom = r.top + tm.tmHeight;
    dc_rect(dc, &d, &r);
  }
  int clipped = (fl & ETO_CLIPPED) && rc;
  if (rc) {
    o = *rc;
    dc_rect(dc, &d, &o);
  }
  if (!center_in(&r, &d.clip) || (clipped && !center_in(&r, &o)))
    ink = 0;
  WCHAR *t = ink ? wdup(s, n) : NULL;
  COLORREF bk = GetBkColor(dc) & 0xFFFFFF, fg = GetTextColor(dc) & 0xFFFFFF;
  EnterCriticalSection(&g_gdi_cs);
  struct layer *L = lay_get(d.key, d.win, 1);
  if (L) {
    if (opaque && IntersectRect(&q, &o, &d.clip))
      lay_fill(L, &q, bk);
    if (bkop && IntersectRect(&q, &r, &d.clip) &&
        (!clipped || IntersectRect(&q, &q, &o)))
      lay_fill(L, &q, bk);
    if (t)
      lay_text(L, &r, fg, t);
    t = NULL;
    lay_touch(L);
  }
  LeaveCriticalSection(&g_gdi_cs);
  free(t);
  if (d.win)
    feed_signal(&GDI_FEED);
}

enum { BLT_OPAQUE, BLT_KEY, BLT_TEXT };

// src 的 sr 区贴到 dc 的 dr 区（均为各自逻辑坐标）。OPAQUE 先盖掉目标区；
// KEY 跳过色键色块（TransparentBlt）；TEXT 只搬文字（半透明 AlphaBlend）。
static void gdi_blit(HDC dc, RECT dr, HDC src, RECT sr, int mode,
                     COLORREF key) {
  struct dci d, s;
  RECT cr;
  if (!dc_info(dc, &d))
    return;
  dc_rect(dc, &d, &dr);
  if (!IntersectRect(&cr, &dr, &d.clip))
    return;
  int hs = src && dc_info(src, &s);
  if (hs)
    dc_rect(src, &s, &sr);
  int sw = sr.right - sr.left, sh = sr.bottom - sr.top;
  int dw = dr.right - dr.left, dh = dr.bottom - dr.top;
  EnterCriticalSection(&g_gdi_cs);
  struct layer *L = lay_get(d.key, d.win, 1);
  struct layer *S = hs ? lay_get(s.key, s.win, 0) : NULL;
  if (L) {
    struct gitem *tmp = NULL;
    int n = 0;
    if (S && S->n && sw > 0 && sh > 0) {
      tmp = (struct gitem *)malloc(S->n * sizeof(*tmp));
      assert(tmp);
      for (int i = 0; i < S->n; i++) {
        struct gitem e = S->it[i];
        if (e.t ? !center_in(&e.r, &sr) : !IntersectRect(&e.r, &e.r, &sr))
          continue;
        if (!e.t && (mode == BLT_TEXT || (mode == BLT_KEY && e.c == key)))
          continue;
        e.r.left = dr.left + MulDiv(e.r.left - sr.left, dw, sw);
        e.r.right = dr.left + MulDiv(e.r.right - sr.left, dw, sw);
        e.r.top = dr.top + MulDiv(e.r.top - sr.top, dh, sh);
        e.r.bottom = dr.top + MulDiv(e.r.bottom - sr.top, dh, sh);
        if (e.t ? !center_in(&e.r, &cr) : !IntersectRect(&e.r, &e.r, &cr))
          continue;
        if (e.t) // S 可能就是 L，下面的 erase 会释放原串
          e.t = wdup(e.t, (int)wcslen(e.t));
        tmp[n++] = e;
      }
    }
    if (mode == BLT_OPAQUE)
      lay_erase(L, &cr);
    for (int i = 0; i < n; i++)
      if (tmp[i].t)
        lay_text(L, &tmp[i].r, tmp[i].c, tmp[i].t);
      else
        lay_push(L, &tmp[i].r, tmp[i].c, NULL);
    free(tmp);
    lay_touch(L);
  }
  LeaveCriticalSection(&g_gdi_cs);
  if (d.win)
    feed_signal(&GDI_FEED);
}

static void gdi_rop(HDC dc, RECT dr, HDC src, RECT sr, DWORD rop) {
  if ((rop & ROP_MASK) == SRCCOPY && src)
    gdi_blit(dc, dr, src, sr, BLT_OPAQUE, 0);
  else
    gdi_fill(dc, dr, rop_color(dc, rop));
}

static void gdi_scroll(HANDLE key, int win, int dx, int dy, const RECT *sc,
                       const RECT *cl) {
  EnterCriticalSection(&g_gdi_cs);
  struct layer *L = lay_get(key, win, 0);
  if (L) {
    lay_scroll(L, dx, dy, sc, cl);
    lay_touch(L);
  }
  LeaveCriticalSection(&g_gdi_cs);
  if (L && win)
    feed_signal(&GDI_FEED);
}

// 客户区坐标；sc/cl 缺省 = 整个客户区。
static void gdi_scroll_wnd(HWND h, int dx, int dy, const RECT *sc,
                           const RECT *cl) {
  RECT c, s, k;
  GetClientRect(h, &c);
  s = sc ? *sc : c;
  k = cl ? *cl : c;
  if (IntersectRect(&k, &k, &c))
    gdi_scroll(h, 1, dx, dy, &s, &k);
}

// BeginPaint：更新区里的旧内容作废（应用接着重画），推送压到 EndPaint。
static void gdi_paint(HWND h, int begin) {
  RGNDATA *rd = NULL;
  if (begin) {
    HRGN rg = CreateRectRgn(0, 0, 0, 0);
    if (GetUpdateRgn(h, rg, FALSE) > NULLREGION) {
      DWORD sz = GetRegionData(rg, 0, NULL);
      rd = (RGNDATA *)malloc(sz);
      assert(rd);
      GetRegionData(rg, sz, rd);
    }
    DeleteObject(rg);
  }
  EnterCriticalSection(&g_gdi_cs);
  struct layer *L = lay_get(h, 1, 1);
  if (L) {
    for (DWORD i = 0; rd && i < rd->rdh.nCount; i++)
      lay_erase(L, (RECT *)rd->Buffer + i);
    L->painting = begin ? GetTickCount() | 1 : 0;
    lay_touch(L);
  }
  LeaveCriticalSection(&g_gdi_cs);
  free(rd);
  if (!begin)
    feed_signal(&GDI_FEED);
}

// ----- 钩子：先记（要读绘制前的当前位置等），再调原函数 -----
// 绘制类钩子连原函数一起占住嵌套标记：gdi32 内部直接调别的入口（TextOutA →
// ExtTextOutA → ExtTextOutW、GdiTransparentBlt 内的 Blt
// 等）也会进钩子，只记最外层。
static BOOL(WINAPI *o_ExtTextOutW)(HDC, int, int, UINT, const RECT *, LPCWSTR,
                                   UINT, const INT *);
static BOOL(WINAPI *o_ExtTextOutA)(HDC, int, int, UINT, const RECT *, LPCSTR,
                                   UINT, const INT *);
static BOOL(WINAPI *o_TextOutW)(HDC, int, int, LPCWSTR, int);
static BOOL(WINAPI *o_TextOutA)(HDC, int, int, LPCSTR, int);
static BOOL(WINAPI *o_PatBlt)(HDC, int, int, int, int, DWORD);
static BOOL(WINAPI *o_BitBlt)(HDC, int, int, int, int, HDC, int, int, DWORD);
static BOOL(WINAPI *o_StretchBlt)(HDC, int, int, int, int, HDC, int, int, int,
                                  int, DWORD);
static BOOL(WINAPI *o_GdiTransparentBlt)(HDC, int, int, int, int, HDC, int, int,
                                         int, int, UINT);
static BOOL(WINAPI *o_GdiAlphaBlend)(HDC, int, int, int, int, HDC, int, int,
                                     int, int, BLENDFUNCTION);
static int(WINAPI *o_StretchDIBits)(HDC, int, int, int, int, int, int, int, int,
                                    const void *, const BITMAPINFO *, UINT,
                                    DWORD);
static int(WINAPI *o_SetDIBitsToDevice)(HDC, int, int, DWORD, DWORD, int, int,
                                        UINT, UINT, const void *,
                                        const BITMAPINFO *, UINT);
static BOOL(WINAPI *o_DeleteObject)(HGDIOBJ);
static int(WINAPI *o_FillRect)(HDC, const RECT *, HBRUSH);
static HDC(WINAPI *o_BeginPaint)(HWND, PAINTSTRUCT *);
static BOOL(WINAPI *o_EndPaint)(HWND, const PAINTSTRUCT *);
static BOOL(WINAPI *o_ScrollWindow)(HWND, int, int, const RECT *, const RECT *);
static int(WINAPI *o_ScrollWindowEx)(HWND, int, int, const RECT *, const RECT *,
                                     HRGN, LPRECT, UINT);
static BOOL(WINAPI *o_ScrollDC)(HDC, int, int, const RECT *, const RECT *, HRGN,
                                LPRECT);

static BOOL WINAPI my_ExtTextOutW(HDC dc, int x, int y, UINT fl, const RECT *rc,
                                  LPCWSTR s, UINT n, const INT *dx) {
  if (!gdi_enter())
    return o_ExtTextOutW(dc, x, y, fl, rc, s, n, dx);
  int g = (fl & ETO_GLYPH_INDEX) || n > INT_MAX;
  gdi_text(dc, x, y, fl, rc, g ? NULL : s, g ? 0 : (int)n,
           g ? 0 : text_w(dc, s, n, dx, n, fl));
  BOOL ok = o_ExtTextOutW(dc, x, y, fl, rc, s, n, dx);
  gdi_leave();
  return ok;
}

// A 版：与 gdi32 内部同样按 GdiGetCodePage 转 wchar；dx 按字节给。
static int a2w(HDC dc, LPCSTR s, int n, WCHAR *w, int cap) {
  if (n > cap)
    n = cap;
  return n > 0 ? MultiByteToWideChar(o_GdiGetCodePage(dc), 0, s, n, w, cap) : 0;
}

static BOOL WINAPI my_ExtTextOutA(HDC dc, int x, int y, UINT fl, const RECT *rc,
                                  LPCSTR s, UINT n, const INT *dx) {
  if (!gdi_enter())
    return o_ExtTextOutA(dc, x, y, fl, rc, s, n, dx);
  WCHAR w[2 * TEXT_CAP];
  int wn = (fl & ETO_GLYPH_INDEX) || n > INT_MAX
               ? 0
               : a2w(dc, s, (int)n, w, 2 * TEXT_CAP);
  gdi_text(dc, x, y, fl, rc, wn ? w : NULL, wn,
           wn ? text_w(dc, w, wn, dx, (int)n, fl) : 0);
  BOOL ok = o_ExtTextOutA(dc, x, y, fl, rc, s, n, dx);
  gdi_leave();
  return ok;
}

static BOOL WINAPI my_TextOutW(HDC dc, int x, int y, LPCWSTR s, int n) {
  if (!gdi_enter())
    return o_TextOutW(dc, x, y, s, n);
  gdi_text(dc, x, y, 0, NULL, s, n, text_w(dc, s, n, NULL, 0, 0));
  BOOL ok = o_TextOutW(dc, x, y, s, n);
  gdi_leave();
  return ok;
}

static BOOL WINAPI my_TextOutA(HDC dc, int x, int y, LPCSTR s, int n) {
  if (!gdi_enter())
    return o_TextOutA(dc, x, y, s, n);
  WCHAR w[2 * TEXT_CAP];
  int wn = a2w(dc, s, n, w, 2 * TEXT_CAP);
  gdi_text(dc, x, y, 0, NULL, wn ? w : NULL, wn, text_w(dc, w, wn, NULL, 0, 0));
  BOOL ok = o_TextOutA(dc, x, y, s, n);
  gdi_leave();
  return ok;
}

static BOOL WINAPI my_PatBlt(HDC dc, int x, int y, int w, int h, DWORD rop) {
  if (!gdi_enter())
    return o_PatBlt(dc, x, y, w, h, rop);
  RECT r = {x, y, x + w, y + h};
  gdi_fill(dc, r, rop_color(dc, rop));
  BOOL ok = o_PatBlt(dc, x, y, w, h, rop);
  gdi_leave();
  return ok;
}

static BOOL WINAPI my_BitBlt(HDC dc, int x, int y, int w, int h, HDC src,
                             int xs, int ys, DWORD rop) {
  if (!gdi_enter())
    return o_BitBlt(dc, x, y, w, h, src, xs, ys, rop);
  RECT dr = {x, y, x + w, y + h}, sr = {xs, ys, xs + w, ys + h};
  gdi_rop(dc, dr, src, sr, rop);
  BOOL ok = o_BitBlt(dc, x, y, w, h, src, xs, ys, rop);
  gdi_leave();
  return ok;
}

static BOOL WINAPI my_StretchBlt(HDC dc, int x, int y, int w, int h, HDC src,
                                 int xs, int ys, int ws, int hs, DWORD rop) {
  if (!gdi_enter())
    return o_StretchBlt(dc, x, y, w, h, src, xs, ys, ws, hs, rop);
  RECT dr = {x, y, x + w, y + h}, sr = {xs, ys, xs + ws, ys + hs};
  gdi_rop(dc, dr, src, sr, rop);
  BOOL ok = o_StretchBlt(dc, x, y, w, h, src, xs, ys, ws, hs, rop);
  gdi_leave();
  return ok;
}

static BOOL WINAPI my_GdiTransparentBlt(HDC dc, int x, int y, int w, int h,
                                        HDC src, int xs, int ys, int ws, int hs,
                                        UINT key) {
  if (!gdi_enter())
    return o_GdiTransparentBlt(dc, x, y, w, h, src, xs, ys, ws, hs, key);
  RECT dr = {x, y, x + w, y + h}, sr = {xs, ys, xs + ws, ys + hs};
  gdi_blit(dc, dr, src, sr, BLT_KEY, key & 0xFFFFFF);
  BOOL ok = o_GdiTransparentBlt(dc, x, y, w, h, src, xs, ys, ws, hs, key);
  gdi_leave();
  return ok;
}

static BOOL WINAPI my_GdiAlphaBlend(HDC dc, int x, int y, int w, int h, HDC src,
                                    int xs, int ys, int ws, int hs,
                                    BLENDFUNCTION bf) {
  if (!gdi_enter())
    return o_GdiAlphaBlend(dc, x, y, w, h, src, xs, ys, ws, hs, bf);
  RECT dr = {x, y, x + w, y + h}, sr = {xs, ys, xs + ws, ys + hs};
  int solid = bf.SourceConstantAlpha == 255 && !(bf.AlphaFormat & AC_SRC_ALPHA);
  gdi_blit(dc, dr, src, sr, solid ? BLT_OPAQUE : BLT_TEXT, 0);
  BOOL ok = o_GdiAlphaBlend(dc, x, y, w, h, src, xs, ys, ws, hs, bf);
  gdi_leave();
  return ok;
}

static int WINAPI my_StretchDIBits(HDC dc, int x, int y, int w, int h, int xs,
                                   int ys, int ws, int hs, const void *bits,
                                   const BITMAPINFO *bi, UINT use, DWORD rop) {
  if (!gdi_enter())
    return o_StretchDIBits(dc, x, y, w, h, xs, ys, ws, hs, bits, bi, use, rop);
  RECT r = {x, y, x + w, y + h};
  gdi_fill(dc, r, (rop & ROP_MASK) == SRCCOPY ? C_OPAQUE : C_KEEP);
  int ret = o_StretchDIBits(dc, x, y, w, h, xs, ys, ws, hs, bits, bi, use, rop);
  gdi_leave();
  return ret;
}

static int WINAPI my_SetDIBitsToDevice(HDC dc, int x, int y, DWORD w, DWORD h,
                                       int xs, int ys, UINT start, UINT lines,
                                       const void *bits, const BITMAPINFO *bi,
                                       UINT use) {
  if (!gdi_enter())
    return o_SetDIBitsToDevice(dc, x, y, w, h, xs, ys, start, lines, bits, bi,
                               use);
  RECT r = {x, y, x + (int)w, y + (int)h};
  gdi_fill(dc, r, C_OPAQUE);
  int ret =
      o_SetDIBitsToDevice(dc, x, y, w, h, xs, ys, start, lines, bits, bi, use);
  gdi_leave();
  return ret;
}

// 删位图即丢其图层（选在 DC 里的位图删不掉，故看返回值）。
static BOOL WINAPI my_DeleteObject(HGDIOBJ h) {
  int bmp = g_gdi_on && GetObjectType(h) == OBJ_BITMAP;
  BOOL ok = o_DeleteObject(h);
  if (bmp && ok) {
    EnterCriticalSection(&g_gdi_cs);
    struct layer *L = lay_get(h, 0, 0);
    if (L)
      lay_free(L);
    LeaveCriticalSection(&g_gdi_cs);
  }
  return ok;
}

// 原函数内部调 PatBlt。
static int WINAPI my_FillRect(HDC dc, const RECT *r, HBRUSH b) {
  if (!r || !gdi_enter())
    return o_FillRect(dc, r, b);
  HBRUSH c = (UINT_PTR)b <= COLOR_MENUBAR + 1 // 系统色序号 + 1
                 ? GetSysColorBrush((int)(UINT_PTR)b - 1)
                 : b;
  gdi_fill(dc, *r, brush_color(c));
  int ret = o_FillRect(dc, r, b);
  gdi_leave();
  return ret;
}

// 不占嵌套标记：BeginPaint 内同步发的 WM_ERASEBKGND 的擦底要照常记。
static HDC WINAPI my_BeginPaint(HWND h, PAINTSTRUCT *ps) {
  if (g_gdi_on)
    gdi_paint(h, 1);
  return o_BeginPaint(h, ps);
}

static BOOL WINAPI my_EndPaint(HWND h, const PAINTSTRUCT *ps) {
  BOOL ok = o_EndPaint(h, ps);
  if (g_gdi_on)
    gdi_paint(h, 0);
  return ok;
}

static BOOL WINAPI my_ScrollWindow(HWND h, int dx, int dy, const RECT *sc,
                                   const RECT *cl) {
  if (gdi_enter()) {
    gdi_scroll_wnd(h, dx, dy, sc, cl);
    gdi_leave();
  }
  return o_ScrollWindow(h, dx, dy, sc, cl);
}

static int WINAPI my_ScrollWindowEx(HWND h, int dx, int dy, const RECT *sc,
                                    const RECT *cl, HRGN rgn, LPRECT upd,
                                    UINT fl) {
  if (gdi_enter()) {
    gdi_scroll_wnd(h, dx, dy, sc, cl);
    gdi_leave();
  }
  return o_ScrollWindowEx(h, dx, dy, sc, cl, rgn, upd, fl);
}

// dx/dy 按 MM_TEXT 取（逻辑单位 = 设备像素）。
static BOOL WINAPI my_ScrollDC(HDC dc, int dx, int dy, const RECT *sc,
                               const RECT *cl, HRGN rgn, LPRECT upd) {
  struct dci d;
  if (gdi_enter()) {
    if (dc_info(dc, &d)) {
      RECT s = d.clip, k = d.clip;
      if (sc) {
        s = *sc;
        dc_rect(dc, &d, &s);
      }
      if (cl) {
        k = *cl;
        dc_rect(dc, &d, &k);
      }
      if (IntersectRect(&k, &k, &d.clip))
        gdi_scroll(d.key, d.win, dx, dy, &s, &k);
    }
    gdi_leave();
  }
  return o_ScrollDC(dc, dx, dy, sc, cl, rgn, upd);
}

#define HOOK(dll, fn) {dll ".dll", #fn, (void **)&o_##fn, (void *)my_##fn}
// clang-format off
static const struct {
  const char *dll, *fn;
  void **orig;
  void *my;
} GDI_HOOKS[] = {
    HOOK("gdi32",  ExtTextOutW),
    HOOK("gdi32",  ExtTextOutA),
    HOOK("gdi32",  TextOutW),
    HOOK("gdi32",  TextOutA),
    HOOK("gdi32",  PatBlt),
    HOOK("gdi32",  BitBlt),
    HOOK("gdi32",  StretchBlt),
    HOOK("gdi32",  StretchDIBits),
    HOOK("gdi32",  SetDIBitsToDevice),
    HOOK("gdi32",  DeleteObject),
    HOOK("gdi32",  GdiTransparentBlt), // msimg32 的 TransparentBlt 转发到此
    HOOK("gdi32",  GdiAlphaBlend),     // msimg32 的 AlphaBlend 转发到此
    HOOK("user32", FillRect),
    HOOK("user32", BeginPaint),
    HOOK("user32", EndPaint),
    HOOK("user32", ScrollWindow),
    HOOK("user32", ScrollWindowEx),
    HOOK("user32", ScrollDC),
    {NULL, NULL, NULL, NULL},
};
// clang-format on
#undef HOOK

// ----- 入口钩子 -----
// TdxW.exe 是 VMProtect 壳，经自己的桩调 API、不读 IAT，故直接改写函数入口：
// 头几条指令搬进跳板（末尾 jmp 回原处），入口换成 jmp 到包装。入口原字节存进
// 跨进程命名共享表：重编后的新 DLL 见到的入口已是旧版的 jmp，从表里取原样。
#define HK_MAX 64
#define HK_TRIES 200

struct hookrec {
  DWORD addr;
  int len;
  BYTE clean[16];
};
struct hooktab {
  volatile LONG lock, n;
  struct hookrec r[HK_MAX];
};
static struct hooktab *g_htab;

// 单条指令长度；认不出回 0。只认入口序言常见、且不含相对寻址的形态（相对
// 跳转/调用搬进跳板会指错）。
static int insn_len(const BYTE *p) {
  BYTE op = p[0];
  if (op >= 0x50 && op <= 0x5f) // push/pop r32
    return 1;
  if (op == 0x6a)
    return 2;
  if (op == 0x68 || (op >= 0xb8 && op <= 0xbf)) // push imm32 / mov r32,imm32
    return 5;
  int imm = 0;
  switch (op) {
  case 0x01:
  case 0x03:
  case 0x09:
  case 0x0b:
  case 0x21:
  case 0x23:
  case 0x29:
  case 0x2b:
  case 0x31:
  case 0x33:
  case 0x39:
  case 0x3b:
  case 0x85:
  case 0x89:
  case 0x8b:
  case 0x8d:
    break;
  case 0x83:
    imm = 1;
    break;
  case 0x81:
    imm = 4;
    break;
  case 0xff: // 只认 jmp [abs32]（user32 转 win32u 的桩）
    return p[1] == 0x25 ? 6 : 0;
  default:
    return 0;
  }
  int mod = p[1] >> 6, rm = p[1] & 7, n = 2;
  if (mod != 3 && rm == 4)
    n++; // SIB
  if (mod == 1)
    n += 1;
  else if (mod == 2 || (mod == 0 && rm == 5))
    n += 4;
  return n + imm;
}

// 凑够 5 字节（jmp rel32）的整条指令长度；认不出回 0。
static int prologue_len(const BYTE *p) {
  int n = 0;
  while (n < 5) {
    int l = insn_len(p + n);
    if (!l || n + l > (int)sizeof(((struct hookrec *)0)->clean))
      return 0;
    n += l;
  }
  return n;
}

static void htab_open(void) {
  HANDLE fm = CreateFileMappingA(INVALID_HANDLE_VALUE, NULL, PAGE_READWRITE, 0,
                                 sizeof(struct hooktab), "tdx_shim_hooktab_v1");
  assert(fm); // 句柄不关：本 DLL 不卸载，共享表随进程存活
  g_htab = MapViewOfFile(fm, FILE_MAP_WRITE, 0, 0, sizeof(struct hooktab));
  assert(g_htab);
}

// addr 入口的原字节：表里有就用表里的；没有说明是全进程第一次钩它，按现字节
// 解析并存表。认不出回 NULL。
static const BYTE *hk_clean(void *addr, int *len) {
  while (InterlockedCompareExchange(&g_htab->lock, 1, 0))
    Sleep(0);
  const BYTE *out = NULL;
  for (int i = 0; i < g_htab->n && !out; i++)
    if (g_htab->r[i].addr == (DWORD)addr) {
      *len = g_htab->r[i].len;
      out = g_htab->r[i].clean;
    }
  int l = out ? 0 : prologue_len(addr);
  if (l) {
    assert(g_htab->n < HK_MAX);
    struct hookrec *r = &g_htab->r[g_htab->n];
    r->addr = (DWORD)addr;
    r->len = l;
    memcpy(r->clean, addr, l);
    InterlockedIncrement(&g_htab->n);
    *len = l;
    out = r->clean;
  }
  InterlockedExchange(&g_htab->lock, 0);
  return out;
}

#define TID_CAP 512
struct tids {
  int n;
  DWORD id[TID_CAP];
};

static int tid_add(DWORD tid, void *ctx) {
  struct tids *t = (struct tids *)ctx;
  assert(t->n < TID_CAP);
  t->id[t->n++] = tid;
  return 1;
}

// 挂起本进程其余线程，确认无线程停在 [dst,dst+n) 内再写，写完恢复。挂起期间
// 只走 wineserver 调用和内存拷贝：不碰堆、映射（Thread32Next 会映射快照）、
// 页保护，免得撞上被挂线程持有的锁；故先收齐线程号再挂起。
static int patch_code(BYTE *dst, const BYTE *src, int n) {
  DWORD old;
  BOOL ok = VirtualProtect(dst, n, PAGE_EXECUTE_READWRITE, &old);
  assert(ok);
  int done = 0;
  for (int tries = 0; tries < HK_TRIES && !done; tries++) {
    struct tids ts;
    HANDLE th[TID_CAP];
    int nt = 0;
    ts.n = 0;
    each_thread(tid_add, &ts);
    for (int i = 0; i < ts.n; i++) {
      HANDLE t = OpenThread(THREAD_SUSPEND_RESUME | THREAD_GET_CONTEXT, FALSE,
                            ts.id[i]);
      if (t && SuspendThread(t) != (DWORD)-1)
        th[nt++] = t;
      else if (t)
        CloseHandle(t); // 已退出
    }
    int busy = 0;
    for (int i = 0; i < nt && !busy; i++) {
      CONTEXT c;
      c.ContextFlags = CONTEXT_CONTROL;
      ok = GetThreadContext(th[i], &c);
      assert(ok);
      busy = c.Eip >= (DWORD)dst && c.Eip < (DWORD)dst + n;
    }
    if (!busy) {
      memcpy(dst, src, n);
      done = 1;
    }
    for (int i = 0; i < nt; i++) {
      ResumeThread(th[i]);
      CloseHandle(th[i]);
    }
    if (!done)
      Sleep(1);
  }
  VirtualProtect(dst, n, old, &old);
  FlushInstructionCache(GetCurrentProcess(), dst, n);
  return done;
}

// target 入口改成 jmp hook，*orig 置为调原函数的跳板。认不出入口或一直有线程
// 停在入口上回 0（不钩）。
static int inline_hook(BYTE *target, void *hook, void **orig) {
  int len = 0;
  const BYTE *clean = hk_clean(target, &len);
  if (!clean)
    return 0;
  BYTE *tr = VirtualAlloc(NULL, len + 5, MEM_COMMIT | MEM_RESERVE,
                          PAGE_EXECUTE_READWRITE);
  assert(tr);
  memcpy(tr, clean, len);
  tr[len] = 0xE9;
  *(DWORD *)(tr + len + 1) = (DWORD)(target + len) - (DWORD)(tr + len + 5);
  FlushInstructionCache(GetCurrentProcess(), tr, len + 5);
  BYTE jmp[sizeof(((struct hookrec *)0)->clean)];
  jmp[0] = 0xE9;
  *(DWORD *)(jmp + 1) = (DWORD)hook - (DWORD)(target + 5);
  memset(jmp + 5, 0x90, len - 5);
  *orig = tr; // 先于入口改写：钩子一生效就可能被调
  if (patch_code(target, jmp, len))
    return 1;
  *orig = NULL;
  VirtualFree(tr, 0, MEM_RELEASE);
  return 0;
}

static int mem_ok(const void *p, size_t n, int exec) {
  MEMORY_BASIC_INFORMATION q;
  if (!p || !VirtualQuery(p, &q, sizeof(q)) || q.State != MEM_COMMIT ||
      q.Protect & (PAGE_GUARD | PAGE_NOACCESS))
    return 0;
  DWORD_PTR end = (DWORD_PTR)p + n;
  if (end < (DWORD_PTR)p || end > (DWORD_PTR)q.BaseAddress + q.RegionSize)
    return 0;
  if (!exec)
    return 1;
  DWORD x = q.Protect & 0xff;
  return x == PAGE_EXECUTE || x == PAGE_EXECUTE_READ ||
         x == PAGE_EXECUTE_READWRITE || x == PAGE_EXECUTE_WRITECOPY;
}

static int req_host_candidate(void *p) {
  if (!mem_ok(p, sizeof(void *), 0))
    return 0;
  void **vt = *(void ***)p;
  if (!mem_ok(vt, 0x274, 0))
    return 0;
  const int slots[] = {0, 1, 2, 0x98, 0x99, 0x9a, 0x9b};
  for (size_t i = 0; i < sizeof(slots) / sizeof(*slots); i++)
    if (!mem_ok(vt[slots[i]], 1, 1))
      return 0;
  return 1;
}

typedef void *(__stdcall *mfc_from_hwnd_fn)(HWND);
typedef void *(TCALL *host_session_fn)(void *);
typedef void *(TCALL *session_req_fn)(void *, DWORD, DWORD);
typedef DWORD(TCALL *release_fn)(void *);

static void req_install(void *req) {
  void **vt = *(void ***)req;
  const int slot[] = {0x14 / 4, 0x18 / 4, 0x1c / 4,
                      0x24 / 4, 0x2c / 4, 0x64 / 4};
  void *hook[] = {(void *)my_req_init,  (void *)my_req_func,
                  (void *)my_req_str,   (void *)my_req_int,
                  (void *)my_req_float, (void *)my_req_send};
  void **orig[] = {(void **)&o_req_init,  (void **)&o_req_func,
                   (void **)&o_req_str,   (void **)&o_req_int,
                   (void **)&o_req_float, (void **)&o_req_send};
  if (g_req_hooked) {
    for (int i = 0; i < 6; i++)
      assert(g_req_target[i] == (DWORD)vt[slot[i]]);
    return;
  }
  if (!g_htab)
    htab_open();
  for (int i = 0; i < 6; i++) {
    assert(mem_ok(vt[slot[i]], 1, 1));
    g_req_target[i] = (DWORD)vt[slot[i]];
    assert(inline_hook((BYTE *)vt[slot[i]], hook[i], orig[i]));
  }
  g_req_hooked = 1;
}

static const char *req_attach(struct out *o, char *a) {
  char *rest;
  HWND h = split_target(a, &rest);
  if (!h)
    return "usage: req attach <hwnd> [object-offset]";
  h = GetAncestor(h, GA_ROOT);
  HMODULE mfc = GetModuleHandleA("MFC42.DLL");
  assert(mfc);
  mfc_from_hwnd_fn from = (mfc_from_hwnd_fn)GetProcAddress(
      mfc, "?FromHandlePermanent@CWnd@@SGPAV1@PAUHWND__@@@Z");
  assert(from);
  BYTE *wnd = (BYTE *)from(h);
  assert(wnd && mem_ok(wnd, 0x204, 0));

  int chosen = -1, found = 0;
  if (*rest) {
    chosen = (int)strtoul(rest, NULL, 0);
    assert(chosen >= 0x20 && chosen <= 0x200 && !(chosen & 3));
    assert(req_host_candidate(*(void **)(wnd + chosen)));
    found = 1;
  } else {
    for (int off = 0x20; off <= 0x200; off += 4) {
      void *p = *(void **)(wnd + off);
      if (!req_host_candidate(p))
        continue;
      emit(o, "{\"candidate\":\"0x%08x\",\"offset\":%d}\n", (DWORD)p, off);
      chosen = off;
      found++;
    }
    if (!found)
      return "host object not found";
    if (found != 1) {
      emit(o, "{\"need_offset\":1,\"found\":%d}\n", found);
      return NULL;
    }
  }

  void *host = *(void **)(wnd + chosen);
  void **hvt = *(void ***)host;
  host_session_fn get_session = (host_session_fn)hvt[0x26c / 4];
  void *session = get_session(host);
  assert(session && mem_ok(session, sizeof(void *), 0));
  void **svt = *(void ***)session;
  assert(mem_ok(svt, 0x20, 0) && mem_ok(svt[0x1c / 4], 1, 1));
  session_req_fn make_req = (session_req_fn)svt[0x1c / 4];
  void *req = make_req(session, 0, 0);
  assert(req && mem_ok(req, sizeof(void *), 0));
  req_install(req);

  void **rvt = *(void ***)req;
  ((release_fn)rvt[0x4 / 4])(req);
  ((release_fn)svt[0x8 / 4])(session);
  g_req_on = 1;
  emit(o,
       "{\"attached\":\"0x%08x\",\"wnd\":\"0x%08x\",\"host\":\"0x%08x\","
       "\"offset\":%d,\"request_vtable\":\"0x%08x\"}\n",
       HX(h), (DWORD)wnd, (DWORD)host, chosen, (DWORD)rvt);
  return NULL;
}

static const char *const REQ_NAMES[] = {
    "init", "func", "str", "int", "float", "send",
};

static void req_emit(struct out *o, const struct req_evt *e) {
  emit(o, "{\"tick\":%lu,\"tid\":%lu,\"obj\":\"0x%08x\",\"op\":\"%s\"", e->tick,
       e->tid, e->self, REQ_NAMES[e->op]);
  if (e->op == REQ_FUNC)
    emit(o, ",\"value\":%lu", e->a);
  else if (e->op == REQ_STR) {
    wchar_t w[512] = {0};
    char u[512 * 3 + 1] = {0};
    int n = MultiByteToWideChar(CP_ACP, 0, e->s, -1, w, 512);
    assert(n > 0);
    w2u(w, u, sizeof(u));
    emit(o, ",\"id\":%d,\"aux\":%lu,\"value\":", e->id, e->a);
    emit_str(o, u);
  } else if (e->op == REQ_INT)
    emit(o, ",\"id\":%d,\"value\":%lu", e->id, e->a);
  else if (e->op == REQ_FLOAT)
    emit(o, ",\"id\":%d,\"value\":%.9g", e->id, (double)e->f);
  else if (e->op == REQ_INIT)
    emit(o, ",\"value\":%lu", e->a);
  else if (e->op == REQ_SEND)
    emit(o, ",\"args\":[%lu,%lu,%lu,%lu]", e->a, e->b, e->c, e->d);
  emit(o, "}\n");
}

static const char *cmd_req(struct out *o, HWND h, char *a) {
  (void)h;
  char *rest = a + strcspn(a, " ");
  if (*rest)
    *rest++ = 0;
  if (!strcmp(a, "attach"))
    return req_attach(o, rest);
  if (!strcmp(a, "on"))
    g_req_on = 1;
  else if (!strcmp(a, "off"))
    g_req_on = 0;
  else if (!strcmp(a, "clear")) {
    EnterCriticalSection(&g_req_cs);
    g_req_head = g_req_count = 0;
    LeaveCriticalSection(&g_req_cs);
  } else if (!strcmp(a, "drain")) {
    EnterCriticalSection(&g_req_cs);
    int start = (g_req_head - g_req_count + REQ_CAP) % REQ_CAP;
    for (int i = 0; i < g_req_count; i++)
      req_emit(o, &g_req[(start + i) % REQ_CAP]);
    g_req_head = g_req_count = 0;
    LeaveCriticalSection(&g_req_cs);
  } else
    return "usage: req on|off|clear|drain|attach <hwnd> [object-offset]";
  if (strcmp(a, "drain"))
    emit(o, "{\"on\":%ld,\"hooked\":%d,\"count\":%d}\n", g_req_on, g_req_hooked,
         g_req_count);
  return NULL;
}

static int redraw(HWND h, void *ctx) {
  return RedrawWindow(h, NULL, NULL,
                      RDW_INVALIDATE | RDW_ERASE | RDW_FRAME | RDW_ALLCHILDREN);
}

// on：首次装入口钩子（全进程生效，之后不卸，off 时钩子直通）、开记录、重绘全部
// 窗口以填满图层；装不上的入口各回一行 {"skip":名}。off：停记并清空。
static const char *cmd_gdi(struct out *o, HWND h, char *a) {
  if (!strcmp(a, "on")) {
    if (!g_stock_bmp) {
      HDC m = CreateCompatibleDC(NULL);
      g_stock_bmp = GetCurrentObject(m, OBJ_BITMAP);
      DeleteDC(m);
      o_GdiGetCodePage = (DWORD(WINAPI *)(HDC))GetProcAddress(
          GetModuleHandleA("gdi32.dll"), "GdiGetCodePage");
      assert(g_stock_bmp && o_GdiGetCodePage);
    }
    if (!g_gdi_hooked) {
      g_gdi_hooked = 1;
      if (!g_htab)
        htab_open();
      for (int k = 0; GDI_HOOKS[k].dll; k++) {
        HMODULE m = GetModuleHandleA(GDI_HOOKS[k].dll);
        assert(m);
        BYTE *t = (BYTE *)GetProcAddress(m, GDI_HOOKS[k].fn);
        assert(t);
        if (inline_hook(t, GDI_HOOKS[k].my, GDI_HOOKS[k].orig))
          g_gdi_patched++;
        else
          emit(o, "{\"skip\":\"%s\"}\n", GDI_HOOKS[k].fn);
      }
    }
    g_gdi_on = 1;
    each_window(redraw, NULL);
  } else if (!strcmp(a, "off")) {
    g_gdi_on = 0;
    EnterCriticalSection(&g_gdi_cs);
    while (g_nlay)
      lay_free(&g_lay[0]);
    LeaveCriticalSection(&g_gdi_cs);
  } else if (*a)
    return "usage: gdi [on|off]";
  int items = 0;
  EnterCriticalSection(&g_gdi_cs);
  for (int i = 0; i < g_nlay; i++)
    items += g_lay[i].n;
  int layers = g_nlay;
  LeaveCriticalSection(&g_gdi_cs);
  emit(o,
       "{\"on\":%ld,\"patched\":%d,\"layers\":%d,\"items\":%d,"
       "\"evicted\":%d}\n",
       (long)g_gdi_on, g_gdi_patched, layers, items, g_gdi_evicted);
  return NULL;
}

static void emit_layer(struct out *o, const struct layer *L, POINT off) {
  layer_open(o, (HWND)L->key, off);
  for (int i = 0; i < L->n; i++)
    emit_elem(o, &L->it[i].r, (unsigned)L->it[i].c, L->it[i].t);
  layer_close(o);
}

static int in_paint(const struct layer *L, DWORD tick) {
  return L->painting && tick - L->painting < PAINT_MS;
}

// seq > last 且不在重绘中的窗口图层整层写入 o，回本轮快照点；beat（心跳）
// 时顺带丢掉已销毁窗口的图层。窗口几何在锁外取。
static LONG gdi_pump(struct out *o, LONG last, int beat) {
  HWND hs[LAYER_CAP];
  POINT off[LAYER_CAP];
  char alive[LAYER_CAP];
  LONG now = g_gdi_seq;
  DWORD tick = GetTickCount();
  int n = 0;
  EnterCriticalSection(&g_gdi_cs);
  for (int i = 0; i < g_nlay; i++)
    if (g_lay[i].win &&
        (beat || (g_lay[i].seq > last && !in_paint(&g_lay[i], tick))))
      hs[n++] = (HWND)g_lay[i].key;
  LeaveCriticalSection(&g_gdi_cs);
  for (int i = 0; i < n; i++)
    alive[i] = client_off(hs[i], &off[i]);
  EnterCriticalSection(&g_gdi_cs);
  for (int i = 0; i < n; i++) {
    struct layer *L = lay_get(hs[i], 1, 0);
    if (!L)
      continue;
    if (!alive[i])
      lay_free(L);
    else if (L->seq > last && !in_paint(L, tick))
      emit_layer(o, L, off[i]);
  }
  LeaveCriticalSection(&g_gdi_cs);
  return now;
}

static const char *cmd_gdidump(struct out *o, HWND h, char *a) {
  return feed_dump(o, &GDI_FEED);
}

static const char *cmd_gdiwatch(struct out *o, HWND h, char *a) {
  return feed_open(o, &GDI_FEED);
}

// ---------- 分派 ----------
static const char *run(struct out *o, const struct cmd *t, char *line);

static const char *cmd_ping(struct out *o, HWND h, char *a) {
  emit(o, "{\"pid\":%lu}\n", GetCurrentProcessId());
  return NULL;
}

static const struct cmd SPY[] = {
    {"on", cmd_spy_on, ARG_NONE},
    {"off", cmd_spy_off, ARG_NONE},
    {"clear", cmd_spy_clear, ARG_NONE},
    {"dump", cmd_spy_dump, ARG_NONE},
    {NULL, NULL, 0},
};

static const char *cmd_spy(struct out *o, HWND h, char *a) {
  return run(o, SPY, a);
}

static const struct cmd CMDS[] = {
    {"ping", cmd_ping, ARG_NONE},
    {"treegen", cmd_treegen, ARG_NONE},
    {"treewatch", cmd_treewatch, ARG_NONE},
    {"gdidump", cmd_gdidump, ARG_NONE},
    {"gdiwatch", cmd_gdiwatch, ARG_NONE},
    {"spy", cmd_spy, ARG_NONE},
    {"enum", cmd_enum, ARG_OPT},
    {"tree", cmd_tree, ARG_OPT},
    {"find", cmd_find, ARG_NONE},
    {"rect", cmd_rect, ARG_REQ},
    {"gettext", cmd_gettext, ARG_REQ},
    {"settext", cmd_settext, ARG_REQ},
    {"post", cmd_post, ARG_REQ},
    {"send", cmd_send, ARG_REQ},
    {"click", cmd_click, ARG_REQ},
    {"clickat", cmd_clickat, ARG_REQ},
    {"highlight", cmd_highlight, ARG_REQ},
    {"detail", cmd_detail, ARG_REQ},
    {"mouse", cmd_mouse, ARG_NONE},
    {"key", cmd_key, ARG_NONE},
    {"char", cmd_char, ARG_NONE},
    {"noraise", cmd_noraise, ARG_NONE},
    {"gdi", cmd_gdi, ARG_NONE},
    {"req", cmd_req, ARG_NONE},
    {NULL, NULL, 0},
};

// 首 token 查表 t；help 列出 t 的命令名。
static const char *run(struct out *o, const struct cmd *t, char *line) {
  char *a = line + strcspn(line, " ");
  if (*a)
    *a++ = 0;
  if (!strcmp(line, "help")) {
    emit(o, "{\"cmds\":[");
    for (const struct cmd *c = t; c->name; c++) {
      if (c != t)
        put(o, ",", 1);
      emit_str(o, c->name);
    }
    emit(o, "]}\n");
    return NULL;
  }
  for (; t->name; t++) {
    if (strcmp(t->name, line))
      continue;
    HWND h = NULL;
    if (t->arg == ARG_REQ || (t->arg == ARG_OPT && *a)) {
      h = split_target(a, &a);
      if (!h)
        return "target not found";
    }
    return t->fn(o, h, a);
  }
  return "unknown cmd (try help)";
}

// 回 0 = 对端已断。
static int send_all(SOCKET c, const char *s, size_t n) {
  for (int k; n && (k = send(c, s, (int)n, 0)) > 0; s += k, n -= k)
    ;
  return n == 0;
}

// 读一行命令（TCP 可能分片，读到 \n 为止），查表执行，应答末行 ok / err。
static void handle(SOCKET c) {
  char line[8192];
  int n = 0, k;
  while (n < (int)sizeof(line) - 1 &&
         (k = recv(c, line + n, sizeof(line) - 1 - n, 0)) > 0) {
    n += k;
    if (memchr(line + n - k, '\n', k))
      break;
  }
  line[n] = 0;
  line[strcspn(line, "\r\n")] = 0;
  struct out o = {c, 0, NULL, 0, 0};
  const char *err = run(&o, CMDS, line);
  if (!o.detach) {
    if (err) {
      o.len = 0;
      emit(&o, "err %s\n", err);
    } else
      put(&o, "ok\n", 3);
    send_all(c, o.buf, o.len);
    closesocket(c);
  }
  free(o.buf);
}

static int read_port(void) {
  FILE *f = fopen("C:\\windows\\temp\\tdx_shim_port.txt", "r");
  if (!f)
    return DEFAULT_PORT;
  char b[32] = {0};
  int p = DEFAULT_PORT;
  if (fgets(b, sizeof(b), f)) {
    int v = atoi(b);
    if (v > 0 && v < 65536)
      p = v;
  }
  fclose(f);
  return p;
}

static DWORD WINAPI listener(LPVOID arg) {
  (void)arg;
  g_exec_msg = RegisterWindowMessageW(L"tdx_shim_exec");
  assert(g_exec_msg);
  g_patched = noraise_patch();
  WSADATA wsa;
  if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0)
    return 1;
  SOCKET srv = socket(AF_INET, SOCK_STREAM, 0);
  if (srv == INVALID_SOCKET)
    return 2;
  BOOL on = 1;
  setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, (const char *)&on, sizeof(on));
  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = htons(read_port());
  if (bind(srv, (struct sockaddr *)&addr, sizeof(addr)) == SOCKET_ERROR) {
    closesocket(srv);
    return 3;
  }
  if (listen(srv, 8) == SOCKET_ERROR) {
    closesocket(srv);
    return 4;
  }
  for (;;) {
    SOCKET c = accept(srv, NULL, NULL);
    if (c == INVALID_SOCKET)
      break;
    handle(c);
  }
  closesocket(srv);
  return 0;
}

BOOL APIENTRY DllMain(HMODULE hMod, DWORD reason, LPVOID reserved) {
  (void)reserved;
  if (reason == DLL_PROCESS_ATTACH) {
    g_hMod = hMod;
    if (!g_cs_inited) {
      InitializeCriticalSection(&g_cs);
      InitializeCriticalSection(&g_req_cs);
      InitializeCriticalSection(&g_feed_cs);
      InitializeCriticalSection(&g_gdi_cs);
      g_tls = TlsAlloc();
      assert(g_tls != TLS_OUT_OF_INDEXES);
      g_cs_inited = 1;
    }
    DisableThreadLibraryCalls(hMod);
    HANDLE t = CreateThread(NULL, 0, listener, NULL, 0, NULL);
    if (t)
      CloseHandle(t);
    HANDLE te = CreateThread(NULL, 0, evt_thread, NULL, 0, NULL);
    if (te)
      CloseHandle(te);
  }
  return TRUE;
}
